"""Independent Worker that claims PostgreSQL work and executes DSH runtimes."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from dotenv import dotenv_values

from dsh_base_agent.control.models import ConversationStatus, WorkLease, WorkResourceType
from dsh_base_agent.control.plane import ControlPlane

_LOGGER = logging.getLogger(__name__)


def _worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    worker_id: str = field(default_factory=_worker_id)
    poll_interval_seconds: float = 0.5
    lease_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    heartbeat_timeout_seconds: float = 3.0
    run_timeout_seconds: float = 600.0
    conversation_idle_seconds: float = 60.0
    max_concurrency: int = 4
    max_owned_conversations: int = 128
    shutdown_timeout_seconds: float = 15.0

    def __post_init__(self) -> None:
        if not self.worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        if self.heartbeat_interval_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_interval_seconds must be less than lease_seconds")
        if self.heartbeat_timeout_seconds <= 0:
            raise ValueError("heartbeat_timeout_seconds must be positive")
        if self.heartbeat_interval_seconds + self.heartbeat_timeout_seconds >= self.lease_seconds:
            raise ValueError("heartbeat interval plus timeout must be less than lease_seconds")
        if self.run_timeout_seconds <= 0:
            raise ValueError("run_timeout_seconds must be positive")
        if self.conversation_idle_seconds <= 0:
            raise ValueError("conversation_idle_seconds must be positive")
        if self.max_concurrency <= 0 or self.max_owned_conversations <= 0:
            raise ValueError("Worker capacity must be positive")
        if self.shutdown_timeout_seconds <= 0:
            raise ValueError("shutdown_timeout_seconds must be positive")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "DSH_BASE_AGENT_WORKER_",
        env_file: str | Path | None = ".env",
    ) -> WorkerConfig:
        file_values = {} if env_file is None else dotenv_values(dotenv_path=env_file)
        values = {key: value for key, value in file_values.items() if value is not None}
        values.update(os.environ)
        return cls(
            worker_id=values.get(f"{prefix}ID", _worker_id()),
            poll_interval_seconds=float(values.get(f"{prefix}POLL_INTERVAL_SECONDS", "0.5")),
            lease_seconds=float(values.get(f"{prefix}LEASE_SECONDS", "30")),
            heartbeat_interval_seconds=float(
                values.get(f"{prefix}HEARTBEAT_INTERVAL_SECONDS", "10")
            ),
            heartbeat_timeout_seconds=float(values.get(f"{prefix}HEARTBEAT_TIMEOUT_SECONDS", "3")),
            run_timeout_seconds=float(values.get(f"{prefix}RUN_TIMEOUT_SECONDS", "600")),
            conversation_idle_seconds=float(values.get(f"{prefix}CONVERSATION_IDLE_SECONDS", "60")),
            max_concurrency=int(values.get(f"{prefix}MAX_CONCURRENCY", "4")),
            max_owned_conversations=int(values.get(f"{prefix}MAX_OWNED_CONVERSATIONS", "128")),
            shutdown_timeout_seconds=float(values.get(f"{prefix}SHUTDOWN_TIMEOUT_SECONDS", "15")),
        )


class WorkerService:
    """Poll, fence and execute work independently from the FastAPI process."""

    def __init__(self, control: ControlPlane, config: WorkerConfig) -> None:
        if control.auto_execute:
            raise ValueError("Worker ControlPlane must use auto_execute=False")
        self.control = control
        self.config = config
        self._stop = asyncio.Event()
        self._started = False
        self._closed = False
        self._tasks: dict[tuple[WorkResourceType, str], asyncio.Task[None]] = {}
        self._execution_slots = asyncio.Semaphore(config.max_concurrency)

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("WorkerService is closed")
        if self._started:
            return
        await self.control.start()
        self._started = True

    async def run_forever(self) -> None:
        await self.start()
        while not self._stop.is_set():
            self._discard_finished_tasks()
            await self.control.reconcile_expired_worker_leases()
            claimed = await self._claim_available_work()
            if not claimed:
                await self._wait_for_poll()

    async def run_once(self) -> bool:
        """Claim and execute one resource, primarily for tests and one-shot jobs."""

        await self.start()
        await self.control.reconcile_expired_worker_leases()
        lease = await self.control.store.claim_work(
            worker_id=self.config.worker_id,
            lease_seconds=self.config.lease_seconds,
        )
        if lease is None:
            return False
        await self._execute_resource(lease, keep_conversation=False)
        return True

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        tasks = tuple(self._tasks.values())
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks),
                    timeout=self.config.shutdown_timeout_seconds,
                )
            except TimeoutError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        await self.control.close()

    async def _claim_available_work(self) -> bool:
        claimed = False
        capacity = self.config.max_concurrency + self.config.max_owned_conversations
        while len(self._tasks) < capacity and not self._stop.is_set():
            lease = await self.control.store.claim_work(
                worker_id=self.config.worker_id,
                lease_seconds=self.config.lease_seconds,
            )
            if lease is None:
                break
            key = (lease.resource_type, lease.resource_id)
            if key in self._tasks:
                await self.control.store.release_work_lease(lease)
                break
            if (
                lease.resource_type is WorkResourceType.RUN
                and self._standalone_run_count() >= self.config.max_concurrency
            ):
                await self.control.store.release_work_lease(lease)
                break
            if (
                lease.resource_type is WorkResourceType.CONVERSATION
                and self._owned_conversation_count() >= self.config.max_owned_conversations
            ):
                await self.control.store.release_work_lease(lease)
                break
            task = asyncio.create_task(
                self._execute_resource(
                    lease,
                    keep_conversation=lease.resource_type is WorkResourceType.CONVERSATION,
                ),
                name=f"worker-{lease.resource_type.value}-{lease.resource_id}",
            )
            self._tasks[key] = task
            claimed = True
        return claimed

    async def _execute_resource(
        self,
        lease: WorkLease,
        *,
        keep_conversation: bool,
    ) -> None:
        owner = asyncio.current_task()
        assert owner is not None
        heartbeat = asyncio.create_task(
            self._heartbeat(lease, owner),
            name=f"heartbeat-{lease.resource_type.value}-{lease.resource_id}",
        )
        try:
            if keep_conversation:
                await self._serve_conversation(lease)
            else:
                async with self._execution_slots:
                    await self._execute_with_timeout(lease)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            _LOGGER.error(
                "Run timed out after %.3f seconds for %s %s",
                self.config.run_timeout_seconds,
                lease.resource_type.value,
                lease.resource_id,
            )
        except Exception:
            _LOGGER.exception(
                "Worker failed while executing %s %s",
                lease.resource_type.value,
                lease.resource_id,
            )
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            if lease.resource_type is WorkResourceType.CONVERSATION:
                await self.control.discard_conversation_runtime(lease.resource_id)
            await self.control.store.release_work_lease(lease)

    async def _serve_conversation(self, lease: WorkLease) -> None:
        loop = asyncio.get_running_loop()
        idle_since = loop.time()
        while not self._stop.is_set():
            conversation = await self.control.store.get_conversation(lease.resource_id)
            if conversation.status is not ConversationStatus.ACTIVE:
                return
            async with self._execution_slots:
                executed = await self._execute_with_timeout(lease)
            if executed:
                idle_since = loop.time()
            elif loop.time() - idle_since >= self.config.conversation_idle_seconds:
                return
            await self._wait_for_poll()

    async def _execute_with_timeout(self, lease: WorkLease) -> bool:
        return await asyncio.wait_for(
            self.control.execute_leased_work(lease),
            timeout=self.config.run_timeout_seconds,
        )

    async def _heartbeat(
        self,
        lease: WorkLease,
        owner: asyncio.Task[None],
    ) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.config.heartbeat_interval_seconds,
                )
                return
            except TimeoutError:
                pass
            try:
                renewed = await asyncio.wait_for(
                    self.control.store.renew_work_lease(
                        lease,
                        lease_seconds=self.config.lease_seconds,
                    ),
                    timeout=self.config.heartbeat_timeout_seconds,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception(
                    "Heartbeat failed closed for %s %s",
                    lease.resource_type.value,
                    lease.resource_id,
                )
                owner.cancel()
                return
            if renewed is None:
                _LOGGER.error(
                    "Worker lost fencing lease for %s %s",
                    lease.resource_type.value,
                    lease.resource_id,
                )
                owner.cancel()
                return

    def _discard_finished_tasks(self) -> None:
        for key, task in tuple(self._tasks.items()):
            if not task.done():
                continue
            self._tasks.pop(key, None)
            if not task.cancelled() and task.exception() is not None:
                _LOGGER.error(
                    "Worker resource task failed",
                    exc_info=task.exception(),
                )

    def _owned_conversation_count(self) -> int:
        return sum(
            resource_type is WorkResourceType.CONVERSATION for resource_type, _ in self._tasks
        )

    def _standalone_run_count(self) -> int:
        return sum(resource_type is WorkResourceType.RUN for resource_type, _ in self._tasks)

    async def _wait_for_poll(self) -> None:
        try:
            await asyncio.wait_for(
                self._stop.wait(),
                timeout=self.config.poll_interval_seconds,
            )
        except TimeoutError:
            pass


__all__ = ["WorkerConfig", "WorkerService"]
