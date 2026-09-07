from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import pytest

from dsh_base_agent import WorkerConfig, WorkerService
from dsh_base_agent.control.models import (
    ConversationStatus,
    WorkLease,
    WorkResourceType,
    utc_now,
)


def lease(resource_type: WorkResourceType = WorkResourceType.RUN) -> WorkLease:
    return WorkLease(
        resource_type=resource_type,
        resource_id="resource-1",
        worker_id="worker-a",
        lease_token=1,
        expires_at=utc_now() + timedelta(seconds=1),
    )


def worker_config(**overrides: object) -> WorkerConfig:
    values: dict[str, object] = {
        "worker_id": "worker-a",
        "poll_interval_seconds": 0.005,
        "lease_seconds": 0.1,
        "heartbeat_interval_seconds": 0.01,
        "heartbeat_timeout_seconds": 0.02,
        "run_timeout_seconds": 1.0,
        "conversation_idle_seconds": 0.03,
    }
    values.update(overrides)
    return WorkerConfig(**values)  # type: ignore[arg-type]


class FakeStore:
    def __init__(
        self,
        *,
        renew_error: BaseException | None = None,
        hang_renewal: bool = False,
    ) -> None:
        self.renew_error = renew_error
        self.hang_renewal = hang_renewal
        self.released = 0
        self.renewed = 0

    async def renew_work_lease(
        self,
        current: WorkLease,
        *,
        lease_seconds: float,
    ) -> WorkLease | None:
        del lease_seconds
        self.renewed += 1
        if self.hang_renewal:
            await asyncio.Event().wait()
        if self.renew_error is not None:
            raise self.renew_error
        return current

    async def release_work_lease(self, current: WorkLease) -> bool:
        del current
        self.released += 1
        return True

    async def get_conversation(self, conversation_id: str) -> SimpleNamespace:
        del conversation_id
        return SimpleNamespace(status=ConversationStatus.ACTIVE)


class FakeControl:
    auto_execute = False

    def __init__(self, store: FakeStore, *, idle: bool = False) -> None:
        self.store = store
        self.idle = idle
        self.execute_started = asyncio.Event()
        self.execute_cancelled = asyncio.Event()
        self.execute_calls = 0
        self.discarded = 0

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def execute_leased_work(self, current: WorkLease) -> bool:
        del current
        self.execute_calls += 1
        if self.idle:
            return False
        self.execute_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.execute_cancelled.set()
            raise
        return True

    async def discard_conversation_runtime(self, conversation_id: str) -> None:
        del conversation_id
        self.discarded += 1


async def test_heartbeat_error_fails_closed_and_cancels_owner() -> None:
    store = FakeStore(renew_error=RuntimeError("database unavailable"))
    control = FakeControl(store)
    worker = WorkerService(control, worker_config())  # type: ignore[arg-type]

    owner = asyncio.create_task(worker._execute_resource(lease(), keep_conversation=False))
    await control.execute_started.wait()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(owner, timeout=0.5)

    assert control.execute_cancelled.is_set()
    assert store.released == 1


async def test_heartbeat_timeout_fails_closed_and_cancels_owner() -> None:
    store = FakeStore(hang_renewal=True)
    control = FakeControl(store)
    worker = WorkerService(control, worker_config())  # type: ignore[arg-type]

    owner = asyncio.create_task(worker._execute_resource(lease(), keep_conversation=False))
    await control.execute_started.wait()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(owner, timeout=0.5)

    assert control.execute_cancelled.is_set()
    assert store.renewed == 1
    assert store.released == 1


async def test_run_timeout_cancels_execution_and_releases_lease() -> None:
    store = FakeStore()
    control = FakeControl(store)
    worker = WorkerService(  # type: ignore[arg-type]
        control,
        worker_config(run_timeout_seconds=0.025),
    )

    await worker._execute_resource(lease(), keep_conversation=False)

    assert control.execute_cancelled.is_set()
    assert store.released == 1


async def test_idle_conversation_releases_lease_and_runtime() -> None:
    store = FakeStore()
    control = FakeControl(store, idle=True)
    worker = WorkerService(  # type: ignore[arg-type]
        control,
        worker_config(conversation_idle_seconds=0.02),
    )

    await worker._execute_resource(
        lease(WorkResourceType.CONVERSATION),
        keep_conversation=True,
    )

    assert control.execute_calls >= 2
    assert control.discarded == 1
    assert store.released == 1
