"""Business Run control plane delegating execution to DSH."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dsh_base_agent.agent import Agent
from dsh_base_agent.auth import Authorizer, Principal, ReadOnlyByDefaultAuthorizer
from dsh_base_agent.gateway import GatewayRunContext, ToolGateway
from dsh_base_agent.models import (
    TERMINAL_RUN_STATUSES,
    ArtifactRecord,
    AttemptStatus,
    AuditRecord,
    ConversationRecord,
    ConversationStatus,
    EventSource,
    RunAttempt,
    RunEvent,
    RunRecord,
    RunStatus,
    new_id,
    utc_now,
)
from dsh_base_agent.runtime import (
    DshRuntime,
    DshRuntimeFactory,
    OfficialDshRuntimeFactory,
    RuntimeConfig,
)
from dsh_base_agent.store import ControlStore, SqliteControlStore, StoreError, request_digest


class AgentNotFoundError(KeyError):
    pass


class RunAccessDenied(PermissionError):
    pass


class ConversationAccessDenied(PermissionError):
    pass


class RunTransitionError(RuntimeError):
    pass


class RunResumeUnsupported(RunTransitionError):
    pass


@dataclass(frozen=True, slots=True)
class RunView:
    run: RunRecord
    attempts: tuple[RunAttempt, ...]


@dataclass(slots=True)
class _ConversationRuntimeHolder:
    runtime: DshRuntime
    gateway: ToolGateway | None
    agent_fingerprint: str
    dsh_session_id: str


class ControlPlane:
    """Own business task state while DSH owns every Agent execution detail."""

    def __init__(
        self,
        *,
        workspace: str | Path,
        runtime: RuntimeConfig,
        store: ControlStore | None = None,
        runtime_factory: DshRuntimeFactory | None = None,
        authorizer: Authorizer | None = None,
    ) -> None:
        self.workspace = Path(workspace).expanduser().resolve()
        self.runtime_config = runtime
        database = self.workspace / ".dsh-base-agent" / "control.db"
        self.store = store or SqliteControlStore(database)
        self.runtime_factory = runtime_factory or OfficialDshRuntimeFactory(runtime)
        self.authorizer = authorizer or ReadOnlyByDefaultAuthorizer()
        self._agents: dict[str, Agent] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._conversation_tasks: dict[str, asyncio.Task[None]] = {}
        self._conversation_runtimes: dict[str, _ConversationRuntimeHolder] = {}
        self._run_done: dict[str, asyncio.Event] = {}
        self._active_runtimes: dict[str, DshRuntime] = {}
        self._run_locks: dict[str, asyncio.Lock] = {}
        self._task_lock = asyncio.Lock()
        self._started = False
        self._closed = False

    def register(self, agent: Agent) -> Agent:
        existing = self._agents.get(agent.name)
        if existing is not None and existing.fingerprint != agent.fingerprint:
            raise ValueError(f"Agent '{agent.name}' is already registered with another definition")
        self._agents[agent.name] = agent
        return agent

    @property
    def agents(self) -> Mapping[str, Agent]:
        return dict(self._agents)

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("ControlPlane is closed")
        if self._started:
            return
        self.workspace.mkdir(parents=True, exist_ok=True)
        await self.store.initialize()
        self._started = True
        await self._recover_startup()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        tasks = tuple(self._tasks.values()) + tuple(self._conversation_tasks.values())
        holders = tuple(self._conversation_runtimes.values())
        runtimes_by_identity = {
            id(runtime): runtime
            for runtime in (
                *self._active_runtimes.values(),
                *(holder.runtime for holder in holders),
            )
        }
        runtimes = tuple(runtimes_by_identity.values())
        await asyncio.gather(*(runtime.close() for runtime in runtimes), return_exceptions=True)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self._conversation_tasks.clear()
        self._active_runtimes.clear()
        self._conversation_runtimes.clear()
        await asyncio.gather(
            *(holder.gateway.close() for holder in holders if holder.gateway is not None),
            return_exceptions=True,
        )
        for event in self._run_done.values():
            event.set()
        self._run_done.clear()
        await self.store.close()

    async def create_conversation(
        self,
        *,
        principal: Principal,
        agent_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> ConversationRecord:
        await self._ensure_started()
        agent = self._agent(agent_id)
        try:
            await self.authorizer.authorize_run(principal, agent)
        except BaseException as exc:
            await self.store.append_audit(
                AuditRecord(
                    tenant_id=principal.tenant_id,
                    principal_id=principal.principal_id,
                    action="conversation.create",
                    outcome="denied",
                    agent_id=agent.name,
                    data={"error_type": type(exc).__name__},
                )
            )
            raise
        home_key = request_digest(
            {
                "tenant_id": principal.tenant_id,
                "principal_id": principal.principal_id,
                "agent_fingerprint": agent.fingerprint,
                "nonce": new_id("home"),
            }
        )
        conversation = ConversationRecord(
            tenant_id=principal.tenant_id,
            principal_id=principal.principal_id,
            agent_id=agent.name,
            agent_version=agent.version,
            agent_fingerprint=agent.fingerprint,
            dsh_home_key=home_key,
            dsh_session_id=new_id("session"),
            metadata=metadata or {},
        )
        await self.store.create_conversation(conversation)
        await self.store.append_audit(
            AuditRecord(
                tenant_id=principal.tenant_id,
                principal_id=principal.principal_id,
                action="conversation.create",
                outcome="success",
                agent_id=agent.name,
                data={"conversation_id": conversation.conversation_id},
            )
        )
        return conversation

    async def get_conversation(
        self,
        principal: Principal,
        conversation_id: str,
    ) -> ConversationRecord:
        await self._ensure_started()
        return await self._owned_conversation(principal, conversation_id)

    async def conversation_runs(
        self,
        principal: Principal,
        conversation_id: str,
    ) -> tuple[RunRecord, ...]:
        await self._ensure_started()
        await self._owned_conversation(principal, conversation_id)
        return await self.store.list_conversation_runs(conversation_id)

    async def submit_to_conversation(
        self,
        *,
        principal: Principal,
        conversation_id: str,
        input: str,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunRecord:
        await self._ensure_started()
        conversation = await self._owned_conversation(principal, conversation_id)
        if conversation.status is not ConversationStatus.ACTIVE:
            raise RunTransitionError(
                f"Conversation is {conversation.status.value}: "
                f"{conversation.blocked_reason or 'not accepting Runs'}"
            )
        if not input.strip():
            raise ValueError("Run input must not be blank")
        agent = self._agent(conversation.agent_id)
        if agent.fingerprint != conversation.agent_fingerprint:
            raise RunTransitionError("registered Agent no longer matches the Conversation")
        try:
            await self.authorizer.authorize_run(principal, agent)
        except BaseException as exc:
            await self.store.append_audit(
                AuditRecord(
                    tenant_id=principal.tenant_id,
                    principal_id=principal.principal_id,
                    action="conversation.run.submit",
                    outcome="denied",
                    agent_id=agent.name,
                    data={
                        "conversation_id": conversation_id,
                        "error_type": type(exc).__name__,
                    },
                )
            )
            raise
        run = RunRecord(
            conversation_id=conversation_id,
            tenant_id=principal.tenant_id,
            principal_id=principal.principal_id,
            agent_id=agent.name,
            agent_version=agent.version,
            agent_fingerprint=agent.fingerprint,
            input=input,
            metadata=metadata or {},
        )
        digest = request_digest(
            {
                "agent_fingerprint": agent.fingerprint,
                "conversation_id": conversation_id,
                "input": input,
                "metadata": metadata or {},
                "principal_id": principal.principal_id,
            }
        )
        try:
            persisted, created = await self.store.create_conversation_run(
                conversation_id,
                run,
                idempotency_key=idempotency_key,
                request_digest=digest,
            )
        except StoreError as exc:
            raise RunTransitionError(str(exc)) from exc
        if created:
            await self.store.append_event(
                run_id=persisted.run_id,
                attempt_id=None,
                source=EventSource.CONTROL,
                kind="run.queued",
                data={
                    "agent_id": agent.name,
                    "agent_version": agent.version,
                    "conversation_id": conversation_id,
                    "sequence": persisted.sequence,
                },
            )
            await self.store.append_audit(
                AuditRecord(
                    tenant_id=principal.tenant_id,
                    principal_id=principal.principal_id,
                    action="conversation.run.submit",
                    outcome="accepted",
                    run_id=persisted.run_id,
                    agent_id=agent.name,
                    data={
                        "conversation_id": conversation_id,
                        "sequence": persisted.sequence,
                        "input_sha256": hashlib.sha256(input.encode()).hexdigest(),
                    },
                )
            )
            await self._schedule_conversation(conversation_id, persisted.run_id)
        return persisted

    async def submit(
        self,
        *,
        principal: Principal,
        agent_id: str,
        input: str,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunRecord:
        await self._ensure_started()
        agent = self._agent(agent_id)
        if not input.strip():
            raise ValueError("Run input must not be blank")
        try:
            await self.authorizer.authorize_run(principal, agent)
        except BaseException as exc:
            await self.store.append_audit(
                AuditRecord(
                    tenant_id=principal.tenant_id,
                    principal_id=principal.principal_id,
                    action="run.submit",
                    outcome="denied",
                    agent_id=agent.name,
                    data={"error_type": type(exc).__name__},
                )
            )
            raise
        run = RunRecord(
            tenant_id=principal.tenant_id,
            principal_id=principal.principal_id,
            agent_id=agent.name,
            agent_version=agent.version,
            agent_fingerprint=agent.fingerprint,
            input=input,
            metadata=metadata or {},
        )
        digest = request_digest(
            {
                "agent_fingerprint": agent.fingerprint,
                "input": input,
                "metadata": metadata or {},
                "principal_id": principal.principal_id,
            }
        )
        persisted, created = await self.store.create_run(
            run,
            idempotency_key=idempotency_key,
            request_digest=digest,
        )
        if created:
            await self.store.append_event(
                run_id=persisted.run_id,
                attempt_id=None,
                source=EventSource.CONTROL,
                kind="run.queued",
                data={"agent_id": agent.name, "agent_version": agent.version},
            )
            await self.store.append_audit(
                AuditRecord(
                    tenant_id=principal.tenant_id,
                    principal_id=principal.principal_id,
                    action="run.submit",
                    outcome="accepted",
                    run_id=persisted.run_id,
                    agent_id=agent.name,
                    data={"input_sha256": hashlib.sha256(input.encode()).hexdigest()},
                )
            )
            await self._schedule(persisted.run_id)
        return persisted

    async def wait(self, run_id: str) -> RunRecord:
        await self._ensure_started()
        run = await self.store.get_run(run_id)
        if run.conversation_id is not None and run.status not in TERMINAL_RUN_STATUSES:
            event = self._run_done.setdefault(run_id, asyncio.Event())
            run = await self.store.get_run(run_id)
            if run.status not in TERMINAL_RUN_STATUSES:
                await event.wait()
            return await self.store.get_run(run_id)
        async with self._task_lock:
            task = self._tasks.get(run_id)
        if task is not None:
            await asyncio.shield(task)
        return await self.store.get_run(run_id)

    async def view(self, principal: Principal, run_id: str) -> RunView:
        await self._ensure_started()
        run = await self._owned_run(principal, run_id)
        return RunView(run=run, attempts=await self.store.list_attempts(run_id))

    async def events(
        self,
        principal: Principal,
        run_id: str,
        *,
        after: int = 0,
    ) -> tuple[RunEvent, ...]:
        await self._ensure_started()
        await self._owned_run(principal, run_id)
        return await self.store.list_events(run_id, after=after)

    async def artifacts(
        self,
        principal: Principal,
        run_id: str,
    ) -> tuple[ArtifactRecord, ...]:
        await self._ensure_started()
        await self._owned_run(principal, run_id)
        return await self.store.list_artifacts(run_id)

    async def cancel(self, principal: Principal, run_id: str) -> RunRecord:
        await self._ensure_started()
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            current = await self._owned_run(principal, run_id)
            if current.status in TERMINAL_RUN_STATUSES:
                return current
            now = utc_now()
            cancelled = current.model_copy(
                update={
                    "status": RunStatus.CANCELLED,
                    "cancel_requested": True,
                    "revision": current.revision + 1,
                    "error": "Run cancellation requested",
                    "updated_at": now,
                }
            )
            if current.active_attempt_id is not None:
                attempt = await self.store.get_attempt(current.active_attempt_id)
                if attempt.status not in {
                    AttemptStatus.SUCCEEDED,
                    AttemptStatus.FAILED,
                    AttemptStatus.CANCELLED,
                    AttemptStatus.INTERRUPTED,
                }:
                    settled = attempt.model_copy(
                        update={
                            "status": AttemptStatus.CANCELLED,
                            "revision": attempt.revision + 1,
                            "error": "Run cancellation requested",
                            "finished_at": now,
                            "updated_at": now,
                        }
                    )
                    await self.store.settle_attempt(
                        cancelled,
                        settled,
                        expected_run_revision=current.revision,
                        expected_attempt_revision=attempt.revision,
                    )
                else:
                    await self.store.replace_run(cancelled, expected_revision=current.revision)
            else:
                await self.store.replace_run(cancelled, expected_revision=current.revision)
        runtime = self._active_runtimes.get(run_id)
        if current.conversation_id is None:
            if runtime is not None:
                await runtime.close()
            async with self._task_lock:
                task = self._tasks.get(run_id)
            if task is not None:
                task.cancel()
        elif current.active_attempt_id is not None:
            await self._block_conversation(
                current.conversation_id,
                "active conversation Run was cancelled; Session reconciliation is required",
            )
            async with self._task_lock:
                task = self._conversation_tasks.get(current.conversation_id)
            if task is not None:
                task.cancel()
            await self._discard_conversation_runtime(current.conversation_id)
        self._run_done.setdefault(run_id, asyncio.Event()).set()
        await self.store.append_event(
            run_id=run_id,
            attempt_id=cancelled.active_attempt_id,
            source=EventSource.CONTROL,
            kind="run.cancelled",
            data={"actor": principal.principal_id},
        )
        await self._audit_run(principal, cancelled, "run.cancel", "success")
        return cancelled

    async def retry(self, principal: Principal, run_id: str) -> RunRecord:
        await self._ensure_started()
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            current = await self._owned_run(principal, run_id)
            if current.conversation_id is not None:
                raise RunTransitionError(
                    "Conversation Runs cannot be retried in place; submit a new Run "
                    "to an active Conversation"
                )
            if current.status not in {RunStatus.FAILED, RunStatus.CANCELLED}:
                raise RunTransitionError("only failed or cancelled Runs can be retried")
            queued = current.model_copy(
                update={
                    "status": RunStatus.QUEUED,
                    "revision": current.revision + 1,
                    "active_attempt_id": None,
                    "output": None,
                    "error": None,
                    "cancel_requested": False,
                    "updated_at": utc_now(),
                }
            )
            await self.store.replace_run(queued, expected_revision=current.revision)
        await self.store.append_event(
            run_id=run_id,
            attempt_id=None,
            source=EventSource.CONTROL,
            kind="run.retry_queued",
            data={"actor": principal.principal_id},
        )
        await self._audit_run(principal, queued, "run.retry", "accepted")
        await self._schedule(run_id)
        return queued

    async def resume(
        self,
        principal: Principal,
        run_id: str,
        *,
        input: str,
    ) -> RunRecord:
        await self._ensure_started()
        del input
        run = await self._owned_run(principal, run_id)
        if run.status is not RunStatus.WAITING:
            raise RunTransitionError("only a waiting Run can be resumed")
        raise RunResumeUnsupported(
            "the pinned DSH SDK has no Host session-resume method; refusing to fake a new turn"
        )

    async def _schedule(self, run_id: str) -> None:
        async with self._task_lock:
            existing = self._tasks.get(run_id)
            if existing is not None and not existing.done():
                return
            task = asyncio.create_task(self._execute(run_id), name=f"run-{run_id}")
            self._tasks[run_id] = task

    async def _schedule_conversation(self, conversation_id: str, run_id: str) -> None:
        self._run_done.setdefault(run_id, asyncio.Event())
        async with self._task_lock:
            existing = self._conversation_tasks.get(conversation_id)
            if existing is not None and not existing.done():
                return
            task = asyncio.create_task(
                self._drain_conversation(conversation_id),
                name=f"conversation-{conversation_id}",
            )
            self._conversation_tasks[conversation_id] = task

    async def _drain_conversation(self, conversation_id: str) -> None:
        try:
            while not self._closed:
                conversation = await self.store.get_conversation(conversation_id)
                if conversation.status is not ConversationStatus.ACTIVE:
                    return
                runs = await self.store.list_conversation_runs(conversation_id)
                if any(
                    run.status in {RunStatus.RUNNING, RunStatus.WAITING}
                    for run in runs
                ):
                    return
                queued = sorted(
                    (run for run in runs if run.status is RunStatus.QUEUED),
                    key=lambda run: run.sequence or 0,
                )
                if not queued:
                    return
                selected = queued[0]
                succeeded = await self._execute_conversation_run(
                    conversation,
                    selected.run_id,
                )
                self._run_done.setdefault(selected.run_id, asyncio.Event()).set()
                if not succeeded:
                    return
        finally:
            async with self._task_lock:
                current = asyncio.current_task()
                if self._conversation_tasks.get(conversation_id) is current:
                    self._conversation_tasks.pop(conversation_id, None)
            await self._reschedule_conversation_if_needed(conversation_id)

    async def _reschedule_conversation_if_needed(self, conversation_id: str) -> None:
        if self._closed:
            return
        try:
            conversation = await self.store.get_conversation(conversation_id)
            runs = await self.store.list_conversation_runs(conversation_id)
        except StoreError:
            return
        if conversation.status is not ConversationStatus.ACTIVE:
            return
        pending = next((run for run in runs if run.status is RunStatus.QUEUED), None)
        if pending is not None:
            await self._schedule_conversation(conversation_id, pending.run_id)

    async def _recover_startup(self) -> None:
        unsettled = await self.store.list_runs(
            statuses=(RunStatus.QUEUED.value, RunStatus.RUNNING.value)
        )
        for run in unsettled:
            if run.status is RunStatus.QUEUED:
                if run.conversation_id is None:
                    await self._schedule(run.run_id)
                continue
            now = utc_now()
            message = (
                "Control plane restarted while the DSH attempt was running; "
                "explicit retry is required"
            )
            failed = run.model_copy(
                update={
                    "status": RunStatus.FAILED,
                    "revision": run.revision + 1,
                    "error": message,
                    "updated_at": now,
                }
            )
            if run.active_attempt_id is None:
                await self.store.replace_run(failed, expected_revision=run.revision)
            else:
                attempt = await self.store.get_attempt(run.active_attempt_id)
                interrupted = attempt.model_copy(
                    update={
                        "status": AttemptStatus.INTERRUPTED,
                        "revision": attempt.revision + 1,
                        "error": message,
                        "finished_at": now,
                        "updated_at": now,
                    }
                )
                await self.store.settle_attempt(
                    failed,
                    interrupted,
                    expected_run_revision=run.revision,
                    expected_attempt_revision=attempt.revision,
                )
            await self.store.append_event(
                run_id=run.run_id,
                attempt_id=run.active_attempt_id,
                source=EventSource.CONTROL,
                kind="run.recovery_required",
                data={"reason": "control_plane_restart"},
            )
            await self.store.append_audit(
                AuditRecord(
                    tenant_id=run.tenant_id,
                    principal_id="system",
                    action="run.recovery",
                    outcome="explicit_retry_required",
                    run_id=run.run_id,
                    attempt_id=run.active_attempt_id,
                    agent_id=run.agent_id,
                    data={"reason": "control_plane_restart"},
                )
            )
            if run.conversation_id is not None:
                await self._block_conversation(
                    run.conversation_id,
                    "Control plane restarted during an active DSH Turn; "
                    "Session reconciliation is required",
                )

        active_conversations = await self.store.list_conversations(
            statuses=(ConversationStatus.ACTIVE.value,)
        )
        for conversation in active_conversations:
            runs = await self.store.list_conversation_runs(conversation.conversation_id)
            if any(run.attempt_count > 0 for run in runs):
                await self._block_conversation(
                    conversation.conversation_id,
                    "Control plane restarted after the DSH Session was opened; "
                    "the current Python SDK cannot resume it",
                )
                continue
            queued = next((run for run in runs if run.status is RunStatus.QUEUED), None)
            if queued is not None:
                await self._schedule_conversation(
                    conversation.conversation_id,
                    queued.run_id,
                )

    async def _execute(self, run_id: str) -> None:
        attempt: RunAttempt | None = None
        run: RunRecord | None = None
        gateway: ToolGateway | None = None
        runtime: DshRuntime | None = None
        try:
            lock = self._run_locks.setdefault(run_id, asyncio.Lock())
            async with lock:
                current = await self.store.get_run(run_id)
                if current.status is not RunStatus.QUEUED:
                    return
                agent = self._agent(current.agent_id)
                if agent.fingerprint != current.agent_fingerprint:
                    raise RunTransitionError("registered Agent no longer matches the pinned Run")
                attempt = RunAttempt(
                    run_id=run_id,
                    number=current.attempt_count + 1,
                    dsh_session_id=f"session_{run_id}_{current.attempt_count + 1}",
                )
                now = utc_now()
                run = current.model_copy(
                    update={
                        "status": RunStatus.RUNNING,
                        "revision": current.revision + 1,
                        "attempt_count": attempt.number,
                        "active_attempt_id": attempt.attempt_id,
                        "updated_at": now,
                    }
                )
                attempt = attempt.model_copy(
                    update={
                        "status": AttemptStatus.RUNNING,
                        "started_at": now,
                        "updated_at": now,
                    }
                )
                await self.store.begin_attempt(
                    run,
                    attempt,
                    expected_run_revision=current.revision,
                )
            await self.store.append_event(
                run_id=run_id,
                attempt_id=attempt.attempt_id,
                source=EventSource.CONTROL,
                kind="run.started",
                data={
                    "attempt": attempt.number,
                    "dsh_session_id": attempt.dsh_session_id,
                },
            )
            await self._audit_run(
                Principal(run.tenant_id, run.principal_id),
                run,
                "run.attempt.start",
                "success",
                attempt_id=attempt.attempt_id,
                data={"attempt": attempt.number, "dsh_session_id": attempt.dsh_session_id},
            )

            async def write_event(
                source: EventSource,
                kind: str,
                data: dict[str, Any],
            ) -> None:
                await self.store.append_event(
                    run_id=run_id,
                    attempt_id=attempt.attempt_id,
                    source=source,
                    kind=kind,
                    data=data,
                )

            async def write_audit(
                tool_name: str,
                action: str,
                outcome: str,
                data: dict[str, Any],
            ) -> None:
                await self.store.append_audit(
                    AuditRecord(
                        tenant_id=run.tenant_id,
                        principal_id=run.principal_id,
                        action=action,
                        outcome=outcome,
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                        agent_id=run.agent_id,
                        tool_name=tool_name,
                        data=data,
                    )
                )

            agent = self._agent(run.agent_id)
            if agent.tools:
                gateway = ToolGateway(
                    agent=agent,
                    authorizer=self.authorizer,
                    write_event=write_event,
                    write_audit=write_audit,
                )
                await gateway.start()
                gateway.bind(
                    GatewayRunContext(
                        principal=Principal(run.tenant_id, run.principal_id),
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                    )
                )
            dsh_home = self._agent_dsh_home(run.agent_fingerprint)
            runtime = self.runtime_factory.create(
                agent,
                workspace=self.workspace,
                dsh_home=dsh_home,
                attempt_id=attempt.attempt_id,
                tool_gateway_url=gateway.url if gateway is not None else None,
            )
            self._active_runtimes[run_id] = runtime

            async def on_dsh_event(event: dict[str, Any]) -> None:
                projected = _project_dsh_event(event)
                if projected is None:
                    return
                kind, data = projected
                await write_event(EventSource.DSH, kind, data)

            result = await runtime.run(
                run.input,
                session_id=attempt.dsh_session_id,
                on_event=on_dsh_event,
            )
            if result.session_id != attempt.dsh_session_id:
                raise RuntimeError("DSH returned a different Session ID")
            if result.finish_reason != "completed":
                raise RuntimeError(f"DSH Run did not complete: {result.finish_reason or 'unknown'}")
            await self._settle_success(run, attempt, result.final_response, result.finish_reason)
        except asyncio.CancelledError:
            # ``cancel`` owns the durable cancellation transition.
            raise
        except BaseException as exc:
            if run is not None and attempt is not None:
                await self._settle_failure(run, attempt, exc)
        finally:
            self._active_runtimes.pop(run_id, None)
            if runtime is not None:
                await runtime.close()
            if gateway is not None:
                if attempt is not None:
                    gateway.release(attempt.attempt_id)
                await gateway.close()

    async def _execute_conversation_run(
        self,
        conversation: ConversationRecord,
        run_id: str,
    ) -> bool:
        attempt: RunAttempt | None = None
        run: RunRecord | None = None
        holder: _ConversationRuntimeHolder | None = None
        gateway_bound = False
        try:
            lock = self._run_locks.setdefault(run_id, asyncio.Lock())
            async with lock:
                current = await self.store.get_run(run_id)
                if current.status is not RunStatus.QUEUED:
                    return current.status is RunStatus.SUCCEEDED
                agent = self._agent(current.agent_id)
                if agent.fingerprint != conversation.agent_fingerprint:
                    raise RunTransitionError(
                        "registered Agent no longer matches the Conversation"
                    )
                attempt = RunAttempt(
                    run_id=run_id,
                    number=current.attempt_count + 1,
                    dsh_session_id=conversation.dsh_session_id,
                )
                now = utc_now()
                run = current.model_copy(
                    update={
                        "status": RunStatus.RUNNING,
                        "revision": current.revision + 1,
                        "attempt_count": attempt.number,
                        "active_attempt_id": attempt.attempt_id,
                        "updated_at": now,
                    }
                )
                attempt = attempt.model_copy(
                    update={
                        "status": AttemptStatus.RUNNING,
                        "started_at": now,
                        "updated_at": now,
                    }
                )
                await self.store.begin_attempt(
                    run,
                    attempt,
                    expected_run_revision=current.revision,
                )
            await self.store.append_event(
                run_id=run_id,
                attempt_id=attempt.attempt_id,
                source=EventSource.CONTROL,
                kind="run.started",
                data={
                    "attempt": attempt.number,
                    "conversation_id": conversation.conversation_id,
                    "sequence": run.sequence,
                    "dsh_session_id": attempt.dsh_session_id,
                },
            )
            await self._audit_run(
                Principal(run.tenant_id, run.principal_id),
                run,
                "run.attempt.start",
                "success",
                attempt_id=attempt.attempt_id,
                data={
                    "attempt": attempt.number,
                    "conversation_id": conversation.conversation_id,
                    "sequence": run.sequence,
                    "dsh_session_id": attempt.dsh_session_id,
                },
            )

            async def write_event(
                source: EventSource,
                kind: str,
                data: dict[str, Any],
            ) -> None:
                await self.store.append_event(
                    run_id=run_id,
                    attempt_id=attempt.attempt_id,
                    source=source,
                    kind=kind,
                    data=data,
                )

            async def write_audit(
                tool_name: str,
                action: str,
                outcome: str,
                data: dict[str, Any],
            ) -> None:
                await self.store.append_audit(
                    AuditRecord(
                        tenant_id=run.tenant_id,
                        principal_id=run.principal_id,
                        action=action,
                        outcome=outcome,
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                        agent_id=run.agent_id,
                        tool_name=tool_name,
                        data={"conversation_id": conversation.conversation_id, **data},
                    )
                )

            agent = self._agent(run.agent_id)
            holder = await self._conversation_runtime_holder(
                conversation,
                agent,
                attempt.attempt_id,
            )
            if holder.gateway is not None:
                holder.gateway.bind(
                    GatewayRunContext(
                        principal=Principal(run.tenant_id, run.principal_id),
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                    ),
                    write_event=write_event,
                    write_audit=write_audit,
                )
                gateway_bound = True
            self._active_runtimes[run_id] = holder.runtime

            async def on_dsh_event(event: dict[str, Any]) -> None:
                projected = _project_dsh_event(event)
                if projected is None:
                    return
                kind, data = projected
                await write_event(EventSource.DSH, kind, data)

            result = await holder.runtime.run(
                run.input,
                session_id=conversation.dsh_session_id,
                on_event=on_dsh_event,
            )
            if result.session_id != conversation.dsh_session_id:
                raise RuntimeError("DSH returned a different Session ID")
            if result.finish_reason != "completed":
                raise RuntimeError(
                    f"DSH Run did not complete: {result.finish_reason or 'unknown'}"
                )
            await self._settle_success(
                run,
                attempt,
                result.final_response,
                result.finish_reason,
            )
            return True
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if run is not None and attempt is not None:
                await self._settle_failure(run, attempt, exc)
            await self._block_conversation(
                conversation.conversation_id,
                f"DSH conversation runtime failed: {type(exc).__name__}",
            )
            await self._discard_conversation_runtime(conversation.conversation_id)
            return False
        finally:
            self._active_runtimes.pop(run_id, None)
            if gateway_bound and holder is not None and holder.gateway is not None:
                if attempt is not None:
                    holder.gateway.release(attempt.attempt_id)

    async def _conversation_runtime_holder(
        self,
        conversation: ConversationRecord,
        agent: Agent,
        attempt_id: str,
    ) -> _ConversationRuntimeHolder:
        existing = self._conversation_runtimes.get(conversation.conversation_id)
        if existing is not None:
            if (
                existing.agent_fingerprint != conversation.agent_fingerprint
                or existing.dsh_session_id != conversation.dsh_session_id
            ):
                raise RuntimeError("live Runtime does not match its Conversation")
            return existing

        gateway: ToolGateway | None = None
        try:
            if agent.tools:
                gateway = ToolGateway(agent=agent, authorizer=self.authorizer)
                await gateway.start()
            runtime = self.runtime_factory.create(
                agent,
                workspace=self.workspace,
                dsh_home=self._conversation_dsh_home(conversation.dsh_home_key),
                attempt_id=attempt_id,
                tool_gateway_url=gateway.url if gateway is not None else None,
            )
        except BaseException:
            if gateway is not None:
                await gateway.close()
            raise
        holder = _ConversationRuntimeHolder(
            runtime=runtime,
            gateway=gateway,
            agent_fingerprint=conversation.agent_fingerprint,
            dsh_session_id=conversation.dsh_session_id,
        )
        self._conversation_runtimes[conversation.conversation_id] = holder
        return holder

    async def _discard_conversation_runtime(self, conversation_id: str) -> None:
        holder = self._conversation_runtimes.pop(conversation_id, None)
        if holder is None:
            return
        await holder.runtime.close()
        if holder.gateway is not None:
            await holder.gateway.close()

    async def _block_conversation(self, conversation_id: str, reason: str) -> None:
        conversation = await self.store.get_conversation(conversation_id)
        if conversation.status is not ConversationStatus.ACTIVE:
            return
        blocked = conversation.model_copy(
            update={
                "status": ConversationStatus.BLOCKED,
                "blocked_reason": reason,
                "revision": conversation.revision + 1,
                "updated_at": utc_now(),
            }
        )
        await self.store.replace_conversation(
            blocked,
            expected_revision=conversation.revision,
        )
        await self.store.append_audit(
            AuditRecord(
                tenant_id=conversation.tenant_id,
                principal_id="system",
                action="conversation.block",
                outcome="blocked",
                agent_id=conversation.agent_id,
                data={
                    "conversation_id": conversation_id,
                    "reason": reason,
                },
            )
        )

    async def _settle_success(
        self,
        started_run: RunRecord,
        started_attempt: RunAttempt,
        output: str,
        finish_reason: str | None,
    ) -> None:
        lock = self._run_locks.setdefault(started_run.run_id, asyncio.Lock())
        async with lock:
            current = await self.store.get_run(started_run.run_id)
            if current.status is RunStatus.CANCELLED:
                return
            attempt = await self.store.get_attempt(started_attempt.attempt_id)
            now = utc_now()
            settled_attempt = attempt.model_copy(
                update={
                    "status": AttemptStatus.SUCCEEDED,
                    "revision": attempt.revision + 1,
                    "finish_reason": finish_reason,
                    "output": output,
                    "finished_at": now,
                    "updated_at": now,
                }
            )
            settled_run = current.model_copy(
                update={
                    "status": RunStatus.SUCCEEDED,
                    "revision": current.revision + 1,
                    "output": output,
                    "error": None,
                    "updated_at": now,
                }
            )
            await self.store.settle_attempt(
                settled_run,
                settled_attempt,
                expected_run_revision=current.revision,
                expected_attempt_revision=attempt.revision,
            )
        await self.store.append_event(
            run_id=settled_run.run_id,
            attempt_id=settled_attempt.attempt_id,
            source=EventSource.CONTROL,
            kind="run.succeeded",
            data={"finish_reason": finish_reason},
        )
        await self._audit_run(
            Principal(settled_run.tenant_id, settled_run.principal_id),
            settled_run,
            "run.attempt.finish",
            "success",
            attempt_id=settled_attempt.attempt_id,
        )

    async def _settle_failure(
        self,
        started_run: RunRecord,
        started_attempt: RunAttempt,
        error: BaseException,
    ) -> None:
        lock = self._run_locks.setdefault(started_run.run_id, asyncio.Lock())
        async with lock:
            current = await self.store.get_run(started_run.run_id)
            if current.status is RunStatus.CANCELLED:
                return
            attempt = await self.store.get_attempt(started_attempt.attempt_id)
            now = utc_now()
            message = f"{type(error).__name__}: {error}"
            failed_attempt = attempt.model_copy(
                update={
                    "status": AttemptStatus.FAILED,
                    "revision": attempt.revision + 1,
                    "error": message,
                    "finished_at": now,
                    "updated_at": now,
                }
            )
            failed_run = current.model_copy(
                update={
                    "status": RunStatus.FAILED,
                    "revision": current.revision + 1,
                    "error": message,
                    "updated_at": now,
                }
            )
            await self.store.settle_attempt(
                failed_run,
                failed_attempt,
                expected_run_revision=current.revision,
                expected_attempt_revision=attempt.revision,
            )
        await self.store.append_event(
            run_id=failed_run.run_id,
            attempt_id=failed_attempt.attempt_id,
            source=EventSource.CONTROL,
            kind="run.failed",
            data={"error_type": type(error).__name__},
        )
        await self._audit_run(
            Principal(failed_run.tenant_id, failed_run.principal_id),
            failed_run,
            "run.attempt.finish",
            "error",
            attempt_id=failed_attempt.attempt_id,
            data={"error_type": type(error).__name__},
        )

    async def _owned_run(self, principal: Principal, run_id: str) -> RunRecord:
        run = await self.store.get_run(run_id)
        if (
            run.tenant_id != principal.tenant_id
            or run.principal_id != principal.principal_id
        ):
            raise RunAccessDenied("Run belongs to another tenant or principal")
        return run

    async def _owned_conversation(
        self,
        principal: Principal,
        conversation_id: str,
    ) -> ConversationRecord:
        conversation = await self.store.get_conversation(conversation_id)
        if (
            conversation.tenant_id != principal.tenant_id
            or conversation.principal_id != principal.principal_id
        ):
            raise ConversationAccessDenied(
                "Conversation belongs to another tenant or principal"
            )
        return conversation

    def _agent(self, agent_id: str) -> Agent:
        try:
            return self._agents[agent_id]
        except KeyError as exc:
            raise AgentNotFoundError(f"Agent '{agent_id}' is not registered") from exc

    def _agent_dsh_home(self, fingerprint: str) -> Path:
        configured = self.runtime_config.dsh_home.expanduser()
        root = configured if configured.is_absolute() else self.workspace / configured
        return (root / "agents" / fingerprint).resolve()

    def _conversation_dsh_home(self, home_key: str) -> Path:
        configured = self.runtime_config.dsh_home.expanduser()
        root = configured if configured.is_absolute() else self.workspace / configured
        return (root / "conversations" / home_key).resolve()

    async def _ensure_started(self) -> None:
        if not self._started:
            await self.start()

    async def _audit_run(
        self,
        principal: Principal,
        run: RunRecord,
        action: str,
        outcome: str,
        *,
        attempt_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        await self.store.append_audit(
            AuditRecord(
                tenant_id=principal.tenant_id,
                principal_id=principal.principal_id,
                action=action,
                outcome=outcome,
                run_id=run.run_id,
                attempt_id=attempt_id,
                agent_id=run.agent_id,
                data=data or {},
            )
        )


def _project_dsh_event(event: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Persist a bounded projection, not a second copy of DSH conversation state."""

    kind = event.get("type")
    if not isinstance(kind, str):
        return None
    allowed = {
        "turn/start": ("turn",),
        "turn/end": ("turn", "reason"),
        "step/start": ("turn", "step"),
        "step/end": ("turn", "step"),
        "tool/call": ("turn", "step", "callId", "name"),
        "tool/result": ("turn", "step", "error"),
    }
    keys = allowed.get(kind)
    if keys is None:
        return None
    raw = event.get("data")
    data = raw if isinstance(raw, dict) else {}
    projection = {key: data[key] for key in keys if key in data}
    sequence = event.get("seq")
    if isinstance(sequence, int):
        projection["dsh_sequence"] = sequence
    return f"dsh.{kind.replace('/', '.')}", projection


__all__ = [
    "AgentNotFoundError",
    "ConversationAccessDenied",
    "ControlPlane",
    "RunAccessDenied",
    "RunResumeUnsupported",
    "RunTransitionError",
    "RunView",
]
