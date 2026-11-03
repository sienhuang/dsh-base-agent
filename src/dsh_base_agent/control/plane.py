"""Business Run control plane delegating execution to DSH."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from dsh_base_agent.adapters.dsh.context_gateway import (
    MemoryContextGateway,
    MemoryRunContext,
)
from dsh_base_agent.adapters.dsh.runtime import (
    DshNotificationHandler,
    DshRuntime,
    DshRuntimeFactory,
    DshWorkflowRequest,
    OfficialDshRuntimeFactory,
    RuntimeConfig,
)
from dsh_base_agent.adapters.kafka import (
    DshNotificationEnvelope,
    NotificationPublisher,
    NullNotificationPublisher,
)
from dsh_base_agent.adapters.mcp.gateway import GatewayRunContext, ToolGateway
from dsh_base_agent.artifacts import (
    ArtifactNotFoundError,
    ArtifactStore,
    ArtifactStoreConfig,
)
from dsh_base_agent.control.auth import Authorizer, Principal, ReadOnlyByDefaultAuthorizer
from dsh_base_agent.control.models import (
    TERMINAL_RUN_STATUSES,
    ArtifactRecord,
    AttemptStatus,
    AuditRecord,
    ConversationRecord,
    ConversationStatus,
    DispatchState,
    EventSource,
    PlanApprovalDecision,
    PlanApprovalResponse,
    PlanRecord,
    PlanStatus,
    RequiredActionRecord,
    RequiredActionSource,
    RequiredActionStatus,
    RequiredActionType,
    RunAttempt,
    RunEvent,
    RunPhase,
    RunRecord,
    RunStatus,
    WorkLease,
    WorkResourceType,
    new_id,
    utc_now,
)
from dsh_base_agent.control.plan_react import (
    CompiledWorkflow,
    PlanCompilationError,
    PlanningAuthorizer,
    PlanSubmissionCollector,
    compile_plan,
    compiled_workflow,
    execution_agent,
    planning_agent,
)
from dsh_base_agent.sdk.agent import Agent
from dsh_base_agent.sdk.user_input import (
    AskUserQuestionRequest,
    validate_user_input_response,
)
from dsh_base_agent.store import (
    ControlStore,
    ControlStoreConfig,
    LeaseLostError,
    PostgresControlStore,
    RequiredActionNotFoundError,
    StoreError,
    request_digest,
)

_LOGGER = logging.getLogger(__name__)


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


class PlanDecisionConflict(RunTransitionError):
    pass


@dataclass(frozen=True, slots=True)
class RunView:
    run: RunRecord
    attempts: tuple[RunAttempt, ...]
    required_action: RequiredActionRecord | None = None


@dataclass(frozen=True, slots=True)
class PlanDecisionView:
    run: RunRecord
    plan: PlanRecord


@dataclass(slots=True)
class _ConversationRuntimeHolder:
    runtime: DshRuntime
    tool_gateway: ToolGateway | None
    memory_gateway: MemoryContextGateway | None
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
        store_config: ControlStoreConfig | None = None,
        artifact_store: ArtifactStore | None = None,
        artifact_store_config: ArtifactStoreConfig | None = None,
        runtime_factory: DshRuntimeFactory | None = None,
        authorizer: Authorizer | None = None,
        notification_publisher: NotificationPublisher | None = None,
        auto_execute: bool = True,
        user_input_timeout_seconds: float = 86_400.0,
    ) -> None:
        if user_input_timeout_seconds <= 0:
            raise ValueError("user_input_timeout_seconds must be positive")
        self.workspace = Path(workspace).expanduser().resolve()
        self.runtime_config = runtime
        self.user_input_timeout_seconds = user_input_timeout_seconds
        if store is not None and store_config is not None:
            raise ValueError("store and store_config cannot both be provided")
        self.store = store or (store_config or ControlStoreConfig()).create(
            workspace=self.workspace
        )
        if artifact_store is not None and artifact_store_config is not None:
            raise ValueError("artifact_store and artifact_store_config cannot both be provided")
        self.artifact_store = artifact_store or (
            artifact_store_config or ArtifactStoreConfig()
        ).create(workspace=self.workspace)
        if auto_execute and isinstance(self.store, PostgresControlStore):
            raise ValueError(
                "PostgresControlStore requires auto_execute=False; "
                "run execution in an independent WorkerService"
            )
        self.runtime_factory = runtime_factory or OfficialDshRuntimeFactory(runtime)
        self.authorizer = authorizer or ReadOnlyByDefaultAuthorizer()
        self.notification_publisher = notification_publisher or NullNotificationPublisher()
        self.auto_execute = auto_execute
        self._agents: dict[str, Agent] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._conversation_tasks: dict[str, asyncio.Task[None]] = {}
        self._conversation_runtimes: dict[str, _ConversationRuntimeHolder] = {}
        self._run_done: dict[str, asyncio.Event] = {}
        self._active_runtimes: dict[str, DshRuntime] = {}
        self._run_locks: dict[str, asyncio.Lock] = {}
        self._deferred_schedules: set[str] = set()
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
        try:
            await self.artifact_store.initialize()
        except BaseException:
            await self.store.close()
            raise
        try:
            await self.notification_publisher.start()
        except BaseException:
            await self.artifact_store.close()
            await self.store.close()
            raise
        self._started = True
        if self.auto_execute:
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
            *(
                holder.tool_gateway.close()
                for holder in holders
                if holder.tool_gateway is not None
            ),
            *(
                holder.memory_gateway.close()
                for holder in holders
                if holder.memory_gateway is not None
            ),
            return_exceptions=True,
        )
        for event in self._run_done.values():
            event.set()
        self._run_done.clear()
        self._deferred_schedules.clear()
        await self.notification_publisher.close()
        await self.artifact_store.close()
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
        if agent.plan_react is not None:
            raise RunTransitionError(
                "Plan-ReAct currently requires a standalone Run because approval may "
                "outlive the live DSH Conversation Session"
            )
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
            if self.auto_execute:
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
        if agent.allow_user_input:
            raise RunTransitionError(
                "Agents with user input enabled must run inside a Conversation"
            )
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
            phase=(RunPhase.PLANNING if agent.plan_react is not None else RunPhase.DIRECT),
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
            if self.auto_execute:
                await self._schedule(persisted.run_id)
        return persisted

    async def wait(self, run_id: str) -> RunRecord:
        await self._ensure_started()
        run = await self.store.get_run(run_id)
        if not self.auto_execute:
            while run.status not in {*TERMINAL_RUN_STATUSES, RunStatus.WAITING}:
                await asyncio.sleep(0.05)
                run = await self.store.get_run(run_id)
            return run
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
        action: RequiredActionRecord | None = None
        if run.required_action_id is not None:
            candidate = await self.store.get_required_action(run.required_action_id)
            if candidate.status is RequiredActionStatus.PENDING:
                action = candidate
        return RunView(
            run=run,
            attempts=await self.store.list_attempts(run_id),
            required_action=action,
        )

    async def get_plan(self, principal: Principal, run_id: str) -> PlanRecord:
        """Return the immutable proposal and its current approval decision."""

        await self._ensure_started()
        await self._owned_run(principal, run_id)
        return await self.store.get_plan(run_id)

    async def approve_plan(
        self,
        principal: Principal,
        run_id: str,
        *,
        plan_id: str,
        plan_revision: int,
        plan_sha256: str,
        comment: str | None = None,
    ) -> PlanDecisionView:
        """Approve exactly the proposal the caller inspected, then queue execution."""

        return await self._resolve_plan_action(
            principal,
            run_id,
            response=PlanApprovalResponse(
                decision=PlanApprovalDecision.APPROVE,
                plan_id=plan_id,
                plan_revision=plan_revision,
                plan_sha256=plan_sha256,
                comment=comment,
            ),
        )

    async def reject_plan(
        self,
        principal: Principal,
        run_id: str,
        *,
        plan_id: str,
        plan_revision: int,
        plan_sha256: str,
        comment: str | None = None,
    ) -> PlanDecisionView:
        """Reject exactly the proposal the caller inspected and cancel the Run."""

        return await self._resolve_plan_action(
            principal,
            run_id,
            response=PlanApprovalResponse(
                decision=PlanApprovalDecision.REJECT,
                plan_id=plan_id,
                plan_revision=plan_revision,
                plan_sha256=plan_sha256,
                comment=comment,
            ),
        )

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

    async def open_artifact(
        self,
        principal: Principal,
        run_id: str,
        artifact_id: str,
    ) -> tuple[ArtifactRecord, AsyncIterator[bytes]]:
        """Authorize a Run-scoped artifact and stream its body from ArtifactStore."""

        await self._ensure_started()
        await self._owned_run(principal, run_id)
        records = await self.store.list_artifacts(run_id)
        try:
            record = next(item for item in records if item.artifact_id == artifact_id)
        except StopIteration as exc:
            raise ArtifactNotFoundError(f"Artifact '{artifact_id}' was not found") from exc
        return record, await self.artifact_store.open(record.location)

    async def cancel(self, principal: Principal, run_id: str) -> RunRecord:
        await self._ensure_started()
        cancelled_plan: PlanRecord | None = None
        cancelled_action: RequiredActionRecord | None = None
        was_waiting_for_action = False
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            current = await self._owned_run(principal, run_id)
            if current.status in TERMINAL_RUN_STATUSES:
                return current
            now = utc_now()
            if current.required_action_id is not None:
                action = await self.store.get_required_action(current.required_action_id)
                if action.status is not RequiredActionStatus.PENDING:
                    raise RunTransitionError(
                        "Run points to a RequiredAction that is no longer pending"
                    )
                was_waiting_for_action = True
                cancelled_action = action.model_copy(
                    update={
                        "status": RequiredActionStatus.CANCELLED,
                        "revision": action.revision + 1,
                        "updated_at": now,
                    }
                )
            cancelled = current.model_copy(
                update={
                    "status": RunStatus.CANCELLED,
                    "active_attempt_id": (
                        None
                        if current.phase is RunPhase.WAITING_APPROVAL
                        or was_waiting_for_action
                        else current.active_attempt_id
                    ),
                    "required_action_id": None,
                    "cancel_requested": True,
                    "revision": current.revision + 1,
                    "error": "Run cancellation requested",
                    "updated_at": now,
                }
            )
            if current.phase is RunPhase.WAITING_APPROVAL:
                if cancelled_action is None:
                    raise RunTransitionError(
                        "Plan-ReAct Run has no pending Plan RequiredAction"
                    )
                plan = await self.store.get_plan(run_id)
                cancelled_plan = plan.model_copy(
                    update={
                        "status": PlanStatus.REJECTED,
                        "revision": plan.revision + 1,
                        "decided_by": principal.principal_id,
                        "decision_comment": "Run cancellation requested",
                        "decided_at": now,
                        "updated_at": now,
                    }
                )
                await self.store.decide_plan(
                    cancelled,
                    cancelled_plan,
                    cancelled_action,
                    expected_run_revision=current.revision,
                    expected_plan_revision=plan.revision,
                    expected_action_revision=cancelled_action.revision - 1,
                )
            elif current.active_attempt_id is not None:
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
                            "dispatch_state": DispatchState.SETTLED,
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
                        settled_action=cancelled_action,
                        expected_action_revision=(
                            None if cancelled_action is None else cancelled_action.revision - 1
                        ),
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
        elif current.active_attempt_id is not None and not was_waiting_for_action:
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
        if cancelled_plan is not None:
            await self.store.append_event(
                run_id=run_id,
                attempt_id=current.active_attempt_id,
                source=EventSource.CONTROL,
                kind="plan.rejected",
                data={
                    "plan_id": cancelled_plan.plan_id,
                    "plan_revision": cancelled_plan.revision,
                    "plan_sha256": cancelled_plan.plan_sha256,
                    "actor": principal.principal_id,
                    "reason": "run_cancelled",
                },
            )
        if cancelled_action is not None:
            await self.store.append_event(
                run_id=run_id,
                attempt_id=current.active_attempt_id,
                source=EventSource.CONTROL,
                kind="run.action_cancelled",
                data={
                    "action_id": cancelled_action.action_id,
                    "type": cancelled_action.type.value,
                    "actor": principal.principal_id,
                },
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
            if current.phase is RunPhase.WAITING_APPROVAL:
                raise RunTransitionError(
                    "a rejected Plan cannot be retried in place; submit a new Run to re-plan"
                )
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
        if self.auto_execute:
            await self._schedule(run_id)
        return queued

    async def resume(
        self,
        principal: Principal,
        run_id: str,
        *,
        action_id: str,
        expected_revision: int,
        response: dict[str, Any],
    ) -> RunView:
        await self._ensure_started()
        snapshot = await self._owned_run(principal, run_id)
        if snapshot.status is not RunStatus.WAITING:
            raise RunTransitionError("only a waiting Run can be resumed")
        if snapshot.required_action_id != action_id:
            raise RunTransitionError("RequiredAction is no longer current for this Run")
        if snapshot.revision != expected_revision:
            raise RunTransitionError("Run revision changed; refresh before resuming")
        try:
            pending_action = await self.store.get_required_action(action_id)
        except RequiredActionNotFoundError as exc:
            raise RunTransitionError(str(exc)) from exc
        if pending_action.type is RequiredActionType.PLAN_APPROVAL:
            try:
                plan_response = PlanApprovalResponse.model_validate(response)
            except ValueError as exc:
                raise RunTransitionError(f"invalid plan-approval response: {exc}") from exc
            await self._resolve_plan_action(
                principal,
                run_id,
                response=plan_response,
                action_id=action_id,
                expected_run_revision=expected_revision,
            )
            return await self.view(principal, run_id)

        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            run = await self._owned_run(principal, run_id)
            if run.status is not RunStatus.WAITING:
                raise RunTransitionError("only a waiting Run can be resumed")
            if run.required_action_id != action_id:
                raise RunTransitionError("RequiredAction is no longer current for this Run")
            if run.revision != expected_revision:
                raise RunTransitionError("Run revision changed; refresh before resuming")
            if run.conversation_id is None:
                raise RunTransitionError("user-input continuation requires a Conversation Run")
            try:
                action = await self.store.get_required_action(action_id)
            except RequiredActionNotFoundError as exc:
                raise RunTransitionError(str(exc)) from exc
            if action.run_id != run_id or action.conversation_id != run.conversation_id:
                raise RunTransitionError("RequiredAction does not belong to this Run")
            if (
                action.tenant_id != principal.tenant_id
                or action.principal_id != principal.principal_id
            ):
                raise RunAccessDenied("RequiredAction belongs to another tenant or principal")
            if action.status is not RequiredActionStatus.PENDING:
                raise RunTransitionError("RequiredAction is no longer pending")
            if action.expires_at is not None and action.expires_at <= utc_now():
                if run.active_attempt_id != action.attempt_id:
                    raise RunTransitionError(
                        "expired RequiredAction does not match the active Attempt"
                    )
                attempt = await self.store.get_attempt(action.attempt_id)
                now = utc_now()
                expired_action = action.model_copy(
                    update={
                        "status": RequiredActionStatus.EXPIRED,
                        "revision": action.revision + 1,
                        "updated_at": now,
                    }
                )
                expired_attempt = attempt.model_copy(
                    update={
                        "status": AttemptStatus.CANCELLED,
                        "dispatch_state": DispatchState.SETTLED,
                        "revision": attempt.revision + 1,
                        "error": "RequiredAction expired",
                        "finished_at": now,
                        "updated_at": now,
                    }
                )
                expired_run = run.model_copy(
                    update={
                        "status": RunStatus.CANCELLED,
                        "revision": run.revision + 1,
                        "active_attempt_id": None,
                        "required_action_id": None,
                        "error": "RequiredAction expired",
                        "updated_at": now,
                    }
                )
                try:
                    await self.store.settle_attempt(
                        expired_run,
                        expired_attempt,
                        expected_run_revision=run.revision,
                        expected_attempt_revision=attempt.revision,
                        settled_action=expired_action,
                        expected_action_revision=action.revision,
                    )
                except StoreError as exc:
                    raise RunTransitionError(
                        "RequiredAction changed while it was being expired"
                    ) from exc
                self._run_done.setdefault(run_id, asyncio.Event()).set()
                await self.store.append_event(
                    run_id=run_id,
                    attempt_id=attempt.attempt_id,
                    source=EventSource.CONTROL,
                    kind="run.action_expired",
                    data={"action_id": action.action_id, "type": action.type.value},
                )
                await self._audit_run(
                    principal,
                    expired_run,
                    "run.required_action.expire",
                    "expired",
                    attempt_id=attempt.attempt_id,
                    data={"action_id": action.action_id, "type": action.type.value},
                )
                raise RunTransitionError("RequiredAction has expired")
            if action.type is not RequiredActionType.USER_INPUT:
                raise RunTransitionError(
                    f"RequiredAction type '{action.type.value}' cannot be submitted here"
                )
            try:
                validated = validate_user_input_response(
                    response,
                    action.payload.get("questions"),
                )
            except ValueError as exc:
                raise RunTransitionError(f"invalid user-input response: {exc}") from exc
            now = utc_now()
            answered_action = action.model_copy(
                update={
                    "status": RequiredActionStatus.ANSWERED,
                    "response": validated.model_dump(mode="json"),
                    "revision": action.revision + 1,
                    "answered_by": principal.principal_id,
                    "answered_at": now,
                    "updated_at": now,
                }
            )
            queued = run.model_copy(
                update={
                    "status": RunStatus.QUEUED,
                    "revision": run.revision + 1,
                    "active_attempt_id": None,
                    "error": None,
                    "updated_at": now,
                }
            )
            try:
                await self.store.answer_required_action(
                    queued,
                    answered_action,
                    expected_run_revision=run.revision,
                    expected_action_revision=action.revision,
                )
            except StoreError as exc:
                raise RunTransitionError(
                    "RequiredAction or Run changed concurrently; refresh before resuming"
                ) from exc
        await self.store.append_event(
            run_id=run_id,
            attempt_id=action.attempt_id,
            source=EventSource.CONTROL,
            kind="run.action_submitted",
            data={
                "action_id": action.action_id,
                "type": action.type.value,
                "submitted_by": principal.principal_id,
            },
        )
        await self._audit_run(
            principal,
            queued,
            "run.required_action.answer",
            "accepted",
            attempt_id=action.attempt_id,
            data={"action_id": action.action_id, "type": action.type.value},
        )
        self._run_done[run_id] = asyncio.Event()
        if self.auto_execute:
            await self._schedule_conversation(run.conversation_id, run_id)
        return await self.view(principal, run_id)

    async def _resolve_plan_action(
        self,
        principal: Principal,
        run_id: str,
        *,
        response: PlanApprovalResponse,
        action_id: str | None = None,
        expected_run_revision: int | None = None,
    ) -> PlanDecisionView:
        await self._ensure_started()
        lock = self._run_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            current = await self._owned_run(principal, run_id)
            if (
                current.status is not RunStatus.WAITING
                or current.phase is not RunPhase.WAITING_APPROVAL
            ):
                raise PlanDecisionConflict("Run is not waiting for a Plan-ReAct decision")
            if expected_run_revision is not None and current.revision != expected_run_revision:
                raise PlanDecisionConflict("Run revision changed; refresh before resuming")
            current_action_id = current.required_action_id
            if current_action_id is None:
                raise PlanDecisionConflict("Run has no pending Plan RequiredAction")
            if action_id is not None and current_action_id != action_id:
                raise PlanDecisionConflict("RequiredAction is no longer current for this Run")
            try:
                action = await self.store.get_required_action(current_action_id)
            except RequiredActionNotFoundError as exc:
                raise PlanDecisionConflict(str(exc)) from exc
            if action.run_id != run_id or action.attempt_id != current.active_attempt_id:
                raise PlanDecisionConflict("Plan RequiredAction does not belong to this Run")
            if action.conversation_id != current.conversation_id:
                raise PlanDecisionConflict("Plan RequiredAction has invalid Conversation ownership")
            if (
                action.tenant_id != principal.tenant_id
                or action.principal_id != principal.principal_id
            ):
                raise RunAccessDenied("RequiredAction belongs to another tenant or principal")
            if action.type is not RequiredActionType.PLAN_APPROVAL:
                raise PlanDecisionConflict("current RequiredAction is not a Plan approval")
            if action.status is not RequiredActionStatus.PENDING:
                raise PlanDecisionConflict("Plan RequiredAction is no longer pending")
            plan = await self.store.get_plan(run_id)
            if plan.status is not PlanStatus.PENDING:
                raise PlanDecisionConflict(f"Plan is already {plan.status.value}")
            if plan.plan_id != response.plan_id:
                raise PlanDecisionConflict("plan_id does not match the pending Plan")
            if plan.revision != response.plan_revision:
                raise PlanDecisionConflict("Plan revision changed; reload it before deciding")
            if plan.plan_sha256 != response.plan_sha256:
                raise PlanDecisionConflict("plan_sha256 does not match the pending Plan")
            if (
                action.payload.get("plan_id") != plan.plan_id
                or action.payload.get("plan_revision") != plan.revision
                or action.payload.get("plan_sha256") != plan.plan_sha256
            ):
                raise PlanDecisionConflict("Plan RequiredAction does not match the stored Plan")
            approved = response.decision is PlanApprovalDecision.APPROVE
            if approved:
                # Verify both the immutable template and arguments again at the
                # approval boundary. Execution repeats the same verification.
                try:
                    compiled_workflow(plan)
                except PlanCompilationError as exc:
                    raise PlanDecisionConflict(str(exc)) from exc
            now = utc_now()
            decision = PlanStatus.APPROVED if approved else PlanStatus.REJECTED
            decided_plan = plan.model_copy(
                update={
                    "status": decision,
                    "revision": plan.revision + 1,
                    "decided_by": principal.principal_id,
                    "decision_comment": response.comment,
                    "decided_at": now,
                    "updated_at": now,
                }
            )
            resolved_action = action.model_copy(
                update={
                    "status": RequiredActionStatus.CONSUMED,
                    "response": response.model_dump(mode="json"),
                    "revision": action.revision + 1,
                    "answered_by": principal.principal_id,
                    "answered_at": now,
                    "updated_at": now,
                }
            )
            decided_run = current.model_copy(
                update={
                    "status": RunStatus.QUEUED if approved else RunStatus.CANCELLED,
                    "phase": RunPhase.EXECUTING if approved else current.phase,
                    "revision": current.revision + 1,
                    "active_attempt_id": None,
                    "required_action_id": None,
                    "error": None if approved else "Plan rejected by approver",
                    "cancel_requested": not approved,
                    "updated_at": now,
                }
            )
            try:
                await self.store.decide_plan(
                    decided_run,
                    decided_plan,
                    resolved_action,
                    expected_run_revision=current.revision,
                    expected_plan_revision=plan.revision,
                    expected_action_revision=action.revision,
                )
            except StoreError as exc:
                raise PlanDecisionConflict(str(exc)) from exc

        outcome = "approved" if approved else "rejected"
        await self.store.append_event(
            run_id=run_id,
            attempt_id=action.attempt_id,
            source=EventSource.CONTROL,
            kind="run.action_submitted",
            data={
                "action_id": action.action_id,
                "type": action.type.value,
                "decision": response.decision.value,
                "submitted_by": principal.principal_id,
            },
        )
        await self.store.append_event(
            run_id=run_id,
            attempt_id=current.active_attempt_id,
            source=EventSource.CONTROL,
            kind=f"plan.{outcome}",
            data={
                "plan_id": decided_plan.plan_id,
                "plan_revision": decided_plan.revision,
                "plan_sha256": decided_plan.plan_sha256,
                "action_id": action.action_id,
                "actor": principal.principal_id,
            },
        )
        await self._audit_run(
            principal,
            decided_run,
            f"plan.{outcome}",
            "success",
            data={
                "plan_id": decided_plan.plan_id,
                "plan_revision": decided_plan.revision,
                "plan_sha256": decided_plan.plan_sha256,
                "action_id": action.action_id,
            },
        )
        if approved and self.auto_execute:
            await self._schedule(run_id)
        return PlanDecisionView(run=decided_run, plan=decided_plan)

    async def execute_leased_work(self, lease: WorkLease) -> bool:
        """Execute at most one Run fenced to this Worker.

        Returning whether a Run was selected lets the Worker release idle
        Conversation leases without treating a persisted Session as closed.
        """

        await self._ensure_started()
        if self.auto_execute:
            raise RuntimeError("leased execution requires auto_execute=False")
        if lease.resource_type is WorkResourceType.RUN:
            run = await self.store.get_run(lease.resource_id)
            if run.status is not RunStatus.QUEUED:
                return False
            await self._execute(lease.resource_id, lease=lease)
            return True
        if lease.resource_type is not WorkResourceType.CONVERSATION:
            raise ValueError(f"unsupported work resource: {lease.resource_type}")
        conversation = await self.store.get_conversation(lease.resource_id)
        if conversation.status is not ConversationStatus.ACTIVE:
            return False
        # A Conversation belongs to its persisted DSH Home and Session ID, not to
        # a particular Worker process.  A replacement Worker deliberately creates
        # a fresh Harness against those same persisted identifiers and submits the
        # next Turn; it does not replay an interrupted Run.
        runs = await self.store.list_conversation_runs(conversation.conversation_id)
        if any(run.status in {RunStatus.RUNNING, RunStatus.WAITING} for run in runs):
            return False
        queued = sorted(
            (run for run in runs if run.status is RunStatus.QUEUED),
            key=lambda run: run.sequence or 0,
        )
        if not queued:
            return False
        await self._execute_conversation_run(
            conversation,
            queued[0].run_id,
            lease=lease,
        )
        return True

    async def reconcile_expired_worker_leases(self) -> int:
        """Atomically settle and record attempts abandoned by expired leases."""

        await self._ensure_started()
        recovered = await self.store.interrupt_expired_attempts()
        return len(recovered)

    async def discard_conversation_runtime(self, conversation_id: str) -> None:
        """Close a sticky Conversation Runtime after its Worker loses ownership."""

        await self._discard_conversation_runtime(conversation_id)

    async def _schedule(self, run_id: str) -> None:
        async with self._task_lock:
            existing = self._tasks.get(run_id)
            if existing is not None and not existing.done():
                if run_id not in self._deferred_schedules:
                    self._deferred_schedules.add(run_id)
                    existing.add_done_callback(
                        lambda _: asyncio.create_task(self._run_deferred_schedule(run_id))
                    )
                return
            task = asyncio.create_task(self._execute(run_id), name=f"run-{run_id}")
            self._tasks[run_id] = task

    async def _run_deferred_schedule(self, run_id: str) -> None:
        try:
            if not self._closed:
                await self._schedule(run_id)
        finally:
            self._deferred_schedules.discard(run_id)

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

    async def _drain_conversation(
        self,
        conversation_id: str,
        *,
        lease: WorkLease | None = None,
    ) -> None:
        try:
            while not self._closed:
                conversation = await self.store.get_conversation(conversation_id)
                if conversation.status is not ConversationStatus.ACTIVE:
                    return
                runs = await self.store.list_conversation_runs(conversation_id)
                if any(run.status in {RunStatus.RUNNING, RunStatus.WAITING} for run in runs):
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
                    lease=lease,
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
        if self._closed or not self.auto_execute:
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
                        "dispatch_state": DispatchState.UNKNOWN,
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
        active_conversations = await self.store.list_conversations(
            statuses=(ConversationStatus.ACTIVE.value,)
        )
        for conversation in active_conversations:
            runs = await self.store.list_conversation_runs(conversation.conversation_id)
            queued = next((run for run in runs if run.status is RunStatus.QUEUED), None)
            if queued is not None:
                await self._schedule_conversation(
                    conversation.conversation_id,
                    queued.run_id,
                )

    async def _execute(self, run_id: str, *, lease: WorkLease | None = None) -> None:
        attempt: RunAttempt | None = None
        run: RunRecord | None = None
        gateway: ToolGateway | None = None
        memory_gateway: MemoryContextGateway | None = None
        runtime: DshRuntime | None = None
        plan_collector: PlanSubmissionCollector | None = None
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
                    phase=current.phase,
                    worker_id=None if lease is None else lease.worker_id,
                    lease_token=None if lease is None else lease.lease_token,
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
                    lease=lease,
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

            async def write_artifact(
                name: str,
                media_type: str,
                content: bytes,
            ) -> ArtifactRecord:
                return await self._store_tool_artifact(
                    run,
                    attempt,
                    name=name,
                    media_type=media_type,
                    content=content,
                )

            registered_agent = self._agent(run.agent_id)
            workflow: CompiledWorkflow | None = None
            approved_plan: PlanRecord | None = None
            if run.phase is RunPhase.PLANNING:
                agent, plan_collector = planning_agent(registered_agent)
            elif run.phase is RunPhase.EXECUTING:
                approved_plan = await self.store.get_plan(run_id)
                if approved_plan.status is not PlanStatus.APPROVED:
                    raise PlanCompilationError("Run execution requires an approved Plan")
                workflow = compiled_workflow(approved_plan)
                agent = execution_agent(registered_agent, approved_plan)
            elif run.phase is RunPhase.DIRECT:
                agent = registered_agent
            else:
                raise RunTransitionError(f"Run phase '{run.phase.value}' is not executable")

            if agent.tools:
                gateway = ToolGateway(
                    agent=agent,
                    authorizer=(
                        PlanningAuthorizer(self.authorizer)
                        if run.phase is RunPhase.PLANNING
                        else self.authorizer
                    ),
                    write_event=write_event,
                    write_audit=write_audit,
                    write_artifact=write_artifact,
                )
                await gateway.start()
                gateway.bind(
                    GatewayRunContext(
                        principal=Principal(run.tenant_id, run.principal_id),
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                    )
                )
            if agent.memory_providers:
                memory_gateway = MemoryContextGateway(
                    agent=agent,
                    authorizer=self.authorizer,
                    write_event=write_event,
                    write_audit=write_audit,
                )
                await memory_gateway.start()
                memory_gateway.bind(
                    MemoryRunContext(
                        principal=Principal(run.tenant_id, run.principal_id),
                        conversation_id=None,
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                        dsh_session_id=attempt.dsh_session_id,
                        query=run.input,
                    )
                )
            dsh_home = self._agent_dsh_home(run.agent_fingerprint)
            runtime = self.runtime_factory.create(
                agent,
                workspace=self.workspace,
                dsh_home=dsh_home,
                attempt_id=attempt.attempt_id,
                tool_gateway_url=gateway.url if gateway is not None else None,
                memory_context_url=(
                    memory_gateway.url if memory_gateway is not None else None
                ),
                memory_context_token=(
                    memory_gateway.token if memory_gateway is not None else None
                ),
            )
            self._active_runtimes[run_id] = runtime
            await self._set_dispatch_state(
                attempt.attempt_id,
                DispatchState.DISPATCHING,
                lease=lease,
            )
            dispatch_accepted = False

            async def on_dsh_event(event: dict[str, Any]) -> None:
                nonlocal dispatch_accepted
                if not dispatch_accepted:
                    await self._set_dispatch_state(
                        attempt.attempt_id,
                        DispatchState.ACCEPTED,
                        lease=lease,
                    )
                    dispatch_accepted = True
                projected = _project_dsh_event(event)
                if projected is None:
                    return
                kind, data = projected
                await write_event(EventSource.DSH, kind, data)

            notification_handler = self._notification_handler(run, attempt)
            if workflow is None:
                result = await runtime.run(
                    run.input,
                    session_id=attempt.dsh_session_id,
                    on_event=on_dsh_event,
                    on_notification=notification_handler,
                )
                if result.session_id != attempt.dsh_session_id:
                    raise RuntimeError("DSH returned a different Session ID")
                if not dispatch_accepted:
                    await self._set_dispatch_state(
                        attempt.attempt_id,
                        DispatchState.ACCEPTED,
                        lease=lease,
                    )
                if result.finish_reason != "completed":
                    raise RuntimeError(
                        f"DSH Run did not complete: {result.finish_reason or 'unknown'}"
                    )
                if run.phase is RunPhase.PLANNING:
                    if plan_collector is None or plan_collector.proposal is None:
                        raise PlanCompilationError(
                            "planning completed without exactly one submit_plan Tool call"
                        )
                    plan, _ = compile_plan(
                        run=run,
                        agent=registered_agent,
                        proposal=plan_collector.proposal,
                    )
                    await self._settle_planning(
                        run,
                        attempt,
                        plan,
                        result.final_response,
                        result.finish_reason,
                        lease=lease,
                    )
                else:
                    await self._settle_success(
                        run,
                        attempt,
                        result.final_response,
                        result.finish_reason,
                        lease=lease,
                    )
            else:
                if approved_plan is None:
                    raise PlanCompilationError("approved Plan disappeared before execution")
                await write_event(
                    EventSource.CONTROL,
                    "workflow.started",
                    {
                        "plan_id": approved_plan.plan_id,
                        "plan_sha256": approved_plan.plan_sha256,
                        "workflow_id": workflow.workflow_id,
                        "workflow_version": workflow.workflow_version,
                        "script_sha256": workflow.script_sha256,
                        "args_sha256": workflow.args_sha256,
                    },
                )

                async def on_workflow_notification(
                    method: str,
                    payload: dict[str, Any],
                ) -> None:
                    nonlocal dispatch_accepted
                    if not dispatch_accepted:
                        await self._set_dispatch_state(
                            attempt.attempt_id,
                            DispatchState.ACCEPTED,
                            lease=lease,
                        )
                        dispatch_accepted = True
                    await notification_handler(method, payload)

                workflow_result = await runtime.execute_workflow(
                    DshWorkflowRequest(
                        workflow_id=workflow.workflow_id,
                        workflow_version=workflow.workflow_version,
                        script=workflow.script,
                        script_sha256=workflow.script_sha256,
                        args=workflow.args,
                        args_sha256=workflow.args_sha256,
                    ),
                    session_id=attempt.dsh_session_id,
                    on_notification=on_workflow_notification,
                )
                if workflow_result.session_id != attempt.dsh_session_id:
                    raise RuntimeError("DSH Workflow returned a different Session ID")
                if not dispatch_accepted:
                    await self._set_dispatch_state(
                        attempt.attempt_id,
                        DispatchState.ACCEPTED,
                        lease=lease,
                    )
                if workflow_result.stop_reason != "completed":
                    detail = workflow_result.error or workflow_result.stop_reason
                    raise RuntimeError(f"DSH Workflow did not complete: {detail}")
                await write_event(
                    EventSource.CONTROL,
                    "workflow.completed",
                    {
                        "plan_id": approved_plan.plan_id,
                        "workflow_run_id": workflow_result.workflow_run_id,
                        "agents_started": workflow_result.agents_started,
                    },
                )
                output = json.dumps(
                    workflow_result.value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                await self._settle_success(
                    run,
                    attempt,
                    output,
                    workflow_result.stop_reason,
                    lease=lease,
                )
        except asyncio.CancelledError:
            # ``cancel`` owns the durable cancellation transition.
            raise
        except LeaseLostError:
            return
        except BaseException as exc:
            if run is not None and attempt is not None:
                try:
                    await self._settle_failure(run, attempt, exc, lease=lease)
                except LeaseLostError:
                    return
        finally:
            self._active_runtimes.pop(run_id, None)
            if runtime is not None:
                await runtime.close()
            if gateway is not None:
                if attempt is not None:
                    gateway.release(attempt.attempt_id)
                await gateway.close()
            if memory_gateway is not None:
                if attempt is not None:
                    memory_gateway.release(attempt.attempt_id)
                await memory_gateway.close()

    async def _execute_conversation_run(
        self,
        conversation: ConversationRecord,
        run_id: str,
        *,
        lease: WorkLease | None = None,
    ) -> bool:
        attempt: RunAttempt | None = None
        run: RunRecord | None = None
        holder: _ConversationRuntimeHolder | None = None
        gateway_bound = False
        pending_action: RequiredActionRecord | None = None
        turn_input = ""
        try:
            lock = self._run_locks.setdefault(run_id, asyncio.Lock())
            async with lock:
                current = await self.store.get_run(run_id)
                if current.status is not RunStatus.QUEUED:
                    return current.status is RunStatus.SUCCEEDED
                agent = self._agent(current.agent_id)
                if agent.fingerprint != conversation.agent_fingerprint:
                    raise RunTransitionError("registered Agent no longer matches the Conversation")
                attempt = RunAttempt(
                    run_id=run_id,
                    number=current.attempt_count + 1,
                    dsh_session_id=conversation.dsh_session_id,
                    phase=current.phase,
                    worker_id=None if lease is None else lease.worker_id,
                    lease_token=None if lease is None else lease.lease_token,
                )
                now = utc_now()
                consumed_action: RequiredActionRecord | None = None
                expected_action_revision: int | None = None
                if current.required_action_id is None:
                    turn_input = current.input
                else:
                    action = await self.store.get_required_action(current.required_action_id)
                    if action.status is not RequiredActionStatus.ANSWERED:
                        raise RunTransitionError(
                            "queued continuation does not have an answered RequiredAction"
                        )
                    if action.response is None:
                        raise RunTransitionError("answered RequiredAction has no response")
                    turn_input = _render_required_action_response(action)
                    expected_action_revision = action.revision
                    consumed_action = action.model_copy(
                        update={
                            "status": RequiredActionStatus.CONSUMED,
                            "revision": action.revision + 1,
                            "consumed_by_attempt_id": attempt.attempt_id,
                            "updated_at": now,
                        }
                    )
                run = current.model_copy(
                    update={
                        "status": RunStatus.RUNNING,
                        "revision": current.revision + 1,
                        "attempt_count": attempt.number,
                        "active_attempt_id": attempt.attempt_id,
                        "required_action_id": None,
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
                    lease=lease,
                    consumed_action=consumed_action,
                    expected_action_revision=expected_action_revision,
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
                    **(
                        {"resumed_action_id": consumed_action.action_id}
                        if consumed_action is not None
                        else {}
                    ),
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

            async def write_artifact(
                name: str,
                media_type: str,
                content: bytes,
            ) -> ArtifactRecord:
                return await self._store_tool_artifact(
                    run,
                    attempt,
                    name=name,
                    media_type=media_type,
                    content=content,
                )

            agent = self._agent(run.agent_id)
            holder = await self._conversation_runtime_holder(
                conversation,
                agent,
                attempt.attempt_id,
            )
            if holder.tool_gateway is not None:
                async def request_user_input(request: AskUserQuestionRequest) -> str:
                    nonlocal pending_action
                    if pending_action is not None:
                        raise RunTransitionError(
                            "this Attempt already created a RequiredAction"
                        )
                    now = utc_now()
                    action = RequiredActionRecord(
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                        conversation_id=conversation.conversation_id,
                        tenant_id=run.tenant_id,
                        principal_id=run.principal_id,
                        type=RequiredActionType.USER_INPUT,
                        source=RequiredActionSource.MODEL,
                        payload=request.model_dump(mode="json"),
                        expires_at=now + timedelta(seconds=self.user_input_timeout_seconds),
                        created_at=now,
                        updated_at=now,
                    )
                    await self.store.enter_required_action(action, lease=lease)
                    pending_action = action
                    await self.store.append_event(
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                        source=EventSource.CONTROL,
                        kind="run.requires_action",
                        data={
                            "action_id": action.action_id,
                            "type": action.type.value,
                            "source": action.source.value,
                            "payload": action.payload,
                            "expires_at": action.expires_at.isoformat()
                            if action.expires_at is not None
                            else None,
                        },
                    )
                    await self._audit_run(
                        Principal(run.tenant_id, run.principal_id),
                        run,
                        "run.required_action.create",
                        "waiting",
                        attempt_id=attempt.attempt_id,
                        data={"action_id": action.action_id, "type": action.type.value},
                    )
                    return action.action_id

                holder.tool_gateway.bind(
                    GatewayRunContext(
                        principal=Principal(run.tenant_id, run.principal_id),
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                    ),
                    write_event=write_event,
                    write_audit=write_audit,
                    write_artifact=write_artifact,
                    request_user_input=(request_user_input if agent.allow_user_input else None),
                )
                gateway_bound = True
            if holder.memory_gateway is not None:
                holder.memory_gateway.bind(
                    MemoryRunContext(
                        principal=Principal(run.tenant_id, run.principal_id),
                        conversation_id=conversation.conversation_id,
                        run_id=run_id,
                        attempt_id=attempt.attempt_id,
                        dsh_session_id=conversation.dsh_session_id,
                        query=turn_input,
                    ),
                    write_event=write_event,
                    write_audit=write_audit,
                )
                gateway_bound = True
            self._active_runtimes[run_id] = holder.runtime
            await self._set_dispatch_state(
                attempt.attempt_id,
                DispatchState.DISPATCHING,
                lease=lease,
            )
            dispatch_accepted = False

            async def on_dsh_event(event: dict[str, Any]) -> None:
                nonlocal dispatch_accepted
                if not dispatch_accepted:
                    await self._set_dispatch_state(
                        attempt.attempt_id,
                        DispatchState.ACCEPTED,
                        lease=lease,
                    )
                    dispatch_accepted = True
                projected = _project_dsh_event(event)
                if projected is None:
                    return
                kind, data = projected
                await write_event(EventSource.DSH, kind, data)

            result = await holder.runtime.run(
                turn_input,
                session_id=conversation.dsh_session_id,
                on_event=on_dsh_event,
                on_notification=self._notification_handler(run, attempt),
            )
            if result.session_id != conversation.dsh_session_id:
                raise RuntimeError("DSH returned a different Session ID")
            if pending_action is not None:
                self._run_done.setdefault(run_id, asyncio.Event()).set()
                return False
            if not dispatch_accepted:
                await self._set_dispatch_state(
                    attempt.attempt_id,
                    DispatchState.ACCEPTED,
                    lease=lease,
                )
            if result.finish_reason != "completed":
                raise RuntimeError(f"DSH Run did not complete: {result.finish_reason or 'unknown'}")
            await self._settle_success(
                run,
                attempt,
                result.final_response,
                result.finish_reason,
                lease=lease,
            )
            return True
        except asyncio.CancelledError:
            raise
        except LeaseLostError:
            return False
        except BaseException as exc:
            if pending_action is not None:
                _LOGGER.warning(
                    "DSH Turn ended with %s after Run %s entered durable user-input waiting",
                    type(exc).__name__,
                    run_id,
                )
                return False
            if run is not None and attempt is not None:
                try:
                    await self._settle_failure(run, attempt, exc, lease=lease)
                except LeaseLostError:
                    return False
            await self._block_conversation(
                conversation.conversation_id,
                f"DSH conversation runtime failed: {type(exc).__name__}",
            )
            await self._discard_conversation_runtime(conversation.conversation_id)
            return False
        finally:
            self._active_runtimes.pop(run_id, None)
            if gateway_bound and holder is not None and holder.tool_gateway is not None:
                if attempt is not None:
                    holder.tool_gateway.release(attempt.attempt_id)
            if gateway_bound and holder is not None and holder.memory_gateway is not None:
                if attempt is not None:
                    holder.memory_gateway.release(attempt.attempt_id)

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
        memory_gateway: MemoryContextGateway | None = None
        try:
            if agent.tools or agent.allow_user_input:
                gateway = ToolGateway(agent=agent, authorizer=self.authorizer)
                await gateway.start()
            if agent.memory_providers:
                memory_gateway = MemoryContextGateway(
                    agent=agent,
                    authorizer=self.authorizer,
                )
                await memory_gateway.start()
            runtime = self.runtime_factory.create(
                agent,
                workspace=self.workspace,
                dsh_home=self._conversation_dsh_home(conversation.dsh_home_key),
                attempt_id=attempt_id,
                tool_gateway_url=gateway.url if gateway is not None else None,
                memory_context_url=(
                    memory_gateway.url if memory_gateway is not None else None
                ),
                memory_context_token=(
                    memory_gateway.token if memory_gateway is not None else None
                ),
            )
        except BaseException:
            if gateway is not None:
                await gateway.close()
            if memory_gateway is not None:
                await memory_gateway.close()
            raise
        holder = _ConversationRuntimeHolder(
            runtime=runtime,
            tool_gateway=gateway,
            memory_gateway=memory_gateway,
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
        if holder.tool_gateway is not None:
            await holder.tool_gateway.close()
        if holder.memory_gateway is not None:
            await holder.memory_gateway.close()

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

    async def _settle_planning(
        self,
        started_run: RunRecord,
        started_attempt: RunAttempt,
        plan: PlanRecord,
        output: str,
        finish_reason: str | None,
        *,
        lease: WorkLease | None = None,
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
                    "dispatch_state": DispatchState.SETTLED,
                    "revision": attempt.revision + 1,
                    "finish_reason": finish_reason,
                    "output": output,
                    "finished_at": now,
                    "updated_at": now,
                }
            )
            action = RequiredActionRecord(
                run_id=current.run_id,
                attempt_id=settled_attempt.attempt_id,
                conversation_id=current.conversation_id,
                tenant_id=current.tenant_id,
                principal_id=current.principal_id,
                type=RequiredActionType.PLAN_APPROVAL,
                source=RequiredActionSource.POLICY,
                payload={
                    "plan_id": plan.plan_id,
                    "plan_revision": plan.revision,
                    "plan_sha256": plan.plan_sha256,
                    "summary": plan.summary,
                    "steps": [step.model_dump(mode="json") for step in plan.steps],
                    "requested_tools": list(plan.requested_tools),
                    "workflow_id": plan.workflow_id,
                    "workflow_version": plan.workflow_version,
                },
            )
            waiting_run = current.model_copy(
                update={
                    "status": RunStatus.WAITING,
                    "phase": RunPhase.WAITING_APPROVAL,
                    "plan_id": plan.plan_id,
                    "required_action_id": action.action_id,
                    "revision": current.revision + 1,
                    "output": None,
                    "error": None,
                    "updated_at": now,
                }
            )
            await self.store.complete_planning(
                waiting_run,
                settled_attempt,
                plan,
                action,
                expected_run_revision=current.revision,
                expected_attempt_revision=attempt.revision,
                lease=lease,
            )
        await self.store.append_event(
            run_id=waiting_run.run_id,
            attempt_id=settled_attempt.attempt_id,
            source=EventSource.CONTROL,
            kind="plan.proposed",
            data={
                "plan_id": plan.plan_id,
                "plan_revision": plan.revision,
                "plan_sha256": plan.plan_sha256,
                "action_id": action.action_id,
                "step_count": len(plan.steps),
                "requested_tools": list(plan.requested_tools),
            },
        )
        await self.store.append_event(
            run_id=waiting_run.run_id,
            attempt_id=settled_attempt.attempt_id,
            source=EventSource.CONTROL,
            kind="run.requires_action",
            data={
                "action_id": action.action_id,
                "type": action.type.value,
                "source": action.source.value,
            },
        )
        await self._audit_run(
            Principal(waiting_run.tenant_id, waiting_run.principal_id),
            waiting_run,
            "plan.propose",
            "waiting_approval",
            attempt_id=settled_attempt.attempt_id,
            data={
                "plan_id": plan.plan_id,
                "plan_revision": plan.revision,
                "plan_sha256": plan.plan_sha256,
                "action_id": action.action_id,
            },
        )

    async def _settle_success(
        self,
        started_run: RunRecord,
        started_attempt: RunAttempt,
        output: str,
        finish_reason: str | None,
        *,
        lease: WorkLease | None = None,
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
                    "dispatch_state": DispatchState.SETTLED,
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
                    "phase": RunPhase.COMPLETED,
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
                lease=lease,
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
        *,
        lease: WorkLease | None = None,
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
                    "dispatch_state": DispatchState.SETTLED,
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
                lease=lease,
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

    async def _set_dispatch_state(
        self,
        attempt_id: str,
        state: DispatchState,
        *,
        lease: WorkLease | None,
    ) -> RunAttempt:
        current = await self.store.get_attempt(attempt_id)
        if (
            current.status
            in {
                AttemptStatus.SUCCEEDED,
                AttemptStatus.FAILED,
                AttemptStatus.CANCELLED,
                AttemptStatus.INTERRUPTED,
                AttemptStatus.WAITING,
            }
            or current.dispatch_state is state
        ):
            return current
        updated = current.model_copy(
            update={
                "dispatch_state": state,
                "revision": current.revision + 1,
                "updated_at": utc_now(),
            }
        )
        await self.store.replace_attempt(
            updated,
            expected_revision=current.revision,
            lease=lease,
        )
        return updated

    async def _owned_run(self, principal: Principal, run_id: str) -> RunRecord:
        run = await self.store.get_run(run_id)
        if run.tenant_id != principal.tenant_id or run.principal_id != principal.principal_id:
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
            raise ConversationAccessDenied("Conversation belongs to another tenant or principal")
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

    async def _store_tool_artifact(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        name: str,
        media_type: str,
        content: bytes,
    ) -> ArtifactRecord:
        artifact_id = new_id("artifact")
        stored = await self.artifact_store.put(
            artifact_id=artifact_id,
            content=content,
        )
        record = ArtifactRecord(
            artifact_id=artifact_id,
            run_id=run.run_id,
            attempt_id=attempt.attempt_id,
            name=name,
            media_type=media_type,
            location=stored.location,
            sha256=stored.sha256,
            size_bytes=stored.size_bytes,
        )
        try:
            await self.store.add_artifact(record)
        except BaseException:
            try:
                await self.artifact_store.delete(stored.location)
            except BaseException:
                _LOGGER.exception(
                    "failed to remove orphaned artifact body %s",
                    artifact_id,
                )
            raise
        return record

    def _notification_handler(
        self,
        run: RunRecord,
        attempt: RunAttempt,
    ) -> DshNotificationHandler:
        async def publish(method: str, payload: dict[str, Any]) -> None:
            await self.notification_publisher.publish(
                DshNotificationEnvelope(
                    tenant_id=run.tenant_id,
                    principal_id=run.principal_id,
                    agent_id=run.agent_id,
                    conversation_id=run.conversation_id,
                    run_id=run.run_id,
                    attempt_id=attempt.attempt_id,
                    dsh_session_id=attempt.dsh_session_id,
                    method=method,
                    payload=payload,
                )
            )

        return publish


def _render_required_action_response(action: RequiredActionRecord) -> str:
    if action.response is None:
        raise ValueError("RequiredAction response is missing")
    return (
        "[base-agent required action response]\n\n"
        f"action_id: {action.action_id}\n"
        f"type: {action.type.value}\n\n"
        "The user answered the questions from the previous turn:\n\n"
        f"{json.dumps(action.response, ensure_ascii=False, separators=(',', ':'))}\n\n"
        "Continue the unfinished task using this answer. Do not ask the same question again."
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
    "PlanDecisionConflict",
    "PlanDecisionView",
    "RunAccessDenied",
    "RunResumeUnsupported",
    "RunTransitionError",
    "RunView",
]
