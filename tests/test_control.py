from __future__ import annotations

import asyncio
from collections.abc import Iterable
from pathlib import Path

import pytest

from dsh_base_agent import (
    Agent,
    AttemptStatus,
    ControlPlane,
    DshNotificationEnvelope,
    Principal,
    RunStatus,
    RuntimeConfig,
)
from dsh_base_agent.adapters.dsh.runtime import (
    DshEventHandler,
    DshNotificationHandler,
    DshRunResult,
    DshRuntime,
)
from dsh_base_agent.control import RunAccessDenied
from dsh_base_agent.store import SqliteControlStore


class FakeRuntime:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.closed = False
        self.calls: list[tuple[str, str]] = []

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        self.calls.append((input, session_id))
        if on_notification is not None:
            await on_notification("status.changed", {"status": "running"})
        if on_event is not None:
            await on_event({"seq": 1, "type": "turn/start", "data": {"turn": 1}})
            await on_event(
                {
                    "seq": 2,
                    "type": "assistant/message",
                    "data": {"message": {"content": [{"type": "text", "text": "secret"}]}},
                }
            )
            await on_event(
                {
                    "seq": 3,
                    "type": "turn/end",
                    "data": {"turn": 1, "reason": {"kind": "completed"}},
                }
            )
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return DshRunResult(
            session_id=session_id,
            final_response=str(outcome),
            finish_reason="completed",
        )

    async def close(self) -> None:
        self.closed = True


class FakeRuntimeFactory:
    def __init__(self, outcomes: Iterable[object]) -> None:
        self.outcomes = list(outcomes)
        self.sessions: list[str] = []
        self.runtimes: list[FakeRuntime] = []

    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
        memory_context_url: str | None = None,
        memory_context_token: str | None = None,
    ) -> DshRuntime:
        del (
            agent,
            workspace,
            dsh_home,
            attempt_id,
            tool_gateway_url,
            memory_context_url,
            memory_context_token,
        )
        runtime = FakeRuntime(self.outcomes)
        self.runtimes.append(runtime)
        return runtime


class RecordingPublisher:
    def __init__(self) -> None:
        self.started = False
        self.closed = False
        self.notifications: list[DshNotificationEnvelope] = []

    async def start(self) -> None:
        self.started = True

    async def publish(self, notification: DshNotificationEnvelope) -> bool:
        self.notifications.append(notification)
        return True

    async def close(self) -> None:
        self.closed = True


def runtime_config(tmp_path) -> RuntimeConfig:  # type: ignore[no-untyped-def]
    return RuntimeConfig(
        provider="test",
        model="test",
        dsh_home=tmp_path / "dsh-home",
    )


async def test_control_plane_runs_through_runtime_and_keeps_bounded_dsh_projection(
    tmp_path,
) -> None:
    factory = FakeRuntimeFactory(["done"])
    store = SqliteControlStore(tmp_path / "control.db")
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=store,
        runtime_factory=factory,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    principal = Principal("tenant-a", "alice")
    submitted = await control.submit(
        principal=principal,
        agent_id="orders",
        input="query order",
        idempotency_key="request-1",
    )
    completed = await control.wait(submitted.run_id)
    view = await control.view(principal, submitted.run_id)
    events = await control.events(principal, submitted.run_id)

    assert completed.status is RunStatus.SUCCEEDED
    assert completed.output == "done"
    assert len(view.attempts) == 1
    assert view.attempts[0].dsh_session_id.startswith(f"session_{submitted.run_id}_")
    assert "dsh.assistant.message" not in {event.kind for event in events}
    assert "dsh.turn.start" in {event.kind for event in events}
    assert all(runtime.closed for runtime in factory.runtimes)

    repeated = await control.submit(
        principal=principal,
        agent_id="orders",
        input="query order",
        idempotency_key="request-1",
    )
    assert repeated.run_id == submitted.run_id
    assert len((await control.view(principal, submitted.run_id)).attempts) == 1
    await control.close()


async def test_control_plane_enriches_raw_notifications_with_run_identity(tmp_path) -> None:
    publisher = RecordingPublisher()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=FakeRuntimeFactory(["done"]),
        notification_publisher=publisher,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))

    submitted = await control.submit(
        principal=Principal("tenant-a", "alice"),
        agent_id="orders",
        input="go",
    )
    await control.wait(submitted.run_id)
    await control.close()

    assert publisher.started is True
    assert publisher.closed is True
    assert len(publisher.notifications) == 1
    notification = publisher.notifications[0]
    assert notification.tenant_id == "tenant-a"
    assert notification.principal_id == "alice"
    assert notification.run_id == submitted.run_id
    assert notification.attempt_id.startswith("attempt_")
    assert notification.dsh_session_id.startswith(f"session_{submitted.run_id}_")
    assert notification.method == "status.changed"


async def test_external_execution_mode_only_persists_queued_run(tmp_path) -> None:
    factory = FakeRuntimeFactory(["should-not-run"])
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
        auto_execute=False,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))

    submitted = await control.submit(
        principal=Principal("tenant-a", "alice"),
        agent_id="orders",
        input="go",
    )
    await asyncio.sleep(0)

    assert submitted.status is RunStatus.QUEUED
    assert (
        await control.view(Principal("tenant-a", "alice"), submitted.run_id)
    ).run.status is RunStatus.QUEUED
    assert factory.runtimes == []
    await control.close()


async def test_retry_creates_new_attempt_and_session(tmp_path) -> None:
    factory = FakeRuntimeFactory([RuntimeError("temporary"), "recovered"])
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    principal = Principal("tenant-a", "alice")
    submitted = await control.submit(principal=principal, agent_id="orders", input="go")
    failed = await control.wait(submitted.run_id)
    assert failed.status is RunStatus.FAILED

    await control.retry(principal, submitted.run_id)
    recovered = await control.wait(submitted.run_id)
    attempts = (await control.view(principal, submitted.run_id)).attempts
    assert recovered.status is RunStatus.SUCCEEDED
    assert len(attempts) == 2
    assert attempts[0].dsh_session_id != attempts[1].dsh_session_id
    await control.close()


async def test_tenant_cannot_read_another_tenants_run(tmp_path) -> None:
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=FakeRuntimeFactory(["done"]),
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    run = await control.submit(
        principal=Principal("tenant-a", "alice"),
        agent_id="orders",
        input="go",
    )
    await control.wait(run.run_id)
    with pytest.raises(RunAccessDenied):
        await control.view(Principal("tenant-b", "bob"), run.run_id)
    await control.close()


async def test_conversation_runs_reuse_one_runtime_and_session_in_sequence(tmp_path) -> None:
    factory = FakeRuntimeFactory(["Which order?", "Order order-001 is paid."])
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    principal = Principal("tenant-a", "alice")
    conversation = await control.create_conversation(
        principal=principal,
        agent_id="orders",
    )

    first = await control.submit_to_conversation(
        principal=principal,
        conversation_id=conversation.conversation_id,
        input="Query order status",
        idempotency_key="conversation-message-1",
    )
    second = await control.submit_to_conversation(
        principal=principal,
        conversation_id=conversation.conversation_id,
        input="order-001",
        idempotency_key="conversation-message-2",
    )
    completed = await control.wait(second.run_id)

    assert completed.output == "Order order-001 is paid."
    assert (first.sequence, second.sequence) == (1, 2)
    assert len(factory.runtimes) == 1
    assert factory.runtimes[0].calls == [
        ("Query order status", conversation.dsh_session_id),
        ("order-001", conversation.dsh_session_id),
    ]
    first_attempt = (await control.view(principal, first.run_id)).attempts[0]
    second_attempt = (await control.view(principal, second.run_id)).attempts[0]
    assert first_attempt.dsh_session_id == second_attempt.dsh_session_id
    assert factory.runtimes[0].closed is False

    await control.close()
    assert factory.runtimes[0].closed is True


async def test_conversation_is_bound_to_both_tenant_and_principal(tmp_path) -> None:
    control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=FakeRuntimeFactory([]),
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    conversation = await control.create_conversation(
        principal=Principal("tenant-a", "alice"),
        agent_id="orders",
    )

    with pytest.raises(PermissionError):
        await control.get_conversation(
            Principal("tenant-a", "bob"),
            conversation.conversation_id,
        )
    await control.close()


async def test_restart_marks_running_attempt_interrupted_without_replaying(tmp_path) -> None:
    store = SqliteControlStore(tmp_path / "control.db")
    await store.initialize()
    agent = Agent(name="orders", prompt="Handle orders.")
    from dsh_base_agent.control.models import RunAttempt, RunRecord

    queued = RunRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id=agent.name,
        agent_version=agent.version,
        agent_fingerprint=agent.fingerprint,
        input="go",
    )
    await store.create_run(queued, idempotency_key=None, request_digest="digest")
    attempt = RunAttempt(run_id=queued.run_id, number=1, dsh_session_id="session-existing")
    running = queued.model_copy(
        update={
            "status": RunStatus.RUNNING,
            "revision": 2,
            "attempt_count": 1,
            "active_attempt_id": attempt.attempt_id,
        }
    )
    await store.begin_attempt(running, attempt, expected_run_revision=1)
    await store.close()

    factory = FakeRuntimeFactory(["must-not-run"])
    restarted = ControlPlane(
        workspace=tmp_path,
        runtime=runtime_config(tmp_path),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    restarted.register(agent)
    await restarted.start()
    recovered = await restarted.view(Principal("tenant-a", "alice"), queued.run_id)
    assert recovered.run.status is RunStatus.FAILED
    assert recovered.attempts[0].status is AttemptStatus.INTERRUPTED
    assert recovered.attempts[0].dsh_session_id == "session-existing"
    assert factory.runtimes == []
    assert (await restarted.events(Principal("tenant-a", "alice"), queued.run_id))[-1].kind == (
        "run.recovery_required"
    )
    await restarted.close()
