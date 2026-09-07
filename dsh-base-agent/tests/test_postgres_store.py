from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from dsh_base_agent import (
    Agent,
    ControlPlane,
    PostgresControlStore,
    Principal,
    RuntimeConfig,
    WorkerConfig,
    WorkerService,
)
from dsh_base_agent.adapters.dsh.runtime import DshRunResult, DshRuntime
from dsh_base_agent.control.models import (
    ArtifactRecord,
    AttemptStatus,
    AuditRecord,
    ConversationRecord,
    EventSource,
    RunAttempt,
    RunRecord,
    RunStatus,
    WorkResourceType,
    utc_now,
)
from dsh_base_agent.store import (
    IdempotencyConflictError,
    LeaseLostError,
    RevisionConflictError,
)

_DATABASE_URL = os.environ.get("DSH_BASE_AGENT_TEST_POSTGRES_URL")
pytestmark = pytest.mark.skipif(
    _DATABASE_URL is None,
    reason="set DSH_BASE_AGENT_TEST_POSTGRES_URL to run PostgreSQL integration tests",
)


@pytest.fixture
async def postgres_store() -> AsyncIterator[PostgresControlStore]:
    assert _DATABASE_URL is not None
    schema = f"dsh_test_{uuid4().hex}"
    store = PostgresControlStore(
        _DATABASE_URL,
        schema=schema,
        min_pool_size=1,
        max_pool_size=6,
    )
    await store.initialize()
    try:
        yield store
    finally:
        await store.close()
        connection = await asyncpg.connect(_DATABASE_URL)
        try:
            await connection.execute(f'DROP SCHEMA "{schema}" CASCADE')
        finally:
            await connection.close()


def _run(*, run_id: str | None = None, input: str = "hello") -> RunRecord:
    values: dict[str, object] = {
        "tenant_id": "tenant-a",
        "principal_id": "alice",
        "agent_id": "orders",
        "agent_version": "1.0.0",
        "agent_fingerprint": "a" * 64,
        "input": input,
    }
    if run_id is not None:
        values["run_id"] = run_id
    return RunRecord.model_validate(values)


class ImmediateRuntime:
    def __init__(self, factory: ImmediateRuntimeFactory) -> None:
        self.factory = factory

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event=None,  # type: ignore[no-untyped-def]
        on_notification=None,  # type: ignore[no-untyped-def]
    ) -> DshRunResult:
        del on_event, on_notification
        self.factory.calls.append((input, session_id))
        return DshRunResult(
            session_id=session_id,
            final_response="worker-done",
            finish_reason="completed",
        )

    async def close(self) -> None:
        return None


class ImmediateRuntimeFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.dsh_homes: list[Path] = []

    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
    ) -> DshRuntime:
        del agent, workspace, attempt_id, tool_gateway_url
        self.dsh_homes.append(dsh_home)
        return ImmediateRuntime(self)


class BlockingRuntime:
    def __init__(self) -> None:
        self.closed = False
        self.cancelled = False

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event=None,  # type: ignore[no-untyped-def]
        on_notification=None,  # type: ignore[no-untyped-def]
    ) -> DshRunResult:
        del input, session_id, on_event, on_notification
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        raise AssertionError("unreachable")

    async def close(self) -> None:
        self.closed = True


class BlockingRuntimeFactory:
    def __init__(self) -> None:
        self.runtime = BlockingRuntime()

    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
    ) -> DshRuntime:
        del agent, workspace, dsh_home, attempt_id, tool_gateway_url
        return self.runtime


async def test_postgres_store_serializes_conversation_sequences_and_events(
    postgres_store: PostgresControlStore,
) -> None:
    conversation = ConversationRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        agent_version="1.0.0",
        agent_fingerprint="a" * 64,
        dsh_home_key="b" * 64,
        dsh_session_id="session-shared",
    )
    await postgres_store.create_conversation(conversation)

    created = await asyncio.gather(
        *(
            postgres_store.create_conversation_run(
                conversation.conversation_id,
                _run(input=f"message-{index}"),
                idempotency_key=f"message-{index}",
                request_digest=f"digest-{index}",
            )
            for index in range(1, 7)
        )
    )
    runs = [item[0] for item in created]
    persisted = await postgres_store.list_conversation_runs(conversation.conversation_id)

    assert [run.sequence for run in persisted] == list(range(1, 7))
    assert {run.run_id for run in persisted} == {run.run_id for run in runs}

    events = await asyncio.gather(
        *(
            postgres_store.append_event(
                run_id=runs[0].run_id,
                attempt_id=None,
                source=EventSource.CONTROL,
                kind=f"test.{index}",
            )
            for index in range(1, 7)
        )
    )
    assert sorted(event.sequence for event in events) == list(range(1, 7))


async def test_postgres_store_serializes_idempotency_and_enforces_cas(
    postgres_store: PostgresControlStore,
) -> None:
    first, second = await asyncio.gather(
        postgres_store.create_run(
            _run(run_id="run-first"),
            idempotency_key="same-request",
            request_digest="same-digest",
        ),
        postgres_store.create_run(
            _run(run_id="run-second"),
            idempotency_key="same-request",
            request_digest="same-digest",
        ),
    )

    assert first[0].run_id == second[0].run_id
    assert sorted((first[1], second[1])) == [False, True]

    stored = await postgres_store.get_run(first[0].run_id)
    replacement = stored.model_copy(update={"revision": stored.revision + 1})
    await postgres_store.replace_run(replacement, expected_revision=stored.revision)
    with pytest.raises(RevisionConflictError):
        await postgres_store.replace_run(replacement, expected_revision=stored.revision)

    with pytest.raises(IdempotencyConflictError):
        await postgres_store.create_run(
            _run(run_id="run-conflict"),
            idempotency_key="same-request",
            request_digest="different-digest",
        )


async def test_postgres_schema_has_migration_and_first_class_relationship_columns(
    postgres_store: PostgresControlStore,
) -> None:
    assert _DATABASE_URL is not None
    reopened = PostgresControlStore(
        _DATABASE_URL,
        schema=postgres_store.schema,
        min_pool_size=1,
        max_pool_size=1,
    )
    await reopened.initialize()
    await reopened.close()
    connection = await asyncpg.connect(_DATABASE_URL)
    try:
        migrations = await connection.fetch(
            f'SELECT version, name FROM "{postgres_store.schema}".schema_migrations '
            "ORDER BY version"
        )
        columns = await connection.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = 'runs'
            """,
            postgres_store.schema,
        )
    finally:
        await connection.close()

    assert [(row["version"], row["name"]) for row in migrations] == [(1, "initial_control_store")]
    assert {row["column_name"] for row in columns} >= {
        "conversation_id",
        "sequence",
        "principal_id",
        "agent_fingerprint",
        "active_attempt_id",
    }


async def test_postgres_store_persists_attempt_audit_and_artifact_lifecycle(
    postgres_store: PostgresControlStore,
) -> None:
    run = _run()
    await postgres_store.create_run(run, idempotency_key=None, request_digest=run.run_id)
    attempt = RunAttempt(run_id=run.run_id, number=1, dsh_session_id="session-1")
    running = run.model_copy(
        update={
            "status": RunStatus.RUNNING,
            "revision": 2,
            "attempt_count": 1,
            "active_attempt_id": attempt.attempt_id,
            "updated_at": utc_now(),
        }
    )
    await postgres_store.begin_attempt(running, attempt, expected_run_revision=1)
    await postgres_store.append_audit(
        AuditRecord(
            tenant_id=run.tenant_id,
            principal_id=run.principal_id,
            action="run.execute",
            outcome="started",
            run_id=run.run_id,
            attempt_id=attempt.attempt_id,
        )
    )
    await postgres_store.add_artifact(
        ArtifactRecord(
            run_id=run.run_id,
            attempt_id=attempt.attempt_id,
            name="result.json",
            media_type="application/json",
            location="s3://test/result.json",
            sha256="c" * 64,
            size_bytes=42,
        )
    )

    succeeded_attempt = attempt.model_copy(
        update={
            "status": AttemptStatus.SUCCEEDED,
            "revision": 2,
            "finish_reason": "completed",
            "output": "done",
            "finished_at": utc_now(),
            "updated_at": utc_now(),
        }
    )
    succeeded_run = running.model_copy(
        update={
            "status": RunStatus.SUCCEEDED,
            "revision": 3,
            "output": "done",
            "updated_at": utc_now(),
        }
    )
    await postgres_store.settle_attempt(
        succeeded_run,
        succeeded_attempt,
        expected_run_revision=2,
        expected_attempt_revision=1,
    )

    assert (await postgres_store.get_attempt(attempt.attempt_id)).status is AttemptStatus.SUCCEEDED
    assert (await postgres_store.get_run(run.run_id)).status is RunStatus.SUCCEEDED
    assert len(await postgres_store.list_audit(run.run_id)) == 1
    assert len(await postgres_store.list_artifacts(run.run_id)) == 1


async def test_postgres_worker_lease_is_exclusive_and_fences_stale_worker(
    postgres_store: PostgresControlStore,
) -> None:
    run = _run(run_id="run-leased")
    await postgres_store.create_run(run, idempotency_key=None, request_digest=run.run_id)

    first = await postgres_store.claim_work(worker_id="worker-a", lease_seconds=30)
    assert first is not None
    assert first.resource_type is WorkResourceType.RUN
    assert first.resource_id == run.run_id
    assert await postgres_store.claim_work(worker_id="worker-b", lease_seconds=30) is None

    assert await postgres_store.release_work_lease(first) is True
    reclaimed = await postgres_store.claim_work(worker_id="worker-a", lease_seconds=30)
    assert reclaimed is not None
    assert reclaimed.lease_token == first.lease_token + 1
    first = reclaimed

    assert _DATABASE_URL is not None
    connection = await asyncpg.connect(_DATABASE_URL)
    try:
        await connection.execute(
            f'UPDATE "{postgres_store.schema}".worker_leases '
            "SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
            "WHERE resource_type = 'run' AND resource_id = $1",
            run.run_id,
        )
    finally:
        await connection.close()

    second = await postgres_store.claim_work(worker_id="worker-b", lease_seconds=30)
    assert second is not None
    assert second.lease_token == first.lease_token + 1
    attempt = RunAttempt(
        run_id=run.run_id,
        number=1,
        dsh_session_id="session-leased",
        worker_id=first.worker_id,
        lease_token=first.lease_token,
    )
    running = run.model_copy(
        update={
            "status": RunStatus.RUNNING,
            "revision": 2,
            "attempt_count": 1,
            "active_attempt_id": attempt.attempt_id,
        }
    )
    with pytest.raises(LeaseLostError):
        await postgres_store.begin_attempt(
            running,
            attempt,
            expected_run_revision=1,
            lease=first,
        )
    current_attempt = attempt.model_copy(
        update={
            "worker_id": second.worker_id,
            "lease_token": second.lease_token,
            "status": AttemptStatus.RUNNING,
        }
    )
    await postgres_store.begin_attempt(
        running,
        current_attempt,
        expected_run_revision=1,
        lease=second,
    )
    connection = await asyncpg.connect(_DATABASE_URL)
    try:
        await connection.execute(
            f'UPDATE "{postgres_store.schema}".worker_leases '
            "SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
            "WHERE resource_type = 'run' AND resource_id = $1",
            run.run_id,
        )
    finally:
        await connection.close()

    recovered = await postgres_store.interrupt_expired_attempts()

    assert len(recovered) == 1
    assert recovered[0][0].status is RunStatus.FAILED
    assert recovered[0][1].status is AttemptStatus.INTERRUPTED
    assert recovered[0][1].dispatch_state.value == "unknown"
    events = await postgres_store.list_events(run.run_id)
    audits = await postgres_store.list_audit(run.run_id)
    assert [event.kind for event in events] == ["run.worker_lease_expired"]
    assert [record.action for record in audits] == ["run.worker_lease.expire"]


async def test_api_submission_is_executed_by_independent_worker(
    postgres_store: PostgresControlStore,
    tmp_path,
) -> None:
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=postgres_store,
        runtime_factory=ImmediateRuntimeFactory(),
        auto_execute=False,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    submitted = await control.submit(
        principal=Principal("tenant-a", "alice"),
        agent_id="orders",
        input="go",
    )
    assert submitted.status is RunStatus.QUEUED

    worker = WorkerService(
        control,
        WorkerConfig(
            worker_id="worker-a",
            poll_interval_seconds=0.01,
            lease_seconds=1,
            heartbeat_interval_seconds=0.1,
            heartbeat_timeout_seconds=0.2,
        ),
    )
    assert await worker.run_once() is True
    completed = await postgres_store.get_run(submitted.run_id)
    attempts = await postgres_store.list_attempts(submitted.run_id)

    assert completed.status is RunStatus.SUCCEEDED
    assert completed.output == "worker-done"
    assert attempts[0].worker_id == "worker-a"
    assert attempts[0].lease_token == 1
    await worker.close()


async def test_worker_run_timeout_is_recovered_atomically(
    postgres_store: PostgresControlStore,
    tmp_path: Path,
) -> None:
    factory = BlockingRuntimeFactory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=postgres_store,
        runtime_factory=factory,
        auto_execute=False,
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    submitted = await control.submit(
        principal=Principal("tenant-a", "alice"),
        agent_id="orders",
        input="hang",
    )
    worker = WorkerService(
        control,
        WorkerConfig(
            worker_id="worker-timeout",
            poll_interval_seconds=0.01,
            lease_seconds=1,
            heartbeat_interval_seconds=0.1,
            heartbeat_timeout_seconds=0.2,
            run_timeout_seconds=0.025,
        ),
    )

    assert await worker.run_once() is True
    assert factory.runtime.cancelled is True
    assert factory.runtime.closed is True
    assert await control.reconcile_expired_worker_leases() == 1

    run = await postgres_store.get_run(submitted.run_id)
    attempts = await postgres_store.list_attempts(submitted.run_id)
    events = await postgres_store.list_events(submitted.run_id)
    audits = await postgres_store.list_audit(submitted.run_id)
    assert run.status is RunStatus.FAILED
    assert attempts[0].status is AttemptStatus.INTERRUPTED
    assert attempts[0].dispatch_state.value == "unknown"
    assert events[-1].kind == "run.worker_lease_expired"
    assert audits[-1].action == "run.worker_lease.expire"
    await worker.close()


async def test_conversation_continues_on_replacement_worker_with_persisted_session(
    postgres_store: PostgresControlStore,
    tmp_path: Path,
) -> None:
    runtime = RuntimeConfig(
        provider="test",
        model="test",
        dsh_home=tmp_path / "shared-dsh-home",
    )
    agent = Agent(name="orders", prompt="Handle orders.")
    principal = Principal("tenant-a", "alice")
    first_factory = ImmediateRuntimeFactory()
    first_control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime,
        store=postgres_store,
        runtime_factory=first_factory,
        auto_execute=False,
    )
    first_control.register(agent)
    conversation = await first_control.create_conversation(
        principal=principal,
        agent_id=agent.name,
    )
    first_run = await first_control.submit_to_conversation(
        principal=principal,
        conversation_id=conversation.conversation_id,
        input="first turn",
    )
    first_worker = WorkerService(
        first_control,
        WorkerConfig(
            worker_id="worker-a",
            lease_seconds=1,
            heartbeat_interval_seconds=0.1,
            heartbeat_timeout_seconds=0.2,
        ),
    )
    assert await first_worker.run_once() is True
    assert (await postgres_store.get_run(first_run.run_id)).status is RunStatus.SUCCEEDED
    await first_worker.close()

    assert _DATABASE_URL is not None
    replacement_store = PostgresControlStore(
        _DATABASE_URL,
        schema=postgres_store.schema,
        min_pool_size=1,
        max_pool_size=2,
    )
    second_factory = ImmediateRuntimeFactory()
    second_control = ControlPlane(
        workspace=tmp_path,
        runtime=runtime,
        store=replacement_store,
        runtime_factory=second_factory,
        auto_execute=False,
    )
    second_control.register(agent)
    second_run = await second_control.submit_to_conversation(
        principal=principal,
        conversation_id=conversation.conversation_id,
        input="second turn",
    )
    second_worker = WorkerService(
        second_control,
        WorkerConfig(
            worker_id="worker-b",
            lease_seconds=1,
            heartbeat_interval_seconds=0.1,
            heartbeat_timeout_seconds=0.2,
        ),
    )
    assert await second_worker.run_once() is True

    completed = await replacement_store.get_run(second_run.run_id)
    persisted_conversation = await replacement_store.get_conversation(conversation.conversation_id)
    assert completed.status is RunStatus.SUCCEEDED
    assert persisted_conversation.status.value == "active"
    assert first_factory.calls == [("first turn", conversation.dsh_session_id)]
    assert second_factory.calls == [("second turn", conversation.dsh_session_id)]
    assert first_factory.dsh_homes == second_factory.dsh_homes
    await second_worker.close()
