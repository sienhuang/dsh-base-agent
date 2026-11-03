from __future__ import annotations

import sqlite3

import pytest

from dsh_base_agent.control.models import (
    AttemptStatus,
    ConversationRecord,
    EventSource,
    RequiredActionRecord,
    RequiredActionSource,
    RequiredActionStatus,
    RequiredActionType,
    RunAttempt,
    RunPhase,
    RunRecord,
    RunStatus,
    utc_now,
)
from dsh_base_agent.store import IdempotencyConflictError, SqliteControlStore


async def test_store_backfills_succeeded_run_phase(tmp_path) -> None:
    path = tmp_path / "control.db"
    store = SqliteControlStore(path)
    await store.initialize()
    run = RunRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        agent_version="1.0.0",
        agent_fingerprint="a" * 64,
        input="hello",
        phase=RunPhase.EXECUTING,
    )
    await store.create_run(run, idempotency_key=None, request_digest=run.run_id)
    await store.replace_run(
        run.model_copy(
            update={
                "status": RunStatus.SUCCEEDED,
                "revision": 2,
            }
        ),
        expected_revision=1,
    )
    await store.close()

    reopened = SqliteControlStore(path)
    await reopened.initialize()
    try:
        migrated = await reopened.get_run(run.run_id)
        assert migrated.status is RunStatus.SUCCEEDED
        assert migrated.phase is RunPhase.COMPLETED
        assert migrated.revision == 2
    finally:
        await reopened.close()


async def test_store_persists_idempotent_run_attempt_and_events(tmp_path) -> None:
    store = SqliteControlStore(tmp_path / "control.db")
    await store.initialize()
    run = RunRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        agent_version="1.0.0",
        agent_fingerprint="a" * 64,
        input="hello",
    )
    created, is_new = await store.create_run(
        run,
        idempotency_key="request-1",
        request_digest="digest-1",
    )
    repeated, repeated_is_new = await store.create_run(
        run.model_copy(update={"run_id": "another"}),
        idempotency_key="request-1",
        request_digest="digest-1",
    )
    assert is_new is True
    assert repeated_is_new is False
    assert repeated.run_id == created.run_id

    with pytest.raises(IdempotencyConflictError):
        await store.create_run(
            run.model_copy(update={"run_id": "conflict"}),
            idempotency_key="request-1",
            request_digest="different",
        )

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
    await store.begin_attempt(running, attempt, expected_run_revision=1)
    first = await store.append_event(
        run_id=run.run_id,
        attempt_id=attempt.attempt_id,
        source=EventSource.CONTROL,
        kind="run.started",
    )
    second = await store.append_event(
        run_id=run.run_id,
        attempt_id=attempt.attempt_id,
        source=EventSource.DSH,
        kind="dsh.turn.start",
    )
    assert (first.sequence, second.sequence) == (1, 2)
    assert (await store.get_run(run.run_id)).status is RunStatus.RUNNING
    assert len(await store.list_attempts(run.run_id)) == 1
    await store.close()


async def test_store_persists_required_action_wait_answer_and_consume(tmp_path) -> None:
    store = SqliteControlStore(tmp_path / "control.db")
    await store.initialize()
    conversation = ConversationRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        agent_version="1.0.0",
        agent_fingerprint="a" * 64,
        dsh_home_key="b" * 64,
        dsh_session_id="session-shared",
    )
    await store.create_conversation(conversation)
    queued, _ = await store.create_conversation_run(
        conversation.conversation_id,
        RunRecord(
            conversation_id=conversation.conversation_id,
            tenant_id="tenant-a",
            principal_id="alice",
            agent_id="orders",
            agent_version="1.0.0",
            agent_fingerprint="a" * 64,
            input="deploy",
        ),
        idempotency_key=None,
        request_digest="request",
    )
    attempt = RunAttempt(
        run_id=queued.run_id,
        number=1,
        dsh_session_id=conversation.dsh_session_id,
        status=AttemptStatus.RUNNING,
    )
    running = queued.model_copy(
        update={
            "status": RunStatus.RUNNING,
            "revision": queued.revision + 1,
            "attempt_count": 1,
            "active_attempt_id": attempt.attempt_id,
        }
    )
    await store.begin_attempt(running, attempt, expected_run_revision=queued.revision)

    action = RequiredActionRecord(
        run_id=queued.run_id,
        attempt_id=attempt.attempt_id,
        conversation_id=conversation.conversation_id,
        tenant_id="tenant-a",
        principal_id="alice",
        type=RequiredActionType.USER_INPUT,
        source=RequiredActionSource.MODEL,
        payload={
            "questions": [
                {
                    "id": "environment",
                    "question": "Choose an environment.",
                    "options": [{"label": "dev"}, {"label": "prod"}],
                    "multi_select": False,
                }
            ]
        },
    )
    waiting_run, waiting_attempt = await store.enter_required_action(action)
    assert waiting_run.status is RunStatus.WAITING
    assert waiting_run.required_action_id == action.action_id
    assert waiting_attempt.status is AttemptStatus.WAITING

    answered = action.model_copy(
        update={
            "status": RequiredActionStatus.ANSWERED,
            "response": {
                "answers": [{"id": "environment", "selected": ["dev"]}]
            },
            "revision": 2,
        }
    )
    resumed = waiting_run.model_copy(
        update={
            "status": RunStatus.QUEUED,
            "revision": waiting_run.revision + 1,
            "active_attempt_id": None,
        }
    )
    await store.answer_required_action(
        resumed,
        answered,
        expected_run_revision=waiting_run.revision,
        expected_action_revision=action.revision,
    )

    second_attempt = RunAttempt(
        run_id=queued.run_id,
        number=2,
        dsh_session_id=conversation.dsh_session_id,
        status=AttemptStatus.RUNNING,
    )
    consumed = answered.model_copy(
        update={
            "status": RequiredActionStatus.CONSUMED,
            "revision": answered.revision + 1,
            "consumed_by_attempt_id": second_attempt.attempt_id,
        }
    )
    running_again = resumed.model_copy(
        update={
            "status": RunStatus.RUNNING,
            "revision": resumed.revision + 1,
            "attempt_count": 2,
            "active_attempt_id": second_attempt.attempt_id,
            "required_action_id": None,
        }
    )
    await store.begin_attempt(
        running_again,
        second_attempt,
        expected_run_revision=resumed.revision,
        consumed_action=consumed,
        expected_action_revision=answered.revision,
    )

    assert (await store.get_required_action(action.action_id)).status is (
        RequiredActionStatus.CONSUMED
    )
    assert (await store.get_run(queued.run_id)).required_action_id is None
    assert len(await store.list_attempts(queued.run_id)) == 2

    second_action = action.model_copy(
        update={
            "action_id": "action-second",
            "attempt_id": second_attempt.attempt_id,
            "status": RequiredActionStatus.PENDING,
            "response": None,
            "revision": 1,
            "answered_by": None,
            "answered_at": None,
            "consumed_by_attempt_id": None,
        }
    )
    waiting_again, waiting_second_attempt = await store.enter_required_action(second_action)
    cancelled_action = second_action.model_copy(
        update={"status": RequiredActionStatus.CANCELLED, "revision": 2}
    )
    cancelled_attempt = waiting_second_attempt.model_copy(
        update={"status": AttemptStatus.CANCELLED, "revision": waiting_second_attempt.revision + 1}
    )
    cancelled_run = waiting_again.model_copy(
        update={
            "status": RunStatus.CANCELLED,
            "revision": waiting_again.revision + 1,
            "active_attempt_id": None,
            "required_action_id": None,
        }
    )
    await store.settle_attempt(
        cancelled_run,
        cancelled_attempt,
        expected_run_revision=waiting_again.revision,
        expected_attempt_revision=waiting_second_attempt.revision,
        settled_action=cancelled_action,
        expected_action_revision=second_action.revision,
    )
    assert (await store.get_required_action(second_action.action_id)).status is (
        RequiredActionStatus.CANCELLED
    )
    assert (await store.get_run(queued.run_id)).status is RunStatus.CANCELLED
    await store.close()


async def test_store_assigns_conversation_sequences_and_allows_shared_session(tmp_path) -> None:
    store = SqliteControlStore(tmp_path / "control.db")
    await store.initialize()
    conversation = ConversationRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        agent_version="1.0.0",
        agent_fingerprint="a" * 64,
        dsh_home_key="b" * 64,
        dsh_session_id="session-shared",
    )
    await store.create_conversation(conversation)

    runs: list[RunRecord] = []
    for index in range(2):
        run, created = await store.create_conversation_run(
            conversation.conversation_id,
            RunRecord(
                conversation_id=conversation.conversation_id,
                tenant_id="tenant-a",
                principal_id="alice",
                agent_id="orders",
                agent_version="1.0.0",
                agent_fingerprint="a" * 64,
                input=f"message-{index + 1}",
            ),
            idempotency_key=f"message-{index + 1}",
            request_digest=f"digest-{index + 1}",
        )
        assert created is True
        runs.append(run)

    assert [run.sequence for run in runs] == [1, 2]
    assert [
        run.run_id for run in await store.list_conversation_runs(conversation.conversation_id)
    ] == [run.run_id for run in runs]

    for run in runs:
        attempt = RunAttempt(run_id=run.run_id, number=1, dsh_session_id="session-shared")
        await store.begin_attempt(
            run.model_copy(
                update={
                    "status": RunStatus.RUNNING,
                    "revision": 2,
                    "attempt_count": 1,
                    "active_attempt_id": attempt.attempt_id,
                }
            ),
            attempt,
            expected_run_revision=1,
        )

    await store.close()


async def test_store_migrates_v01_unique_session_constraint(tmp_path) -> None:
    path = tmp_path / "control.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE TABLE run_attempts (
            attempt_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            number INTEGER NOT NULL,
            session_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            revision INTEGER NOT NULL,
            payload TEXT NOT NULL,
            UNIQUE(run_id, number)
        );
        """
    )
    connection.close()

    store = SqliteControlStore(path)
    await store.initialize()
    runs = [
        RunRecord(
            tenant_id="tenant-a",
            principal_id="alice",
            agent_id="orders",
            agent_version="1.0.0",
            agent_fingerprint="a" * 64,
            input=f"message-{index}",
        )
        for index in range(2)
    ]
    for run in runs:
        await store.create_run(run, idempotency_key=None, request_digest=run.run_id)
        attempt = RunAttempt(run_id=run.run_id, number=1, dsh_session_id="session-shared")
        await store.begin_attempt(
            run.model_copy(
                update={
                    "status": RunStatus.RUNNING,
                    "revision": 2,
                    "attempt_count": 1,
                    "active_attempt_id": attempt.attempt_id,
                }
            ),
            attempt,
            expected_run_revision=1,
        )
    await store.close()
