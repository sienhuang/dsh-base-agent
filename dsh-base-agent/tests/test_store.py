from __future__ import annotations

import sqlite3

import pytest

from dsh_base_agent.control.models import (
    ConversationRecord,
    EventSource,
    RunAttempt,
    RunRecord,
    RunStatus,
    utc_now,
)
from dsh_base_agent.store import IdempotencyConflictError, SqliteControlStore


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
