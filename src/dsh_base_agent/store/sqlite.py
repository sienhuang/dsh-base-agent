"""Control-store protocol, shared errors, and the local SQLite implementation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from collections.abc import Collection
from pathlib import Path
from typing import Any, Protocol

from dsh_base_agent.control.models import (
    ArtifactRecord,
    AttemptStatus,
    AuditRecord,
    ConversationRecord,
    ConversationStatus,
    DispatchState,
    EventSource,
    PlanRecord,
    RequiredActionRecord,
    RequiredActionStatus,
    RequiredActionType,
    RunAttempt,
    RunEvent,
    RunRecord,
    RunStatus,
    WorkLease,
    utc_now,
)


class StoreError(RuntimeError):
    pass


class RunNotFoundError(StoreError):
    pass


class ConversationNotFoundError(StoreError):
    pass


class AttemptNotFoundError(StoreError):
    pass


class RevisionConflictError(StoreError):
    pass


class IdempotencyConflictError(StoreError):
    pass


class LeaseLostError(StoreError):
    pass


class PlanNotFoundError(StoreError):
    pass


class RequiredActionNotFoundError(StoreError):
    pass


class ControlStore(Protocol):
    async def initialize(self) -> None: ...

    async def close(self) -> None: ...

    async def create_conversation(self, conversation: ConversationRecord) -> None: ...

    async def get_conversation(self, conversation_id: str) -> ConversationRecord: ...

    async def list_conversations(
        self, *, statuses: Collection[str] = ()
    ) -> tuple[ConversationRecord, ...]: ...

    async def replace_conversation(
        self,
        conversation: ConversationRecord,
        *,
        expected_revision: int,
    ) -> None: ...

    async def create_conversation_run(
        self,
        conversation_id: str,
        run: RunRecord,
        *,
        idempotency_key: str | None,
        request_digest: str,
    ) -> tuple[RunRecord, bool]: ...

    async def list_conversation_runs(self, conversation_id: str) -> tuple[RunRecord, ...]: ...

    async def create_run(
        self,
        run: RunRecord,
        *,
        idempotency_key: str | None,
        request_digest: str,
    ) -> tuple[RunRecord, bool]: ...

    async def get_run(self, run_id: str) -> RunRecord: ...

    async def list_runs(self, *, statuses: Collection[str] = ()) -> tuple[RunRecord, ...]: ...

    async def replace_run(self, run: RunRecord, *, expected_revision: int) -> None: ...

    async def begin_attempt(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        expected_run_revision: int,
        lease: WorkLease | None = None,
        consumed_action: RequiredActionRecord | None = None,
        expected_action_revision: int | None = None,
    ) -> None: ...

    async def get_attempt(self, attempt_id: str) -> RunAttempt: ...

    async def replace_attempt(
        self,
        attempt: RunAttempt,
        *,
        expected_revision: int,
        lease: WorkLease | None = None,
    ) -> None: ...

    async def settle_attempt(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        expected_run_revision: int,
        expected_attempt_revision: int,
        lease: WorkLease | None = None,
        settled_action: RequiredActionRecord | None = None,
        expected_action_revision: int | None = None,
    ) -> None: ...

    async def list_attempts(self, run_id: str) -> tuple[RunAttempt, ...]: ...

    async def get_required_action(self, action_id: str) -> RequiredActionRecord: ...

    async def enter_required_action(
        self,
        action: RequiredActionRecord,
        *,
        lease: WorkLease | None = None,
    ) -> tuple[RunRecord, RunAttempt]: ...

    async def answer_required_action(
        self,
        run: RunRecord,
        action: RequiredActionRecord,
        *,
        expected_run_revision: int,
        expected_action_revision: int,
    ) -> None: ...

    async def get_plan(self, run_id: str) -> PlanRecord: ...

    async def complete_planning(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        plan: PlanRecord,
        action: RequiredActionRecord,
        *,
        expected_run_revision: int,
        expected_attempt_revision: int,
        lease: WorkLease | None = None,
    ) -> None: ...

    async def decide_plan(
        self,
        run: RunRecord,
        plan: PlanRecord,
        action: RequiredActionRecord,
        *,
        expected_run_revision: int,
        expected_plan_revision: int,
        expected_action_revision: int,
    ) -> None: ...

    async def append_event(
        self,
        *,
        run_id: str,
        attempt_id: str | None,
        source: EventSource,
        kind: str,
        data: dict[str, Any] | None = None,
    ) -> RunEvent: ...

    async def list_events(self, run_id: str, *, after: int = 0) -> tuple[RunEvent, ...]: ...

    async def append_audit(self, record: AuditRecord) -> None: ...

    async def list_audit(self, run_id: str) -> tuple[AuditRecord, ...]: ...

    async def add_artifact(self, artifact: ArtifactRecord) -> None: ...

    async def list_artifacts(self, run_id: str) -> tuple[ArtifactRecord, ...]: ...

    async def claim_work(self, *, worker_id: str, lease_seconds: float) -> WorkLease | None: ...

    async def renew_work_lease(
        self,
        lease: WorkLease,
        *,
        lease_seconds: float,
    ) -> WorkLease | None: ...

    async def release_work_lease(self, lease: WorkLease) -> bool: ...

    async def interrupt_expired_attempts(
        self,
        *,
        limit: int = 100,
    ) -> tuple[tuple[RunRecord, RunAttempt], ...]: ...


class SqliteControlStore:
    """Small durable store suitable for one control-plane process.

    Production multi-replica deployments use ``PostgresControlStore`` without
    changing the public SDK or HTTP contract.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self._connection: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self._lock:
            if self._connection is not None:
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, check_same_thread=False)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.executescript(_SCHEMA)
            _drop_session_id_uniqueness(connection)
            _normalize_succeeded_run_phases(connection)
            self._connection = connection

    async def close(self) -> None:
        async with self._lock:
            connection = self._connection
            self._connection = None
            if connection is not None:
                connection.close()

    async def create_conversation(self, conversation: ConversationRecord) -> None:
        async with self._lock:
            connection = self._require_connection()
            try:
                connection.execute(
                    """
                    INSERT INTO conversations(
                        conversation_id, tenant_id, principal_id, status,
                        next_sequence, revision, payload
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        conversation.conversation_id,
                        conversation.tenant_id,
                        conversation.principal_id,
                        conversation.status.value,
                        conversation.next_sequence,
                        conversation.revision,
                        conversation.model_dump_json(),
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    async def get_conversation(self, conversation_id: str) -> ConversationRecord:
        async with self._lock:
            return self._get_conversation_sync(self._require_connection(), conversation_id)

    async def list_conversations(
        self, *, statuses: Collection[str] = ()
    ) -> tuple[ConversationRecord, ...]:
        async with self._lock:
            connection = self._require_connection()
            selected = tuple(statuses)
            if selected:
                placeholders = ",".join("?" for _ in selected)
                rows = connection.execute(
                    f"SELECT payload FROM conversations "
                    f"WHERE status IN ({placeholders}) ORDER BY rowid",
                    selected,
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT payload FROM conversations ORDER BY rowid"
                ).fetchall()
            return tuple(ConversationRecord.model_validate_json(row["payload"]) for row in rows)

    async def replace_conversation(
        self,
        conversation: ConversationRecord,
        *,
        expected_revision: int,
    ) -> None:
        if conversation.revision != expected_revision + 1:
            raise ValueError("replacement Conversation revision must increment by one")
        async with self._lock:
            connection = self._require_connection()
            result = connection.execute(
                """
                UPDATE conversations
                SET tenant_id = ?, principal_id = ?, status = ?, next_sequence = ?,
                    revision = ?, payload = ?
                WHERE conversation_id = ? AND revision = ?
                """,
                (
                    conversation.tenant_id,
                    conversation.principal_id,
                    conversation.status.value,
                    conversation.next_sequence,
                    conversation.revision,
                    conversation.model_dump_json(),
                    conversation.conversation_id,
                    expected_revision,
                ),
            )
            if result.rowcount != 1:
                connection.rollback()
                raise RevisionConflictError(
                    f"Conversation '{conversation.conversation_id}' changed concurrently"
                )
            connection.commit()

    async def create_conversation_run(
        self,
        conversation_id: str,
        run: RunRecord,
        *,
        idempotency_key: str | None,
        request_digest: str,
    ) -> tuple[RunRecord, bool]:
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                conversation = self._get_conversation_sync(connection, conversation_id)
                if conversation.status is not ConversationStatus.ACTIVE:
                    raise StoreError(
                        f"Conversation '{conversation_id}' is {conversation.status.value}"
                    )
                if (
                    run.tenant_id != conversation.tenant_id
                    or run.principal_id != conversation.principal_id
                    or run.agent_id != conversation.agent_id
                    or run.agent_fingerprint != conversation.agent_fingerprint
                ):
                    raise StoreError("Run identity does not match its Conversation")
                if idempotency_key is not None:
                    prior = connection.execute(
                        """
                        SELECT run_id, request_digest
                        FROM run_idempotency
                        WHERE tenant_id = ? AND idempotency_key = ?
                        """,
                        (run.tenant_id, idempotency_key),
                    ).fetchone()
                    if prior is not None:
                        if prior["request_digest"] != request_digest:
                            raise IdempotencyConflictError(
                                "idempotency key was already used for another Run request"
                            )
                        existing = self._get_run_sync(connection, str(prior["run_id"]))
                        connection.commit()
                        return existing, False
                waiting_rows = connection.execute(
                    "SELECT payload FROM runs WHERE status = 'waiting'"
                ).fetchall()
                if any(
                    RunRecord.model_validate_json(row["payload"]).conversation_id
                    == conversation_id
                    for row in waiting_rows
                ):
                    raise StoreError(
                        f"Conversation '{conversation_id}' has a pending required action"
                    )
                persisted = run.model_copy(
                    update={
                        "conversation_id": conversation_id,
                        "sequence": conversation.next_sequence,
                    }
                )
                updated = conversation.model_copy(
                    update={
                        "next_sequence": conversation.next_sequence + 1,
                        "revision": conversation.revision + 1,
                        "updated_at": persisted.created_at,
                    }
                )
                connection.execute(
                    """
                    UPDATE conversations
                    SET status = ?, next_sequence = ?, revision = ?, payload = ?
                    WHERE conversation_id = ? AND revision = ?
                    """,
                    (
                        updated.status.value,
                        updated.next_sequence,
                        updated.revision,
                        updated.model_dump_json(),
                        updated.conversation_id,
                        conversation.revision,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO runs(run_id, tenant_id, status, revision, payload)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        persisted.run_id,
                        persisted.tenant_id,
                        persisted.status.value,
                        persisted.revision,
                        persisted.model_dump_json(),
                    ),
                )
                if idempotency_key is not None:
                    connection.execute(
                        """
                        INSERT INTO run_idempotency(
                            tenant_id, idempotency_key, request_digest, run_id
                        ) VALUES(?,?,?,?)
                        """,
                        (
                            persisted.tenant_id,
                            idempotency_key,
                            request_digest,
                            persisted.run_id,
                        ),
                    )
                connection.commit()
                return persisted, True
            except BaseException:
                connection.rollback()
                raise

    async def list_conversation_runs(self, conversation_id: str) -> tuple[RunRecord, ...]:
        async with self._lock:
            connection = self._require_connection()
            self._get_conversation_sync(connection, conversation_id)
            rows = connection.execute("SELECT payload FROM runs ORDER BY rowid").fetchall()
            runs = (RunRecord.model_validate_json(row["payload"]) for row in rows)
            return tuple(
                sorted(
                    (run for run in runs if run.conversation_id == conversation_id),
                    key=lambda run: run.sequence or 0,
                )
            )

    async def create_run(
        self,
        run: RunRecord,
        *,
        idempotency_key: str | None,
        request_digest: str,
    ) -> tuple[RunRecord, bool]:
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                if idempotency_key is not None:
                    prior = connection.execute(
                        """
                        SELECT run_id, request_digest
                        FROM run_idempotency
                        WHERE tenant_id = ? AND idempotency_key = ?
                        """,
                        (run.tenant_id, idempotency_key),
                    ).fetchone()
                    if prior is not None:
                        if prior["request_digest"] != request_digest:
                            raise IdempotencyConflictError(
                                "idempotency key was already used for another Run request"
                            )
                        existing = self._get_run_sync(connection, str(prior["run_id"]))
                        connection.commit()
                        return existing, False
                connection.execute(
                    """
                    INSERT INTO runs(run_id, tenant_id, status, revision, payload)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        run.run_id,
                        run.tenant_id,
                        run.status.value,
                        run.revision,
                        run.model_dump_json(),
                    ),
                )
                if idempotency_key is not None:
                    connection.execute(
                        """
                        INSERT INTO run_idempotency(
                            tenant_id, idempotency_key, request_digest, run_id
                        ) VALUES(?,?,?,?)
                        """,
                        (run.tenant_id, idempotency_key, request_digest, run.run_id),
                    )
                connection.commit()
                return run, True
            except BaseException:
                connection.rollback()
                raise

    async def get_run(self, run_id: str) -> RunRecord:
        async with self._lock:
            return self._get_run_sync(self._require_connection(), run_id)

    async def list_runs(self, *, statuses: Collection[str] = ()) -> tuple[RunRecord, ...]:
        async with self._lock:
            connection = self._require_connection()
            selected = tuple(statuses)
            if selected:
                placeholders = ",".join("?" for _ in selected)
                rows = connection.execute(
                    f"SELECT payload FROM runs WHERE status IN ({placeholders}) ORDER BY rowid",
                    selected,
                ).fetchall()
            else:
                rows = connection.execute("SELECT payload FROM runs ORDER BY rowid").fetchall()
            return tuple(RunRecord.model_validate_json(row["payload"]) for row in rows)

    async def replace_run(self, run: RunRecord, *, expected_revision: int) -> None:
        if run.revision != expected_revision + 1:
            raise ValueError("replacement Run revision must increment by one")
        async with self._lock:
            connection = self._require_connection()
            result = connection.execute(
                """
                UPDATE runs SET tenant_id = ?, status = ?, revision = ?, payload = ?
                WHERE run_id = ? AND revision = ?
                """,
                (
                    run.tenant_id,
                    run.status.value,
                    run.revision,
                    run.model_dump_json(),
                    run.run_id,
                    expected_revision,
                ),
            )
            if result.rowcount != 1:
                connection.rollback()
                raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")
            connection.commit()

    async def begin_attempt(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        expected_run_revision: int,
        lease: WorkLease | None = None,
        consumed_action: RequiredActionRecord | None = None,
        expected_action_revision: int | None = None,
    ) -> None:
        if lease is not None:
            raise StoreError("distributed Worker fencing requires PostgreSQL")
        if run.revision != expected_run_revision + 1:
            raise ValueError("replacement Run revision must increment by one")
        if (consumed_action is None) != (expected_action_revision is None):
            raise ValueError("consumed Action and expected revision must be supplied together")
        if consumed_action is not None:
            assert expected_action_revision is not None
            if consumed_action.revision != expected_action_revision + 1:
                raise ValueError("replacement RequiredAction revision must increment by one")
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                result = connection.execute(
                    """
                    UPDATE runs SET status = ?, revision = ?, payload = ?
                    WHERE run_id = ? AND revision = ?
                    """,
                    (
                        run.status.value,
                        run.revision,
                        run.model_dump_json(),
                        run.run_id,
                        expected_run_revision,
                    ),
                )
                if result.rowcount != 1:
                    raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")
                connection.execute(
                    """
                    INSERT INTO run_attempts(
                        attempt_id, run_id, number, session_id, status, revision, payload
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        attempt.attempt_id,
                        attempt.run_id,
                        attempt.number,
                        attempt.dsh_session_id,
                        attempt.status.value,
                        attempt.revision,
                        attempt.model_dump_json(),
                    ),
                )
                if consumed_action is not None:
                    action_result = connection.execute(
                        """
                        UPDATE required_actions
                        SET status = ?, revision = ?, payload = ?
                        WHERE action_id = ? AND revision = ? AND status = 'answered'
                        """,
                        (
                            consumed_action.status.value,
                            consumed_action.revision,
                            consumed_action.model_dump_json(),
                            consumed_action.action_id,
                            expected_action_revision,
                        ),
                    )
                    if action_result.rowcount != 1:
                        raise RevisionConflictError(
                            f"RequiredAction '{consumed_action.action_id}' changed concurrently"
                        )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    async def get_attempt(self, attempt_id: str) -> RunAttempt:
        async with self._lock:
            row = (
                self._require_connection()
                .execute(
                    "SELECT payload FROM run_attempts WHERE attempt_id = ?",
                    (attempt_id,),
                )
                .fetchone()
            )
            if row is None:
                raise AttemptNotFoundError(f"RunAttempt '{attempt_id}' was not found")
            return RunAttempt.model_validate_json(row["payload"])

    async def replace_attempt(
        self,
        attempt: RunAttempt,
        *,
        expected_revision: int,
        lease: WorkLease | None = None,
    ) -> None:
        if lease is not None:
            raise StoreError("distributed Worker fencing requires PostgreSQL")
        if attempt.revision != expected_revision + 1:
            raise ValueError("replacement RunAttempt revision must increment by one")
        async with self._lock:
            connection = self._require_connection()
            result = connection.execute(
                """
                UPDATE run_attempts SET status = ?, revision = ?, payload = ?
                WHERE attempt_id = ? AND revision = ?
                """,
                (
                    attempt.status.value,
                    attempt.revision,
                    attempt.model_dump_json(),
                    attempt.attempt_id,
                    expected_revision,
                ),
            )
            if result.rowcount != 1:
                connection.rollback()
                raise RevisionConflictError(
                    f"RunAttempt '{attempt.attempt_id}' changed concurrently"
                )
            connection.commit()

    async def settle_attempt(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        expected_run_revision: int,
        expected_attempt_revision: int,
        lease: WorkLease | None = None,
        settled_action: RequiredActionRecord | None = None,
        expected_action_revision: int | None = None,
    ) -> None:
        if lease is not None:
            raise StoreError("distributed Worker fencing requires PostgreSQL")
        if run.revision != expected_run_revision + 1:
            raise ValueError("replacement Run revision must increment by one")
        if attempt.revision != expected_attempt_revision + 1:
            raise ValueError("replacement RunAttempt revision must increment by one")
        if (settled_action is None) != (expected_action_revision is None):
            raise ValueError("settled Action and expected revision must be supplied together")
        if settled_action is not None:
            assert expected_action_revision is not None
            if settled_action.revision != expected_action_revision + 1:
                raise ValueError("replacement RequiredAction revision must increment by one")
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                attempt_result = connection.execute(
                    """
                    UPDATE run_attempts SET status = ?, revision = ?, payload = ?
                    WHERE attempt_id = ? AND revision = ?
                    """,
                    (
                        attempt.status.value,
                        attempt.revision,
                        attempt.model_dump_json(),
                        attempt.attempt_id,
                        expected_attempt_revision,
                    ),
                )
                if attempt_result.rowcount != 1:
                    raise RevisionConflictError(
                        f"RunAttempt '{attempt.attempt_id}' changed concurrently"
                    )
                run_result = connection.execute(
                    """
                    UPDATE runs SET status = ?, revision = ?, payload = ?
                    WHERE run_id = ? AND revision = ?
                    """,
                    (
                        run.status.value,
                        run.revision,
                        run.model_dump_json(),
                        run.run_id,
                        expected_run_revision,
                    ),
                )
                if run_result.rowcount != 1:
                    raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")
                if settled_action is not None:
                    action_result = connection.execute(
                        """
                        UPDATE required_actions SET status = ?, revision = ?, payload = ?
                        WHERE action_id = ? AND revision = ? AND status = 'pending'
                        """,
                        (
                            settled_action.status.value,
                            settled_action.revision,
                            settled_action.model_dump_json(),
                            settled_action.action_id,
                            expected_action_revision,
                        ),
                    )
                    if action_result.rowcount != 1:
                        raise RevisionConflictError(
                            f"RequiredAction '{settled_action.action_id}' changed concurrently"
                        )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    async def list_attempts(self, run_id: str) -> tuple[RunAttempt, ...]:
        async with self._lock:
            rows = (
                self._require_connection()
                .execute(
                    "SELECT payload FROM run_attempts WHERE run_id = ? ORDER BY number",
                    (run_id,),
                )
                .fetchall()
            )
            return tuple(RunAttempt.model_validate_json(row["payload"]) for row in rows)

    async def get_required_action(self, action_id: str) -> RequiredActionRecord:
        async with self._lock:
            row = (
                self._require_connection()
                .execute(
                    "SELECT payload FROM required_actions WHERE action_id = ?",
                    (action_id,),
                )
                .fetchone()
            )
            if row is None:
                raise RequiredActionNotFoundError(
                    f"RequiredAction '{action_id}' was not found"
                )
            return RequiredActionRecord.model_validate_json(row["payload"])

    async def enter_required_action(
        self,
        action: RequiredActionRecord,
        *,
        lease: WorkLease | None = None,
    ) -> tuple[RunRecord, RunAttempt]:
        if lease is not None:
            raise StoreError("distributed Worker fencing requires PostgreSQL")
        if action.status is not RequiredActionStatus.PENDING:
            raise ValueError("new RequiredAction must be pending")
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                run = self._get_run_sync(connection, action.run_id)
                attempt = self._get_attempt_sync(connection, action.attempt_id)
                if (
                    run.status is not RunStatus.RUNNING
                    or run.active_attempt_id != attempt.attempt_id
                ):
                    raise RevisionConflictError("Run is no longer executing this Attempt")
                if attempt.status is not AttemptStatus.RUNNING:
                    raise RevisionConflictError("RunAttempt is no longer running")
                if run.conversation_id != action.conversation_id:
                    raise ValueError("RequiredAction does not belong to the Run Conversation")
                now = utc_now()
                waiting_run = run.model_copy(
                    update={
                        "status": RunStatus.WAITING,
                        "required_action_id": action.action_id,
                        "revision": run.revision + 1,
                        "updated_at": now,
                    }
                )
                waiting_attempt = attempt.model_copy(
                    update={
                        "status": AttemptStatus.WAITING,
                        "dispatch_state": DispatchState.SETTLED,
                        "finish_reason": "user_input_required",
                        "revision": attempt.revision + 1,
                        "finished_at": now,
                        "updated_at": now,
                    }
                )
                connection.execute(
                    """
                    INSERT INTO required_actions(action_id, run_id, status, revision, payload)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        action.action_id,
                        action.run_id,
                        action.status.value,
                        action.revision,
                        action.model_dump_json(),
                    ),
                )
                self._update_run_sync(connection, waiting_run, run.revision)
                self._update_attempt_sync(connection, waiting_attempt, attempt.revision)
                connection.commit()
                return waiting_run, waiting_attempt
            except BaseException:
                connection.rollback()
                raise

    async def answer_required_action(
        self,
        run: RunRecord,
        action: RequiredActionRecord,
        *,
        expected_run_revision: int,
        expected_action_revision: int,
    ) -> None:
        if run.revision != expected_run_revision + 1:
            raise ValueError("replacement Run revision must increment by one")
        if action.revision != expected_action_revision + 1:
            raise ValueError("replacement RequiredAction revision must increment by one")
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                action_result = connection.execute(
                    """
                    UPDATE required_actions SET status = ?, revision = ?, payload = ?
                    WHERE action_id = ? AND revision = ? AND status = 'pending'
                    """,
                    (
                        action.status.value,
                        action.revision,
                        action.model_dump_json(),
                        action.action_id,
                        expected_action_revision,
                    ),
                )
                if action_result.rowcount != 1:
                    raise RevisionConflictError(
                        f"RequiredAction '{action.action_id}' changed concurrently"
                    )
                self._update_run_sync(connection, run, expected_run_revision)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    async def get_plan(self, run_id: str) -> PlanRecord:
        async with self._lock:
            row = (
                self._require_connection()
                .execute("SELECT payload FROM plans WHERE run_id = ?", (run_id,))
                .fetchone()
            )
            if row is None:
                raise PlanNotFoundError(f"Plan for Run '{run_id}' was not found")
            return PlanRecord.model_validate_json(row["payload"])

    async def complete_planning(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        plan: PlanRecord,
        action: RequiredActionRecord,
        *,
        expected_run_revision: int,
        expected_attempt_revision: int,
        lease: WorkLease | None = None,
    ) -> None:
        if lease is not None:
            raise StoreError("distributed Worker fencing requires PostgreSQL")
        if run.revision != expected_run_revision + 1:
            raise ValueError("replacement Run revision must increment by one")
        if attempt.revision != expected_attempt_revision + 1:
            raise ValueError("replacement RunAttempt revision must increment by one")
        if (
            plan.run_id != run.run_id
            or attempt.run_id != run.run_id
            or action.run_id != run.run_id
            or action.attempt_id != attempt.attempt_id
        ):
            raise ValueError("planning records must belong to the same Run")
        if action.type is not RequiredActionType.PLAN_APPROVAL:
            raise ValueError("planning must create a plan_approval RequiredAction")
        if action.status is not RequiredActionStatus.PENDING:
            raise ValueError("new Plan RequiredAction must be pending")
        if run.required_action_id != action.action_id:
            raise ValueError("waiting Run must point to its Plan RequiredAction")
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                attempt_result = connection.execute(
                    """
                    UPDATE run_attempts SET status = ?, revision = ?, payload = ?
                    WHERE attempt_id = ? AND revision = ?
                    """,
                    (
                        attempt.status.value,
                        attempt.revision,
                        attempt.model_dump_json(),
                        attempt.attempt_id,
                        expected_attempt_revision,
                    ),
                )
                if attempt_result.rowcount != 1:
                    raise RevisionConflictError(
                        f"RunAttempt '{attempt.attempt_id}' changed concurrently"
                    )
                run_result = connection.execute(
                    """
                    UPDATE runs SET status = ?, revision = ?, payload = ?
                    WHERE run_id = ? AND revision = ?
                    """,
                    (
                        run.status.value,
                        run.revision,
                        run.model_dump_json(),
                        run.run_id,
                        expected_run_revision,
                    ),
                )
                if run_result.rowcount != 1:
                    raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")
                connection.execute(
                    """
                    INSERT INTO plans(plan_id, run_id, status, revision, payload)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        plan.plan_id,
                        plan.run_id,
                        plan.status.value,
                        plan.revision,
                        plan.model_dump_json(),
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO required_actions(action_id, run_id, status, revision, payload)
                    VALUES(?,?,?,?,?)
                    """,
                    (
                        action.action_id,
                        action.run_id,
                        action.status.value,
                        action.revision,
                        action.model_dump_json(),
                    ),
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    async def decide_plan(
        self,
        run: RunRecord,
        plan: PlanRecord,
        action: RequiredActionRecord,
        *,
        expected_run_revision: int,
        expected_plan_revision: int,
        expected_action_revision: int,
    ) -> None:
        if run.revision != expected_run_revision + 1:
            raise ValueError("replacement Run revision must increment by one")
        if plan.revision != expected_plan_revision + 1:
            raise ValueError("replacement Plan revision must increment by one")
        if action.revision != expected_action_revision + 1:
            raise ValueError("replacement RequiredAction revision must increment by one")
        if plan.run_id != run.run_id or action.run_id != run.run_id:
            raise ValueError("Plan and RequiredAction must belong to the Run being decided")
        if action.type is not RequiredActionType.PLAN_APPROVAL:
            raise ValueError("Plan decision requires a plan_approval RequiredAction")
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                plan_result = connection.execute(
                    """
                    UPDATE plans SET status = ?, revision = ?, payload = ?
                    WHERE plan_id = ? AND revision = ?
                    """,
                    (
                        plan.status.value,
                        plan.revision,
                        plan.model_dump_json(),
                        plan.plan_id,
                        expected_plan_revision,
                    ),
                )
                if plan_result.rowcount != 1:
                    raise RevisionConflictError(f"Plan '{plan.plan_id}' changed concurrently")
                run_result = connection.execute(
                    """
                    UPDATE runs SET status = ?, revision = ?, payload = ?
                    WHERE run_id = ? AND revision = ?
                    """,
                    (
                        run.status.value,
                        run.revision,
                        run.model_dump_json(),
                        run.run_id,
                        expected_run_revision,
                    ),
                )
                if run_result.rowcount != 1:
                    raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")
                action_result = connection.execute(
                    """
                    UPDATE required_actions SET status = ?, revision = ?, payload = ?
                    WHERE action_id = ? AND revision = ? AND status = 'pending'
                    """,
                    (
                        action.status.value,
                        action.revision,
                        action.model_dump_json(),
                        action.action_id,
                        expected_action_revision,
                    ),
                )
                if action_result.rowcount != 1:
                    raise RevisionConflictError(
                        f"RequiredAction '{action.action_id}' changed concurrently"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    async def append_event(
        self,
        *,
        run_id: str,
        attempt_id: str | None,
        source: EventSource,
        kind: str,
        data: dict[str, Any] | None = None,
    ) -> RunEvent:
        async with self._lock:
            connection = self._require_connection()
            connection.execute("BEGIN IMMEDIATE")
            try:
                exists = connection.execute(
                    "SELECT 1 FROM runs WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
                if exists is None:
                    raise RunNotFoundError(f"Run '{run_id}' was not found")
                value = connection.execute(
                    "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_events WHERE run_id = ?",
                    (run_id,),
                ).fetchone()[0]
                event = RunEvent(
                    run_id=run_id,
                    attempt_id=attempt_id,
                    sequence=int(value),
                    source=source,
                    kind=kind,
                    data=data or {},
                )
                connection.execute(
                    """
                    INSERT INTO run_events(event_id, run_id, sequence, source, kind, payload)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (
                        event.event_id,
                        event.run_id,
                        event.sequence,
                        event.source.value,
                        event.kind,
                        event.model_dump_json(),
                    ),
                )
                connection.commit()
                return event
            except BaseException:
                connection.rollback()
                raise

    async def list_events(self, run_id: str, *, after: int = 0) -> tuple[RunEvent, ...]:
        async with self._lock:
            rows = (
                self._require_connection()
                .execute(
                    """
                SELECT payload FROM run_events
                WHERE run_id = ? AND sequence > ? ORDER BY sequence
                """,
                    (run_id, after),
                )
                .fetchall()
            )
            return tuple(RunEvent.model_validate_json(row["payload"]) for row in rows)

    async def append_audit(self, record: AuditRecord) -> None:
        async with self._lock:
            connection = self._require_connection()
            connection.execute(
                """
                INSERT INTO audit_log(
                    audit_id, tenant_id, principal_id, action, outcome, run_id, payload
                ) VALUES(?,?,?,?,?,?,?)
                """,
                (
                    record.audit_id,
                    record.tenant_id,
                    record.principal_id,
                    record.action,
                    record.outcome,
                    record.run_id,
                    record.model_dump_json(),
                ),
            )
            connection.commit()

    async def list_audit(self, run_id: str) -> tuple[AuditRecord, ...]:
        async with self._lock:
            rows = (
                self._require_connection()
                .execute(
                    "SELECT payload FROM audit_log WHERE run_id = ? ORDER BY rowid",
                    (run_id,),
                )
                .fetchall()
            )
            return tuple(AuditRecord.model_validate_json(row["payload"]) for row in rows)

    async def add_artifact(self, artifact: ArtifactRecord) -> None:
        async with self._lock:
            connection = self._require_connection()
            connection.execute(
                """
                INSERT INTO artifacts(artifact_id, run_id, attempt_id, payload)
                VALUES(?,?,?,?)
                """,
                (
                    artifact.artifact_id,
                    artifact.run_id,
                    artifact.attempt_id,
                    artifact.model_dump_json(),
                ),
            )
            connection.commit()

    async def list_artifacts(self, run_id: str) -> tuple[ArtifactRecord, ...]:
        async with self._lock:
            rows = (
                self._require_connection()
                .execute(
                    "SELECT payload FROM artifacts WHERE run_id = ? ORDER BY rowid",
                    (run_id,),
                )
                .fetchall()
            )
            return tuple(ArtifactRecord.model_validate_json(row["payload"]) for row in rows)

    async def claim_work(self, *, worker_id: str, lease_seconds: float) -> WorkLease | None:
        del worker_id, lease_seconds
        raise StoreError("distributed Worker leases require PostgreSQL")

    async def renew_work_lease(
        self,
        lease: WorkLease,
        *,
        lease_seconds: float,
    ) -> WorkLease | None:
        del lease, lease_seconds
        raise StoreError("distributed Worker leases require PostgreSQL")

    async def release_work_lease(self, lease: WorkLease) -> bool:
        del lease
        raise StoreError("distributed Worker leases require PostgreSQL")

    async def interrupt_expired_attempts(
        self,
        *,
        limit: int = 100,
    ) -> tuple[tuple[RunRecord, RunAttempt], ...]:
        del limit
        raise StoreError("distributed Worker recovery requires PostgreSQL")

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise StoreError("ControlStore is not initialized")
        return self._connection

    @staticmethod
    def _get_run_sync(connection: sqlite3.Connection, run_id: str) -> RunRecord:
        row = connection.execute(
            "SELECT payload FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise RunNotFoundError(f"Run '{run_id}' was not found")
        return RunRecord.model_validate_json(row["payload"])

    @staticmethod
    def _get_conversation_sync(
        connection: sqlite3.Connection,
        conversation_id: str,
    ) -> ConversationRecord:
        row = connection.execute(
            "SELECT payload FROM conversations WHERE conversation_id = ?",
            (conversation_id,),
        ).fetchone()
        if row is None:
            raise ConversationNotFoundError(f"Conversation '{conversation_id}' was not found")
        return ConversationRecord.model_validate_json(row["payload"])

    @staticmethod
    def _get_attempt_sync(connection: sqlite3.Connection, attempt_id: str) -> RunAttempt:
        row = connection.execute(
            "SELECT payload FROM run_attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if row is None:
            raise AttemptNotFoundError(f"RunAttempt '{attempt_id}' was not found")
        return RunAttempt.model_validate_json(row["payload"])

    @staticmethod
    def _update_run_sync(
        connection: sqlite3.Connection,
        run: RunRecord,
        expected_revision: int,
    ) -> None:
        result = connection.execute(
            """
            UPDATE runs SET tenant_id = ?, status = ?, revision = ?, payload = ?
            WHERE run_id = ? AND revision = ?
            """,
            (
                run.tenant_id,
                run.status.value,
                run.revision,
                run.model_dump_json(),
                run.run_id,
                expected_revision,
            ),
        )
        if result.rowcount != 1:
            raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")

    @staticmethod
    def _update_attempt_sync(
        connection: sqlite3.Connection,
        attempt: RunAttempt,
        expected_revision: int,
    ) -> None:
        result = connection.execute(
            """
            UPDATE run_attempts SET status = ?, revision = ?, payload = ?
            WHERE attempt_id = ? AND revision = ?
            """,
            (
                attempt.status.value,
                attempt.revision,
                attempt.model_dump_json(),
                attempt.attempt_id,
                expected_revision,
            ),
        )
        if result.rowcount != 1:
            raise RevisionConflictError(
                f"RunAttempt '{attempt.attempt_id}' changed concurrently"
            )


def request_digest(value: dict[str, Any]) -> str:
    canonical = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


_SCHEMA = """
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    status TEXT NOT NULL,
    next_sequence INTEGER NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_conversations_owner_status
ON conversations(tenant_id, principal_id, status);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_runs_tenant_status ON runs(tenant_id, status);

CREATE TABLE IF NOT EXISTS run_idempotency (
    tenant_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    PRIMARY KEY(tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS run_attempts (
    attempt_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    number INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE(run_id, number)
);
CREATE INDEX IF NOT EXISTS ix_run_attempts_session ON run_attempts(session_id);

CREATE TABLE IF NOT EXISTS plans (
    plan_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE REFERENCES runs(run_id),
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_plans_run_status ON plans(run_id, status);

CREATE TABLE IF NOT EXISTS run_events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    sequence INTEGER NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE(run_id, sequence)
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    action TEXT NOT NULL,
    outcome TEXT NOT NULL,
    run_id TEXT,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_audit_run ON audit_log(run_id);

CREATE TABLE IF NOT EXISTS artifacts (
    artifact_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    attempt_id TEXT NOT NULL REFERENCES run_attempts(attempt_id),
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS required_actions (
    action_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    status TEXT NOT NULL,
    revision INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_required_actions_run
ON required_actions(run_id, status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_required_actions_pending_run
ON required_actions(run_id) WHERE status = 'pending';
"""


def _drop_session_id_uniqueness(connection: sqlite3.Connection) -> None:
    """Migrate the v0.1 attempt table so one Conversation can reuse a Session."""

    row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'run_attempts'"
    ).fetchone()
    definition = "" if row is None or row["sql"] is None else str(row["sql"])
    normalized = " ".join(definition.lower().split())
    if "session_id text not null unique" not in normalized:
        return
    connection.execute("PRAGMA foreign_keys=OFF")
    try:
        connection.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE run_attempts_v02 (
                attempt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                number INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                status TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload TEXT NOT NULL,
                UNIQUE(run_id, number)
            );
            INSERT INTO run_attempts_v02(
                attempt_id, run_id, number, session_id, status, revision, payload
            )
            SELECT attempt_id, run_id, number, session_id, status, revision, payload
            FROM run_attempts;
            DROP TABLE run_attempts;
            ALTER TABLE run_attempts_v02 RENAME TO run_attempts;
            CREATE INDEX ix_run_attempts_session ON run_attempts(session_id);
            COMMIT;
            """
        )
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.execute("PRAGMA foreign_keys=ON")


def _normalize_succeeded_run_phases(connection: sqlite3.Connection) -> None:
    """Backfill the terminal phase introduced after Plan-ReAct's first release."""

    rows = connection.execute(
        "SELECT run_id, payload FROM runs WHERE status = 'succeeded'"
    ).fetchall()
    for row in rows:
        payload = json.loads(row["payload"])
        if payload.get("phase") == "completed":
            continue
        payload["phase"] = "completed"
        connection.execute(
            "UPDATE runs SET payload = ? WHERE run_id = ?",
            (
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                row["run_id"],
            ),
        )
    connection.commit()


__all__ = [
    "AttemptNotFoundError",
    "ConversationNotFoundError",
    "ControlStore",
    "IdempotencyConflictError",
    "LeaseLostError",
    "PlanNotFoundError",
    "RequiredActionNotFoundError",
    "RevisionConflictError",
    "RunNotFoundError",
    "SqliteControlStore",
    "StoreError",
    "request_digest",
]
