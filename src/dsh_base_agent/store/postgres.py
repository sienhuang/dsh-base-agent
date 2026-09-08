"""PostgreSQL implementation of the business control-plane store."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Collection
from typing import Any

import asyncpg
from asyncpg.pool import PoolConnectionProxy
from pydantic import BaseModel

from dsh_base_agent.control.models import (
    ArtifactRecord,
    AttemptStatus,
    AuditRecord,
    ConversationRecord,
    ConversationStatus,
    DispatchState,
    EventSource,
    RunAttempt,
    RunEvent,
    RunRecord,
    RunStatus,
    WorkLease,
    WorkResourceType,
    utc_now,
)
from dsh_base_agent.store.postgres_migrations import POSTGRES_MIGRATIONS
from dsh_base_agent.store.sqlite import (
    AttemptNotFoundError,
    ConversationNotFoundError,
    IdempotencyConflictError,
    LeaseLostError,
    RevisionConflictError,
    RunNotFoundError,
    StoreError,
)

_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
type _PgConnection = asyncpg.Connection[asyncpg.Record] | PoolConnectionProxy[asyncpg.Record]


class PostgresControlStore:
    """Async, pooled PostgreSQL store for multi-process control planes.

    Business records retain a JSONB snapshot for lossless model round-tripping while
    ownership, ordering, status and CAS fields are also stored as first-class columns.
    """

    def __init__(
        self,
        database_url: str,
        *,
        schema: str = "dsh_base_agent",
        min_pool_size: int = 1,
        max_pool_size: int = 10,
        command_timeout_seconds: float = 30.0,
        statement_cache_size: int = 100,
    ) -> None:
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("PostgresControlStore requires a PostgreSQL database URL")
        if not _SCHEMA_NAME.fullmatch(schema):
            raise ValueError("PostgreSQL schema must be a simple SQL identifier")
        if min_pool_size < 0 or max_pool_size < 1 or min_pool_size > max_pool_size:
            raise ValueError("invalid PostgreSQL pool size")
        if command_timeout_seconds <= 0:
            raise ValueError("PostgreSQL command timeout must be positive")
        if statement_cache_size < 0:
            raise ValueError("PostgreSQL statement cache size cannot be negative")
        self._database_url = database_url
        self.schema = schema
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        self.command_timeout_seconds = command_timeout_seconds
        self.statement_cache_size = statement_cache_size
        self._pool: asyncpg.Pool | None = None
        self._initialize_lock = asyncio.Lock()

    async def initialize(self) -> None:
        async with self._initialize_lock:
            if self._pool is not None:
                return
            pool = await asyncpg.create_pool(
                dsn=self._database_url,
                min_size=self.min_pool_size,
                max_size=self.max_pool_size,
                command_timeout=self.command_timeout_seconds,
                statement_cache_size=self.statement_cache_size,
                server_settings={
                    "application_name": "dsh-base-agent",
                    "search_path": f'"{self.schema}", public',
                },
            )
            if pool is None:  # pragma: no cover - defensive for asyncpg's overloaded API
                raise StoreError("asyncpg did not create a connection pool")
            try:
                await self._migrate(pool)
            except BaseException:
                await pool.close()
                raise
            self._pool = pool

    async def close(self) -> None:
        async with self._initialize_lock:
            pool = self._pool
            self._pool = None
            if pool is not None:
                await pool.close()

    async def create_conversation(self, conversation: ConversationRecord) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO conversations(
                    conversation_id, tenant_id, principal_id, agent_id, agent_version,
                    agent_fingerprint, dsh_home_key, dsh_session_id, status, next_sequence,
                    revision, blocked_reason, metadata, created_at, updated_at, payload
                ) VALUES(
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16::jsonb
                )
                """,
                conversation.conversation_id,
                conversation.tenant_id,
                conversation.principal_id,
                conversation.agent_id,
                conversation.agent_version,
                conversation.agent_fingerprint,
                conversation.dsh_home_key,
                conversation.dsh_session_id,
                conversation.status.value,
                conversation.next_sequence,
                conversation.revision,
                conversation.blocked_reason,
                _json(conversation.metadata),
                conversation.created_at,
                conversation.updated_at,
                conversation.model_dump_json(),
            )

    async def get_conversation(self, conversation_id: str) -> ConversationRecord:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT payload FROM conversations WHERE conversation_id = $1",
                conversation_id,
            )
        if row is None:
            raise ConversationNotFoundError(f"Conversation '{conversation_id}' was not found")
        return _model_from_payload(ConversationRecord, row["payload"])

    async def list_conversations(
        self, *, statuses: Collection[str] = ()
    ) -> tuple[ConversationRecord, ...]:
        pool = self._require_pool()
        selected = list(statuses)
        async with pool.acquire() as connection:
            if selected:
                rows = await connection.fetch(
                    """
                    SELECT payload FROM conversations
                    WHERE status = ANY($1::text[])
                    ORDER BY created_at, conversation_id
                    """,
                    selected,
                )
            else:
                rows = await connection.fetch(
                    "SELECT payload FROM conversations ORDER BY created_at, conversation_id"
                )
        return tuple(_model_from_payload(ConversationRecord, row["payload"]) for row in rows)

    async def replace_conversation(
        self,
        conversation: ConversationRecord,
        *,
        expected_revision: int,
    ) -> None:
        _validate_revision(conversation.revision, expected_revision, "Conversation")
        pool = self._require_pool()
        async with pool.acquire() as connection:
            updated = await connection.fetchval(
                """
                UPDATE conversations
                SET tenant_id = $1, principal_id = $2, agent_id = $3, agent_version = $4,
                    agent_fingerprint = $5, dsh_home_key = $6, dsh_session_id = $7,
                    status = $8, next_sequence = $9, revision = $10, blocked_reason = $11,
                    metadata = $12::jsonb, created_at = $13, updated_at = $14,
                    payload = $15::jsonb
                WHERE conversation_id = $16 AND revision = $17
                RETURNING conversation_id
                """,
                conversation.tenant_id,
                conversation.principal_id,
                conversation.agent_id,
                conversation.agent_version,
                conversation.agent_fingerprint,
                conversation.dsh_home_key,
                conversation.dsh_session_id,
                conversation.status.value,
                conversation.next_sequence,
                conversation.revision,
                conversation.blocked_reason,
                _json(conversation.metadata),
                conversation.created_at,
                conversation.updated_at,
                conversation.model_dump_json(),
                conversation.conversation_id,
                expected_revision,
            )
        if updated is None:
            raise RevisionConflictError(
                f"Conversation '{conversation.conversation_id}' changed concurrently"
            )

    async def create_conversation_run(
        self,
        conversation_id: str,
        run: RunRecord,
        *,
        idempotency_key: str | None,
        request_digest: str,
    ) -> tuple[RunRecord, bool]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            if idempotency_key is not None:
                await _lock_idempotency(connection, run.tenant_id, idempotency_key)
            row = await connection.fetchrow(
                """
                SELECT payload FROM conversations
                WHERE conversation_id = $1
                FOR UPDATE
                """,
                conversation_id,
            )
            if row is None:
                raise ConversationNotFoundError(f"Conversation '{conversation_id}' was not found")
            conversation = _model_from_payload(ConversationRecord, row["payload"])
            if conversation.status is not ConversationStatus.ACTIVE:
                raise StoreError(f"Conversation '{conversation_id}' is {conversation.status.value}")
            if (
                run.tenant_id != conversation.tenant_id
                or run.principal_id != conversation.principal_id
                or run.agent_id != conversation.agent_id
                or run.agent_fingerprint != conversation.agent_fingerprint
            ):
                raise StoreError("Run identity does not match its Conversation")
            if idempotency_key is not None:
                prior = await connection.fetchrow(
                    """
                    SELECT run_id, request_digest FROM run_idempotency
                    WHERE tenant_id = $1 AND idempotency_key = $2
                    """,
                    run.tenant_id,
                    idempotency_key,
                )
                if prior is not None:
                    if prior["request_digest"] != request_digest:
                        raise IdempotencyConflictError(
                            "idempotency key was already used for another Run request"
                        )
                    return await self._get_run(connection, str(prior["run_id"])), False

            persisted = run.model_copy(
                update={
                    "conversation_id": conversation_id,
                    "sequence": conversation.next_sequence,
                }
            )
            updated_conversation = conversation.model_copy(
                update={
                    "next_sequence": conversation.next_sequence + 1,
                    "revision": conversation.revision + 1,
                    "updated_at": persisted.created_at,
                }
            )
            changed = await connection.fetchval(
                """
                UPDATE conversations
                SET next_sequence = $1, revision = $2, updated_at = $3, payload = $4::jsonb
                WHERE conversation_id = $5 AND revision = $6
                RETURNING conversation_id
                """,
                updated_conversation.next_sequence,
                updated_conversation.revision,
                updated_conversation.updated_at,
                updated_conversation.model_dump_json(),
                conversation_id,
                conversation.revision,
            )
            if changed is None:
                raise RevisionConflictError(
                    f"Conversation '{conversation_id}' changed concurrently"
                )
            await self._insert_run(connection, persisted)
            if idempotency_key is not None:
                await self._insert_idempotency(
                    connection,
                    persisted,
                    idempotency_key=idempotency_key,
                    request_digest=request_digest,
                )
            return persisted, True

    async def list_conversation_runs(self, conversation_id: str) -> tuple[RunRecord, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            exists = await connection.fetchval(
                "SELECT 1 FROM conversations WHERE conversation_id = $1", conversation_id
            )
            if exists is None:
                raise ConversationNotFoundError(f"Conversation '{conversation_id}' was not found")
            rows = await connection.fetch(
                """
                SELECT payload FROM runs
                WHERE conversation_id = $1
                ORDER BY sequence
                """,
                conversation_id,
            )
        return tuple(_model_from_payload(RunRecord, row["payload"]) for row in rows)

    async def create_run(
        self,
        run: RunRecord,
        *,
        idempotency_key: str | None,
        request_digest: str,
    ) -> tuple[RunRecord, bool]:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            if idempotency_key is not None:
                await _lock_idempotency(connection, run.tenant_id, idempotency_key)
                prior = await connection.fetchrow(
                    """
                    SELECT run_id, request_digest FROM run_idempotency
                    WHERE tenant_id = $1 AND idempotency_key = $2
                    """,
                    run.tenant_id,
                    idempotency_key,
                )
                if prior is not None:
                    if prior["request_digest"] != request_digest:
                        raise IdempotencyConflictError(
                            "idempotency key was already used for another Run request"
                        )
                    return await self._get_run(connection, str(prior["run_id"])), False
            await self._insert_run(connection, run)
            if idempotency_key is not None:
                await self._insert_idempotency(
                    connection,
                    run,
                    idempotency_key=idempotency_key,
                    request_digest=request_digest,
                )
            return run, True

    async def get_run(self, run_id: str) -> RunRecord:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            return await self._get_run(connection, run_id)

    async def list_runs(self, *, statuses: Collection[str] = ()) -> tuple[RunRecord, ...]:
        pool = self._require_pool()
        selected = list(statuses)
        async with pool.acquire() as connection:
            if selected:
                rows = await connection.fetch(
                    """
                    SELECT payload FROM runs
                    WHERE status = ANY($1::text[])
                    ORDER BY created_at, run_id
                    """,
                    selected,
                )
            else:
                rows = await connection.fetch(
                    "SELECT payload FROM runs ORDER BY created_at, run_id"
                )
        return tuple(_model_from_payload(RunRecord, row["payload"]) for row in rows)

    async def replace_run(self, run: RunRecord, *, expected_revision: int) -> None:
        _validate_revision(run.revision, expected_revision, "Run")
        pool = self._require_pool()
        async with pool.acquire() as connection:
            updated = await self._update_run(connection, run, expected_revision)
        if updated is None:
            raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")

    async def begin_attempt(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        expected_run_revision: int,
        lease: WorkLease | None = None,
    ) -> None:
        _validate_revision(run.revision, expected_run_revision, "Run")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            if lease is not None:
                await self._assert_lease(connection, lease)
            await self._insert_attempt(connection, attempt)
            updated = await self._update_run(connection, run, expected_run_revision)
            if updated is None:
                raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")

    async def get_attempt(self, attempt_id: str) -> RunAttempt:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT payload FROM run_attempts WHERE attempt_id = $1", attempt_id
            )
        if row is None:
            raise AttemptNotFoundError(f"RunAttempt '{attempt_id}' was not found")
        return _model_from_payload(RunAttempt, row["payload"])

    async def replace_attempt(
        self,
        attempt: RunAttempt,
        *,
        expected_revision: int,
        lease: WorkLease | None = None,
    ) -> None:
        _validate_revision(attempt.revision, expected_revision, "RunAttempt")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            if lease is not None:
                await self._assert_lease(connection, lease)
            updated = await self._update_attempt(connection, attempt, expected_revision)
        if updated is None:
            raise RevisionConflictError(f"RunAttempt '{attempt.attempt_id}' changed concurrently")

    async def settle_attempt(
        self,
        run: RunRecord,
        attempt: RunAttempt,
        *,
        expected_run_revision: int,
        expected_attempt_revision: int,
        lease: WorkLease | None = None,
    ) -> None:
        _validate_revision(run.revision, expected_run_revision, "Run")
        _validate_revision(attempt.revision, expected_attempt_revision, "RunAttempt")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            if lease is not None:
                await self._assert_lease(connection, lease)
            attempt_updated = await self._update_attempt(
                connection, attempt, expected_attempt_revision
            )
            if attempt_updated is None:
                raise RevisionConflictError(
                    f"RunAttempt '{attempt.attempt_id}' changed concurrently"
                )
            run_updated = await self._update_run(connection, run, expected_run_revision)
            if run_updated is None:
                raise RevisionConflictError(f"Run '{run.run_id}' changed concurrently")

    async def list_attempts(self, run_id: str) -> tuple[RunAttempt, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT payload FROM run_attempts
                WHERE run_id = $1
                ORDER BY number
                """,
                run_id,
            )
        return tuple(_model_from_payload(RunAttempt, row["payload"]) for row in rows)

    async def append_event(
        self,
        *,
        run_id: str,
        attempt_id: str | None,
        source: EventSource,
        kind: str,
        data: dict[str, Any] | None = None,
    ) -> RunEvent:
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            return await self._append_event(
                connection,
                run_id=run_id,
                attempt_id=attempt_id,
                source=source,
                kind=kind,
                data=data or {},
            )

    async def list_events(self, run_id: str, *, after: int = 0) -> tuple[RunEvent, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT payload FROM run_events
                WHERE run_id = $1 AND sequence > $2
                ORDER BY sequence
                """,
                run_id,
                after,
            )
        return tuple(_model_from_payload(RunEvent, row["payload"]) for row in rows)

    async def append_audit(self, record: AuditRecord) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            await self._insert_audit(connection, record)

    async def list_audit(self, run_id: str) -> tuple[AuditRecord, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT payload FROM audit_log
                WHERE run_id = $1
                ORDER BY created_at, audit_id
                """,
                run_id,
            )
        return tuple(_model_from_payload(AuditRecord, row["payload"]) for row in rows)

    async def add_artifact(self, artifact: ArtifactRecord) -> None:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO artifacts(
                    artifact_id, run_id, attempt_id, name, media_type, location, sha256,
                    size_bytes, created_at, payload
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
                """,
                artifact.artifact_id,
                artifact.run_id,
                artifact.attempt_id,
                artifact.name,
                artifact.media_type,
                artifact.location,
                artifact.sha256,
                artifact.size_bytes,
                artifact.created_at,
                artifact.model_dump_json(),
            )

    async def list_artifacts(self, run_id: str) -> tuple[ArtifactRecord, ...]:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT payload FROM artifacts
                WHERE run_id = $1
                ORDER BY created_at, artifact_id
                """,
                run_id,
            )
        return tuple(_model_from_payload(ArtifactRecord, row["payload"]) for row in rows)

    async def claim_work(self, *, worker_id: str, lease_seconds: float) -> WorkLease | None:
        """Claim the oldest runnable standalone Run or Conversation.

        Candidate Run rows are protected with ``SKIP LOCKED`` while the independent
        resource lease prevents two candidates from the same Conversation being
        claimed by different Workers.
        """

        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        pool = self._require_pool()
        async with pool.acquire() as connection, connection.transaction():
            candidates = await connection.fetch(
                """
                SELECT r.run_id, r.conversation_id
                FROM runs AS r
                LEFT JOIN conversations AS c
                    ON c.conversation_id = r.conversation_id
                WHERE r.status = 'queued'
                  AND (r.conversation_id IS NULL OR c.status = 'active')
                  AND (
                      r.conversation_id IS NULL
                      OR NOT EXISTS (
                          SELECT 1
                          FROM runs AS active
                          WHERE active.conversation_id = r.conversation_id
                            AND active.status IN ('running', 'waiting')
                      )
                  )
                ORDER BY r.created_at, r.run_id
                FOR UPDATE OF r SKIP LOCKED
                LIMIT 32
                """
            )
            for candidate in candidates:
                conversation_id = candidate["conversation_id"]
                resource_type = (
                    WorkResourceType.RUN
                    if conversation_id is None
                    else WorkResourceType.CONVERSATION
                )
                resource_id = str(
                    candidate["run_id"] if conversation_id is None else conversation_id
                )
                row = await connection.fetchrow(
                    """
                    INSERT INTO worker_leases(
                        resource_type, resource_id, worker_id, lease_token,
                        expires_at, updated_at
                    ) VALUES(
                        $1, $2, $3, 1,
                        CURRENT_TIMESTAMP + ($4 * INTERVAL '1 second'),
                        CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(resource_type, resource_id) DO UPDATE
                    SET worker_id = EXCLUDED.worker_id,
                        lease_token = worker_leases.lease_token + 1,
                        expires_at = EXCLUDED.expires_at,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE worker_leases.expires_at <= CURRENT_TIMESTAMP
                    RETURNING resource_type, resource_id, worker_id,
                              lease_token, expires_at
                    """,
                    resource_type.value,
                    resource_id,
                    worker_id,
                    lease_seconds,
                )
                if row is not None:
                    return _work_lease(row)
        return None

    async def renew_work_lease(
        self,
        lease: WorkLease,
        *,
        lease_seconds: float,
    ) -> WorkLease | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        pool = self._require_pool()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                UPDATE worker_leases
                SET expires_at = CURRENT_TIMESTAMP + ($5 * INTERVAL '1 second'),
                    updated_at = CURRENT_TIMESTAMP
                WHERE resource_type = $1
                  AND resource_id = $2
                  AND worker_id = $3
                  AND lease_token = $4
                  AND expires_at > CURRENT_TIMESTAMP
                RETURNING resource_type, resource_id, worker_id,
                          lease_token, expires_at
                """,
                lease.resource_type.value,
                lease.resource_id,
                lease.worker_id,
                lease.lease_token,
                lease_seconds,
            )
        return None if row is None else _work_lease(row)

    async def release_work_lease(self, lease: WorkLease) -> bool:
        pool = self._require_pool()
        async with pool.acquire() as connection:
            released = await connection.fetchval(
                """
                UPDATE worker_leases
                SET expires_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE resource_type = $1
                  AND resource_id = $2
                  AND worker_id = $3
                  AND lease_token = $4
                RETURNING TRUE
                """,
                lease.resource_type.value,
                lease.resource_id,
                lease.worker_id,
                lease.lease_token,
            )
        return bool(released)

    async def interrupt_expired_attempts(
        self,
        *,
        limit: int = 100,
    ) -> tuple[tuple[RunRecord, RunAttempt], ...]:
        """Settle abandoned running work without replaying an uncertain DSH prompt."""

        if limit <= 0:
            raise ValueError("recovery limit must be positive")
        pool = self._require_pool()
        recovered: list[tuple[RunRecord, RunAttempt]] = []
        async with pool.acquire() as connection, connection.transaction():
            rows = await connection.fetch(
                """
                SELECT r.payload AS run_payload, a.payload AS attempt_payload
                FROM runs AS r
                JOIN run_attempts AS a ON a.attempt_id = r.active_attempt_id
                LEFT JOIN worker_leases AS lease
                  ON lease.resource_type = CASE
                      WHEN r.conversation_id IS NULL THEN 'run'
                      ELSE 'conversation'
                  END
                 AND lease.resource_id = COALESCE(r.conversation_id, r.run_id)
                 AND lease.worker_id = a.worker_id
                 AND lease.lease_token = a.lease_token
                WHERE r.status = 'running'
                  AND a.status = 'running'
                  AND a.worker_id IS NOT NULL
                  AND a.lease_token IS NOT NULL
                  AND (lease.resource_id IS NULL OR lease.expires_at <= CURRENT_TIMESTAMP)
                ORDER BY r.updated_at, r.run_id
                FOR UPDATE OF r, a SKIP LOCKED
                LIMIT $1
                """,
                limit,
            )
            for row in rows:
                run = _model_from_payload(RunRecord, row["run_payload"])
                attempt = _model_from_payload(RunAttempt, row["attempt_payload"])
                now = utc_now()
                message = (
                    "Worker lease expired during DSH execution; the prompt delivery "
                    "boundary is uncertain, so the interrupted Run will not be replayed"
                )
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
                failed = run.model_copy(
                    update={
                        "status": RunStatus.FAILED,
                        "revision": run.revision + 1,
                        "error": message,
                        "updated_at": now,
                    }
                )
                attempt_updated = await self._update_attempt(
                    connection,
                    interrupted,
                    attempt.revision,
                )
                run_updated = await self._update_run(connection, failed, run.revision)
                if attempt_updated is None or run_updated is None:
                    raise RevisionConflictError(
                        f"expired Run '{run.run_id}' changed during recovery"
                    )
                await self._append_event(
                    connection,
                    run_id=failed.run_id,
                    attempt_id=interrupted.attempt_id,
                    source=EventSource.CONTROL,
                    kind="run.worker_lease_expired",
                    data={
                        "worker_id": interrupted.worker_id,
                        "lease_token": interrupted.lease_token,
                        "dispatch_state": interrupted.dispatch_state.value,
                    },
                )
                await self._insert_audit(
                    connection,
                    AuditRecord(
                        tenant_id=failed.tenant_id,
                        principal_id="system",
                        action="run.worker_lease.expire",
                        outcome="interrupted",
                        run_id=failed.run_id,
                        attempt_id=interrupted.attempt_id,
                        agent_id=failed.agent_id,
                        data={
                            "worker_id": interrupted.worker_id,
                            "lease_token": interrupted.lease_token,
                        },
                    ),
                )
                await connection.execute(
                    """
                    UPDATE worker_leases
                    SET expires_at = LEAST(expires_at, CURRENT_TIMESTAMP),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE resource_type = CASE
                            WHEN $1::text IS NULL THEN 'run'
                            ELSE 'conversation'
                          END
                      AND resource_id = COALESCE($1, $2)
                      AND worker_id = $3
                      AND lease_token = $4
                    """,
                    run.conversation_id,
                    run.run_id,
                    attempt.worker_id,
                    attempt.lease_token,
                )
                recovered.append((failed, interrupted))
        return tuple(recovered)

    @staticmethod
    async def _assert_lease(connection: _PgConnection, lease: WorkLease) -> None:
        valid = await connection.fetchval(
            """
            SELECT TRUE
            FROM worker_leases
            WHERE resource_type = $1
              AND resource_id = $2
              AND worker_id = $3
              AND lease_token = $4
              AND expires_at > CURRENT_TIMESTAMP
            FOR UPDATE
            """,
            lease.resource_type.value,
            lease.resource_id,
            lease.worker_id,
            lease.lease_token,
        )
        if not valid:
            raise LeaseLostError(
                f"Worker lease for {lease.resource_type.value} "
                f"'{lease.resource_id}' is no longer valid"
            )

    async def _migrate(self, pool: asyncpg.Pool) -> None:
        async with pool.acquire() as connection:
            await connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
            async with connection.transaction():
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"dsh-base-agent:migrations:{self.schema}",
                )
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        checksum TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                applied_rows = await connection.fetch(
                    "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
                )
                applied = {int(row["version"]): row for row in applied_rows}
                for version, name, sql in POSTGRES_MIGRATIONS:
                    checksum = hashlib.sha256(sql.encode()).hexdigest()
                    prior = applied.get(version)
                    if prior is not None:
                        if prior["name"] != name or prior["checksum"] != checksum:
                            raise StoreError(
                                f"PostgreSQL migration {version} differs from the applied version"
                            )
                        continue
                    await connection.execute(sql)
                    await connection.execute(
                        """
                        INSERT INTO schema_migrations(version, name, checksum)
                        VALUES($1, $2, $3)
                        """,
                        version,
                        name,
                        checksum,
                    )

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise StoreError("ControlStore is not initialized")
        return self._pool

    @staticmethod
    async def _get_run(connection: _PgConnection, run_id: str) -> RunRecord:
        row = await connection.fetchrow("SELECT payload FROM runs WHERE run_id = $1", run_id)
        if row is None:
            raise RunNotFoundError(f"Run '{run_id}' was not found")
        return _model_from_payload(RunRecord, row["payload"])

    @staticmethod
    async def _append_event(
        connection: _PgConnection,
        *,
        run_id: str,
        attempt_id: str | None,
        source: EventSource,
        kind: str,
        data: dict[str, Any],
    ) -> RunEvent:
        exists = await connection.fetchval(
            "SELECT 1 FROM runs WHERE run_id = $1 FOR UPDATE", run_id
        )
        if exists is None:
            raise RunNotFoundError(f"Run '{run_id}' was not found")
        sequence = await connection.fetchval(
            """
            SELECT COALESCE(MAX(sequence), 0) + 1
            FROM run_events WHERE run_id = $1
            """,
            run_id,
        )
        event = RunEvent(
            run_id=run_id,
            attempt_id=attempt_id,
            sequence=int(sequence),
            source=source,
            kind=kind,
            data=data,
        )
        await connection.execute(
            """
            INSERT INTO run_events(
                event_id, run_id, attempt_id, sequence, source, kind, data,
                created_at, payload
            ) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9::jsonb)
            """,
            event.event_id,
            event.run_id,
            event.attempt_id,
            event.sequence,
            event.source.value,
            event.kind,
            _json(event.data),
            event.created_at,
            event.model_dump_json(),
        )
        return event

    @staticmethod
    async def _insert_audit(connection: _PgConnection, record: AuditRecord) -> None:
        await connection.execute(
            """
            INSERT INTO audit_log(
                audit_id, tenant_id, principal_id, action, outcome, run_id, attempt_id,
                agent_id, tool_name, data, created_at, payload
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12::jsonb)
            """,
            record.audit_id,
            record.tenant_id,
            record.principal_id,
            record.action,
            record.outcome,
            record.run_id,
            record.attempt_id,
            record.agent_id,
            record.tool_name,
            _json(record.data),
            record.created_at,
            record.model_dump_json(),
        )

    @staticmethod
    async def _insert_idempotency(
        connection: _PgConnection,
        run: RunRecord,
        *,
        idempotency_key: str,
        request_digest: str,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO run_idempotency(tenant_id, idempotency_key, request_digest, run_id)
            VALUES($1,$2,$3,$4)
            """,
            run.tenant_id,
            idempotency_key,
            request_digest,
            run.run_id,
        )

    @staticmethod
    async def _insert_run(connection: _PgConnection, run: RunRecord) -> None:
        await connection.execute(
            """
            INSERT INTO runs(
                run_id, conversation_id, sequence, tenant_id, principal_id, agent_id,
                agent_version, agent_fingerprint, input, metadata, status, revision,
                attempt_count, active_attempt_id, output, error, cancel_requested,
                created_at, updated_at, payload
            ) VALUES(
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,
                $18,$19,$20::jsonb
            )
            """,
            run.run_id,
            run.conversation_id,
            run.sequence,
            run.tenant_id,
            run.principal_id,
            run.agent_id,
            run.agent_version,
            run.agent_fingerprint,
            run.input,
            _json(run.metadata),
            run.status.value,
            run.revision,
            run.attempt_count,
            run.active_attempt_id,
            run.output,
            run.error,
            run.cancel_requested,
            run.created_at,
            run.updated_at,
            run.model_dump_json(),
        )

    @staticmethod
    async def _update_run(
        connection: _PgConnection,
        run: RunRecord,
        expected_revision: int,
    ) -> str | None:
        value = await connection.fetchval(
            """
            UPDATE runs
            SET conversation_id = $1, sequence = $2, tenant_id = $3, principal_id = $4,
                agent_id = $5, agent_version = $6, agent_fingerprint = $7, input = $8,
                metadata = $9::jsonb, status = $10, revision = $11, attempt_count = $12,
                active_attempt_id = $13, output = $14, error = $15,
                cancel_requested = $16, created_at = $17, updated_at = $18,
                payload = $19::jsonb
            WHERE run_id = $20 AND revision = $21
            RETURNING run_id
            """,
            run.conversation_id,
            run.sequence,
            run.tenant_id,
            run.principal_id,
            run.agent_id,
            run.agent_version,
            run.agent_fingerprint,
            run.input,
            _json(run.metadata),
            run.status.value,
            run.revision,
            run.attempt_count,
            run.active_attempt_id,
            run.output,
            run.error,
            run.cancel_requested,
            run.created_at,
            run.updated_at,
            run.model_dump_json(),
            run.run_id,
            expected_revision,
        )
        return None if value is None else str(value)

    @staticmethod
    async def _insert_attempt(connection: _PgConnection, attempt: RunAttempt) -> None:
        await connection.execute(
            """
            INSERT INTO run_attempts(
                attempt_id, run_id, number, dsh_session_id, status, dispatch_state,
                worker_id, lease_token, revision, finish_reason, output, error,
                started_at, finished_at, created_at, updated_at, payload
            ) VALUES(
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb
            )
            """,
            attempt.attempt_id,
            attempt.run_id,
            attempt.number,
            attempt.dsh_session_id,
            attempt.status.value,
            attempt.dispatch_state.value,
            attempt.worker_id,
            attempt.lease_token,
            attempt.revision,
            attempt.finish_reason,
            attempt.output,
            attempt.error,
            attempt.started_at,
            attempt.finished_at,
            attempt.created_at,
            attempt.updated_at,
            attempt.model_dump_json(),
        )

    @staticmethod
    async def _update_attempt(
        connection: _PgConnection,
        attempt: RunAttempt,
        expected_revision: int,
    ) -> str | None:
        value = await connection.fetchval(
            """
            UPDATE run_attempts
            SET run_id = $1, number = $2, dsh_session_id = $3, status = $4,
                dispatch_state = $5, worker_id = $6, lease_token = $7,
                revision = $8, finish_reason = $9, output = $10, error = $11,
                started_at = $12, finished_at = $13, created_at = $14,
                updated_at = $15, payload = $16::jsonb
            WHERE attempt_id = $17 AND revision = $18
            RETURNING attempt_id
            """,
            attempt.run_id,
            attempt.number,
            attempt.dsh_session_id,
            attempt.status.value,
            attempt.dispatch_state.value,
            attempt.worker_id,
            attempt.lease_token,
            attempt.revision,
            attempt.finish_reason,
            attempt.output,
            attempt.error,
            attempt.started_at,
            attempt.finished_at,
            attempt.created_at,
            attempt.updated_at,
            attempt.model_dump_json(),
            attempt.attempt_id,
            expected_revision,
        )
        return None if value is None else str(value)


async def _lock_idempotency(
    connection: _PgConnection,
    tenant_id: str,
    idempotency_key: str,
) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        _json([tenant_id, idempotency_key]),
    )


def _validate_revision(value: int, expected: int, kind: str) -> None:
    if value != expected + 1:
        raise ValueError(f"replacement {kind} revision must increment by one")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _model_from_payload[ModelT: BaseModel](model: type[ModelT], payload: Any) -> ModelT:
    if isinstance(payload, str | bytes | bytearray):
        return model.model_validate_json(payload)
    return model.model_validate(payload)


def _work_lease(row: asyncpg.Record) -> WorkLease:
    return WorkLease(
        resource_type=WorkResourceType(str(row["resource_type"])),
        resource_id=str(row["resource_id"]),
        worker_id=str(row["worker_id"]),
        lease_token=int(row["lease_token"]),
        expires_at=row["expires_at"],
    )


__all__ = ["PostgresControlStore"]
