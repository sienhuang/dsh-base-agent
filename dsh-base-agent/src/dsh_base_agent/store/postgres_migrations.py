"""Versioned PostgreSQL schema migrations for the control store."""

from __future__ import annotations

POSTGRES_MIGRATIONS: tuple[tuple[int, str, str], ...] = (
    (
        1,
        "initial_control_store",
        """
        CREATE TABLE conversations (
            conversation_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            agent_version TEXT NOT NULL,
            agent_fingerprint TEXT NOT NULL,
            dsh_home_key TEXT NOT NULL,
            dsh_session_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('active', 'blocked', 'closed')),
            next_sequence INTEGER NOT NULL CHECK (next_sequence >= 1),
            revision INTEGER NOT NULL CHECK (revision >= 1),
            blocked_reason TEXT,
            metadata JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL,
            UNIQUE(dsh_home_key, dsh_session_id)
        );
        CREATE INDEX ix_conversations_owner_status
            ON conversations(tenant_id, principal_id, status);
        CREATE INDEX ix_conversations_session
            ON conversations(dsh_session_id);

        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            conversation_id TEXT REFERENCES conversations(conversation_id),
            sequence INTEGER,
            tenant_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            agent_version TEXT NOT NULL,
            agent_fingerprint TEXT NOT NULL,
            input TEXT NOT NULL,
            metadata JSONB NOT NULL,
            status TEXT NOT NULL
                CHECK (status IN ('queued', 'running', 'waiting', 'succeeded', 'failed',
                                  'cancelled')),
            revision INTEGER NOT NULL CHECK (revision >= 1),
            attempt_count INTEGER NOT NULL CHECK (attempt_count >= 0),
            active_attempt_id TEXT,
            output TEXT,
            error TEXT,
            cancel_requested BOOLEAN NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL,
            CHECK (
                (conversation_id IS NULL AND sequence IS NULL)
                OR (conversation_id IS NOT NULL AND sequence IS NOT NULL AND sequence >= 1)
            ),
            UNIQUE(conversation_id, sequence)
        );
        CREATE INDEX ix_runs_tenant_status ON runs(tenant_id, status);
        CREATE INDEX ix_runs_conversation_status
            ON runs(conversation_id, status, sequence);
        CREATE UNIQUE INDEX uq_runs_conversation_active
            ON runs(conversation_id)
            WHERE conversation_id IS NOT NULL AND status IN ('running', 'waiting');

        CREATE TABLE run_idempotency (
            tenant_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            request_digest TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(tenant_id, idempotency_key)
        );

        CREATE TABLE run_attempts (
            attempt_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            number INTEGER NOT NULL CHECK (number >= 1),
            dsh_session_id TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK (status IN ('queued', 'running', 'waiting', 'succeeded', 'failed',
                                  'cancelled', 'interrupted')),
            dispatch_state TEXT NOT NULL DEFAULT 'not_sent'
                CHECK (dispatch_state IN ('not_sent', 'dispatching', 'accepted', 'settled',
                                          'unknown')),
            worker_id TEXT,
            lease_token BIGINT,
            revision INTEGER NOT NULL CHECK (revision >= 1),
            finish_reason TEXT,
            output TEXT,
            error TEXT,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL,
            UNIQUE(run_id, number)
        );
        CREATE INDEX ix_run_attempts_session ON run_attempts(dsh_session_id);
        CREATE INDEX ix_run_attempts_status ON run_attempts(status);

        ALTER TABLE runs
            ADD CONSTRAINT fk_runs_active_attempt
            FOREIGN KEY(active_attempt_id) REFERENCES run_attempts(attempt_id)
            DEFERRABLE INITIALLY DEFERRED;

        CREATE TABLE run_events (
            event_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            attempt_id TEXT REFERENCES run_attempts(attempt_id),
            sequence INTEGER NOT NULL CHECK (sequence >= 1),
            source TEXT NOT NULL CHECK (source IN ('control', 'dsh', 'tool')),
            kind TEXT NOT NULL,
            data JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL,
            UNIQUE(run_id, sequence)
        );

        CREATE TABLE audit_log (
            audit_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            action TEXT NOT NULL,
            outcome TEXT NOT NULL,
            run_id TEXT,
            attempt_id TEXT,
            agent_id TEXT,
            tool_name TEXT,
            data JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        );
        CREATE INDEX ix_audit_run ON audit_log(run_id, created_at, audit_id);
        CREATE INDEX ix_audit_owner ON audit_log(tenant_id, principal_id, created_at);

        CREATE TABLE artifacts (
            artifact_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES runs(run_id),
            attempt_id TEXT NOT NULL REFERENCES run_attempts(attempt_id),
            name TEXT NOT NULL,
            media_type TEXT NOT NULL,
            location TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
            created_at TIMESTAMPTZ NOT NULL,
            payload JSONB NOT NULL
        );
        CREATE INDEX ix_artifacts_run ON artifacts(run_id, created_at, artifact_id);

        CREATE TABLE worker_leases (
            resource_type TEXT NOT NULL,
            resource_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            lease_token BIGINT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY(resource_type, resource_id),
            UNIQUE(resource_type, resource_id, lease_token)
        );
        CREATE INDEX ix_worker_leases_expiry ON worker_leases(expires_at);

        CREATE TABLE outbox (
            outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'publishing', 'published', 'failed')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            available_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            locked_by TEXT,
            locked_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            published_at TIMESTAMPTZ
        );
        CREATE INDEX ix_outbox_publishable ON outbox(status, available_at, outbox_id);
        """,
    ),
)


__all__ = ["POSTGRES_MIGRATIONS"]
