"""Business control-plane records; none of these implement an Agent loop."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AttemptStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class ConversationStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    CLOSED = "closed"


class EventSource(StrEnum):
    CONTROL = "control"
    DSH = "dsh"
    TOOL = "tool"


class ConversationRecord(BaseModel):
    """Business owner and serialization boundary for one DSH Session."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    conversation_id: str = Field(default_factory=lambda: new_id("conversation"))
    tenant_id: str = Field(min_length=1)
    principal_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    agent_version: str = Field(min_length=1)
    agent_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    dsh_home_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    dsh_session_id: str = Field(min_length=1)
    status: ConversationStatus = ConversationStatus.ACTIVE
    next_sequence: int = Field(default=1, ge=1)
    revision: int = Field(default=1, ge=1)
    blocked_reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class RunRecord(BaseModel):
    """Authoritative business task state exposed to company callers."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    run_id: str = Field(default_factory=lambda: new_id("run"))
    conversation_id: str | None = None
    sequence: int | None = Field(default=None, ge=1)
    tenant_id: str = Field(min_length=1)
    principal_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    agent_version: str = Field(min_length=1)
    agent_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    input: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    status: RunStatus = RunStatus.QUEUED
    revision: int = Field(default=1, ge=1)
    attempt_count: int = Field(default=0, ge=0)
    active_attempt_id: str | None = None
    output: str | None = None
    error: str | None = None
    cancel_requested: bool = False
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class RunAttempt(BaseModel):
    """One real DSH execution attempt belonging to a stable business Run."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    attempt_id: str = Field(default_factory=lambda: new_id("attempt"))
    run_id: str
    number: int = Field(ge=1)
    dsh_session_id: str = Field(min_length=1)
    status: AttemptStatus = AttemptStatus.QUEUED
    revision: int = Field(default=1, ge=1)
    finish_reason: str | None = None
    output: str | None = None
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class RunEvent(BaseModel):
    """Ordered event projection for business observation; DSH remains execution truth."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str = Field(default_factory=lambda: new_id("event"))
    run_id: str
    attempt_id: str | None = None
    sequence: int = Field(ge=1)
    source: EventSource
    kind: str = Field(min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class AuditRecord(BaseModel):
    """Append-only governance fact, separate from ordinary runtime events."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    audit_id: str = Field(default_factory=lambda: new_id("audit"))
    tenant_id: str
    principal_id: str
    action: str = Field(min_length=1)
    outcome: str = Field(min_length=1)
    run_id: str | None = None
    attempt_id: str | None = None
    agent_id: str | None = None
    tool_name: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utc_now)


class ArtifactRecord(BaseModel):
    """Host-owned durable reference to a business-visible artifact."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    artifact_id: str = Field(default_factory=lambda: new_id("artifact"))
    run_id: str
    attempt_id: str
    name: str = Field(min_length=1)
    media_type: str = Field(min_length=1)
    location: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)
    created_at: datetime = Field(default_factory=utc_now)


TERMINAL_RUN_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
)
