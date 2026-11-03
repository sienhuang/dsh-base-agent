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


class RunPhase(StrEnum):
    DIRECT = "direct"
    PLANNING = "planning"
    WAITING_APPROVAL = "waiting_approval"
    EXECUTING = "executing"
    COMPLETED = "completed"


class AttemptStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class DispatchState(StrEnum):
    NOT_SENT = "not_sent"
    DISPATCHING = "dispatching"
    ACCEPTED = "accepted"
    SETTLED = "settled"
    UNKNOWN = "unknown"


class WorkResourceType(StrEnum):
    RUN = "run"
    CONVERSATION = "conversation"


class ConversationStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    CLOSED = "closed"


class EventSource(StrEnum):
    CONTROL = "control"
    DSH = "dsh"
    TOOL = "tool"


class PlanStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class PlanApprovalDecision(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"


class RequiredActionType(StrEnum):
    USER_INPUT = "user_input"
    PLAN_APPROVAL = "plan_approval"


class RequiredActionSource(StrEnum):
    MODEL = "model"
    POLICY = "policy"
    TOOL = "tool"


class RequiredActionStatus(StrEnum):
    PENDING = "pending"
    ANSWERED = "answered"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


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
    phase: RunPhase = RunPhase.DIRECT
    plan_id: str | None = None
    status: RunStatus = RunStatus.QUEUED
    revision: int = Field(default=1, ge=1)
    attempt_count: int = Field(default=0, ge=0)
    active_attempt_id: str | None = None
    required_action_id: str | None = None
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
    phase: RunPhase = RunPhase.DIRECT
    status: AttemptStatus = AttemptStatus.QUEUED
    dispatch_state: DispatchState = DispatchState.NOT_SENT
    worker_id: str | None = None
    lease_token: int | None = Field(default=None, ge=1)
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


class RequiredActionRecord(BaseModel):
    """Durable action that must be resolved before a business Run may continue."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    action_id: str = Field(default_factory=lambda: new_id("action"))
    run_id: str
    attempt_id: str
    conversation_id: str | None = None
    tenant_id: str = Field(min_length=1)
    principal_id: str = Field(min_length=1)
    type: RequiredActionType
    source: RequiredActionSource
    status: RequiredActionStatus = RequiredActionStatus.PENDING
    payload: dict[str, Any]
    response: dict[str, Any] | None = None
    revision: int = Field(default=1, ge=1)
    expires_at: datetime | None = None
    answered_by: str | None = None
    answered_at: datetime | None = None
    consumed_by_attempt_id: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class PlanApprovalResponse(BaseModel):
    """Exact human decision submitted for a pending Plan RequiredAction."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    decision: PlanApprovalDecision
    plan_id: str = Field(min_length=1)
    plan_revision: int = Field(ge=1)
    plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    comment: str | None = Field(default=None, max_length=4_000)


class PlanStep(BaseModel):
    """One approved unit in a compiled Workflow dependency graph."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    step_id: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    task: str = Field(min_length=1, max_length=4_000)
    depends_on: tuple[str, ...] = ()


class PlanRecord(BaseModel):
    """Durable plan plus the exact compiled Workflow identity approved for execution."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    plan_id: str = Field(default_factory=lambda: new_id("plan"))
    run_id: str
    status: PlanStatus = PlanStatus.PENDING
    revision: int = Field(default=1, ge=1)
    summary: str = Field(min_length=1, max_length=8_000)
    steps: tuple[PlanStep, ...] = Field(min_length=1)
    requested_tools: tuple[str, ...] = ()
    workflow_id: str = Field(min_length=1)
    workflow_version: str = Field(min_length=1)
    workflow_script_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    workflow_args: dict[str, Any]
    workflow_args_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    decided_by: str | None = None
    decision_comment: str | None = None
    decided_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class WorkLease(BaseModel):
    """A PostgreSQL-owned fencing lease for one executable resource."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    resource_type: WorkResourceType
    resource_id: str = Field(min_length=1)
    worker_id: str = Field(min_length=1)
    lease_token: int = Field(ge=1)
    expires_at: datetime


TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED})
