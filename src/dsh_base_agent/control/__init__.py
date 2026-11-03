"""Business Run control plane, ownership, and governance records."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dsh_base_agent.control.auth import (
    AuthorizationDenied,
    Authorizer,
    MemoryAuthorization,
    Principal,
    ReadOnlyByDefaultAuthorizer,
    ToolAuthorization,
)
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
    PlanStep,
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

if TYPE_CHECKING:
    from dsh_base_agent.control.plane import (
        AgentNotFoundError,
        ControlPlane,
        ConversationAccessDenied,
        PlanDecisionConflict,
        PlanDecisionView,
        RunAccessDenied,
        RunResumeUnsupported,
        RunTransitionError,
        RunView,
    )

_PLANE_EXPORTS = frozenset(
    {
        "AgentNotFoundError",
        "ConversationAccessDenied",
        "ControlPlane",
        "PlanDecisionConflict",
        "PlanDecisionView",
        "RunAccessDenied",
        "RunResumeUnsupported",
        "RunTransitionError",
        "RunView",
    }
)


def __getattr__(name: str) -> Any:
    if name in _PLANE_EXPORTS:
        from dsh_base_agent.control import plane

        return getattr(plane, name)
    raise AttributeError(name)


__all__ = [
    "AgentNotFoundError",
    "ArtifactRecord",
    "AttemptStatus",
    "AuditRecord",
    "AuthorizationDenied",
    "Authorizer",
    "ConversationAccessDenied",
    "ConversationRecord",
    "ConversationStatus",
    "ControlPlane",
    "DispatchState",
    "EventSource",
    "MemoryAuthorization",
    "PlanApprovalDecision",
    "PlanApprovalResponse",
    "PlanDecisionConflict",
    "PlanDecisionView",
    "PlanRecord",
    "PlanStatus",
    "PlanStep",
    "Principal",
    "RequiredActionRecord",
    "RequiredActionSource",
    "RequiredActionStatus",
    "RequiredActionType",
    "ReadOnlyByDefaultAuthorizer",
    "RunAccessDenied",
    "RunAttempt",
    "RunEvent",
    "RunRecord",
    "RunPhase",
    "RunResumeUnsupported",
    "RunStatus",
    "RunTransitionError",
    "RunView",
    "TERMINAL_RUN_STATUSES",
    "ToolAuthorization",
    "WorkLease",
    "WorkResourceType",
    "new_id",
    "utc_now",
]
