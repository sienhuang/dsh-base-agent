"""Developer-facing configuration for governed Plan-ReAct Runs."""

from __future__ import annotations

import re
from dataclasses import dataclass

_WORKFLOW_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True, slots=True)
class PlanReact:
    """Enable a two-phase, approval-gated Plan-ReAct execution policy.

    The planning model submits a structured plan.  The control plane compiles
    that plan into the pinned workflow template and a later Worker executes it
    only after an explicit approval decision.
    """

    workflow_id: str = "plan-react-dag"
    workflow_version: str = "1.0.0"
    max_steps: int = 12
    max_task_chars: int = 4_000

    def __post_init__(self) -> None:
        if not _WORKFLOW_ID.fullmatch(self.workflow_id):
            raise ValueError(
                "Plan-ReAct workflow_id must contain only letters, numbers, '.', '_' or '-'"
            )
        if not self.workflow_version.strip():
            raise ValueError("Plan-ReAct workflow_version must not be blank")
        if not 1 <= self.max_steps <= 64:
            raise ValueError("Plan-ReAct max_steps must be between 1 and 64")
        if not 1 <= self.max_task_chars <= 4_000:
            raise ValueError("Plan-ReAct max_task_chars must be between 1 and 4000")


__all__ = ["PlanReact"]
