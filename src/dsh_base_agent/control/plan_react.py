"""Compile model-submitted plans into one pinned, deterministic DSH Workflow."""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, replace
from typing import Any

from dsh_base_agent.control.auth import (
    Authorizer,
    MemoryAuthorization,
    Principal,
    ToolAuthorization,
)
from dsh_base_agent.control.models import PlanRecord, PlanStep, RunRecord
from dsh_base_agent.sdk.agent import Agent
from dsh_base_agent.sdk.plan_react import PlanReact
from dsh_base_agent.sdk.tools import FunctionTool, SideEffect, tool

_WORKFLOW_SCRIPT = """\
let pending = [...args.steps]
const outputs = {}

while (pending.length > 0) {
  const ready = pending.filter(step =>
    step.dependsOn.every(dependency =>
      Object.prototype.hasOwnProperty.call(outputs, dependency)
    )
  )

  if (ready.length === 0) {
    throw new Error("approved workflow contains an unresolved dependency cycle")
  }

  const values = await parallel(ready.map(step => () => {
    const dependencyResults = Object.fromEntries(
      step.dependsOn.map(dependency => [dependency, outputs[dependency]])
    )
    return agent([
        "Execute exactly one approved Plan-ReAct step.",
        `Original request: ${args.originalInput}`,
        `Step id: ${step.stepId}`,
        `Task: ${step.task}`,
        `Approved tools: ${args.approvedTools.join(", ") || "none"}`,
        `Dependency results: ${JSON.stringify(dependencyResults)}`,
        "Do not perform work outside this step.",
      ].join("\\n"))
  }))

  ready.forEach((step, index) => {
    outputs[step.stepId] = values[index]
  })
  const completed = new Set(ready.map(step => step.stepId))
  pending = pending.filter(step => !completed.has(step.stepId))
}

return {
  workflowId: args.workflowId,
  workflowVersion: args.workflowVersion,
  steps: args.steps.map(step => ({
    stepId: step.stepId,
    result: outputs[step.stepId],
  })),
}
"""


class PlanCompilationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class PlanProposal:
    summary: str
    steps: tuple[PlanStep, ...]
    requested_tools: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CompiledWorkflow:
    workflow_id: str
    workflow_version: str
    script: str
    script_sha256: str
    args: dict[str, Any]
    args_sha256: str


class PlanSubmissionCollector:
    """Capture exactly one valid structured plan through a temporary Python Tool."""

    def __init__(self, *, config: PlanReact, available_tools: frozenset[str]) -> None:
        self.config = config
        self.available_tools = available_tools
        self._proposal: PlanProposal | None = None
        self._lock = threading.Lock()
        self.tool = self._build_tool()

    @property
    def proposal(self) -> PlanProposal | None:
        return self._proposal

    def _build_tool(self) -> FunctionTool:
        @tool(
            name="submit_plan",
            description=(
                "Submit the complete structured execution plan exactly once. "
                "This records a proposal for human approval; it does not execute the plan."
            ),
            side_effect=False,
        )
        def submit_plan(
            summary: str,
            steps: list[dict[str, Any]],
            requested_tools: list[str],
        ) -> dict[str, Any]:
            proposal = validate_proposal(
                summary=summary,
                steps=steps,
                requested_tools=requested_tools,
                config=self.config,
                available_tools=self.available_tools,
            )
            with self._lock:
                if self._proposal is not None:
                    raise PlanCompilationError("submit_plan may be called only once")
                self._proposal = proposal
            return {
                "accepted": True,
                "steps": len(proposal.steps),
                "requested_tools": list(proposal.requested_tools),
                "message": "The plan is recorded for human approval. Do not execute it now.",
            }

        return submit_plan


@dataclass(frozen=True, slots=True)
class PlanningAuthorizer:
    """Trust only the control-owned plan submission Tool; delegate everything else."""

    delegate: Authorizer

    async def authorize_run(self, principal: Principal, agent: Agent) -> None:
        await self.delegate.authorize_run(principal, agent)

    async def authorize_tool(self, request: ToolAuthorization) -> None:
        if request.tool.spec.name == "submit_plan":
            if request.tool.permissions or request.tool.side_effect is not SideEffect.READ_ONLY:
                raise PlanCompilationError("internal submit_plan Tool has unsafe metadata")
            return
        await self.delegate.authorize_tool(request)

    async def authorize_memory(self, request: MemoryAuthorization) -> None:
        await self.delegate.authorize_memory(request)


def planning_agent(agent: Agent) -> tuple[Agent, PlanSubmissionCollector]:
    config = _require_config(agent)
    available = frozenset(item.spec.name for item in agent.tools)
    collector = PlanSubmissionCollector(config=config, available_tools=available)
    read_only = tuple(item for item in agent.tools if item.side_effect is SideEffect.READ_ONLY)
    catalog = [
        {
            "name": item.spec.name,
            "description": item.spec.description,
            "side_effect": item.side_effect.value,
        }
        for item in agent.tools
    ]
    planning_prompt = f"""{agent.prompt.rstrip()}

You are now in the governed PLANNING phase.
- Investigate only with the read-only tools currently available.
- Do not execute the requested business operation.
- Produce between 1 and {config.max_steps} concrete steps.
- Dependencies must reference earlier or independent step ids and must form a DAG.
- requested_tools may contain only names from this complete catalog:
{json.dumps(catalog, ensure_ascii=False, separators=(",", ":"))}
- Call submit_plan exactly once. After it succeeds, explain that human approval is pending.
"""
    return (
        replace(
            agent,
            prompt=planning_prompt,
            tools=(*read_only, collector.tool),
            plan_react=None,
        ),
        collector,
    )


def execution_agent(agent: Agent, plan: PlanRecord) -> Agent:
    selected = frozenset(plan.requested_tools)
    tools = tuple(item for item in agent.tools if item.spec.name in selected)
    if frozenset(item.spec.name for item in tools) != selected:
        raise PlanCompilationError("approved plan refers to a Tool absent from the Agent")
    return replace(
        agent,
        prompt=(
            f"{agent.prompt.rstrip()}\n\n"
            "You are executing a human-approved Plan-ReAct Workflow. "
            f"Plan id: {plan.plan_id}. Plan sha256: {plan.plan_sha256}. "
            "Use only the exposed approved tools and stay inside each assigned step."
        ),
        tools=tools,
    )


def compile_plan(
    *,
    run: RunRecord,
    agent: Agent,
    proposal: PlanProposal,
) -> tuple[PlanRecord, CompiledWorkflow]:
    config = _require_config(agent)
    # Revalidate the captured proposal against the immutable registered Agent.
    proposal = validate_proposal(
        summary=proposal.summary,
        steps=[item.model_dump(mode="json") for item in proposal.steps],
        requested_tools=list(proposal.requested_tools),
        config=config,
        available_tools=frozenset(item.spec.name for item in agent.tools),
    )
    args: dict[str, Any] = {
        "workflowId": config.workflow_id,
        "workflowVersion": config.workflow_version,
        "originalInput": run.input,
        "approvedTools": list(proposal.requested_tools),
        "steps": [
            {
                "stepId": item.step_id,
                "task": item.task,
                "dependsOn": list(item.depends_on),
            }
            for item in proposal.steps
        ],
    }
    script_sha256 = _text_digest(_WORKFLOW_SCRIPT)
    args_sha256 = _digest(args)
    semantic = {
        "run_id": run.run_id,
        "summary": proposal.summary,
        "steps": [item.model_dump(mode="json") for item in proposal.steps],
        "requested_tools": list(proposal.requested_tools),
        "workflow_id": config.workflow_id,
        "workflow_version": config.workflow_version,
        "workflow_script_sha256": script_sha256,
        "workflow_args_sha256": args_sha256,
    }
    plan = PlanRecord(
        run_id=run.run_id,
        summary=proposal.summary,
        steps=proposal.steps,
        requested_tools=proposal.requested_tools,
        workflow_id=config.workflow_id,
        workflow_version=config.workflow_version,
        workflow_script_sha256=script_sha256,
        workflow_args=args,
        workflow_args_sha256=args_sha256,
        plan_sha256=_digest(semantic),
    )
    return plan, compiled_workflow(plan)


def compiled_workflow(plan: PlanRecord) -> CompiledWorkflow:
    script_sha256 = _text_digest(_WORKFLOW_SCRIPT)
    args_sha256 = _digest(plan.workflow_args)
    if script_sha256 != plan.workflow_script_sha256:
        raise PlanCompilationError("approved Workflow template hash no longer matches")
    if args_sha256 != plan.workflow_args_sha256:
        raise PlanCompilationError("approved Workflow arguments hash no longer matches")
    semantic = {
        "run_id": plan.run_id,
        "summary": plan.summary,
        "steps": [item.model_dump(mode="json") for item in plan.steps],
        "requested_tools": list(plan.requested_tools),
        "workflow_id": plan.workflow_id,
        "workflow_version": plan.workflow_version,
        "workflow_script_sha256": plan.workflow_script_sha256,
        "workflow_args_sha256": plan.workflow_args_sha256,
    }
    if _digest(semantic) != plan.plan_sha256:
        raise PlanCompilationError("approved Plan hash no longer matches")
    return CompiledWorkflow(
        workflow_id=plan.workflow_id,
        workflow_version=plan.workflow_version,
        script=_WORKFLOW_SCRIPT,
        script_sha256=script_sha256,
        args=dict(plan.workflow_args),
        args_sha256=args_sha256,
    )


def validate_proposal(
    *,
    summary: str,
    steps: list[dict[str, Any]],
    requested_tools: list[str],
    config: PlanReact,
    available_tools: frozenset[str],
) -> PlanProposal:
    normalized_summary = summary.strip()
    if not normalized_summary:
        raise PlanCompilationError("plan summary must not be blank")
    if len(normalized_summary) > 8_000:
        raise PlanCompilationError("plan summary exceeds 8000 characters")
    if not 1 <= len(steps) <= config.max_steps:
        raise PlanCompilationError(f"plan must contain between 1 and {config.max_steps} steps")
    try:
        normalized_steps = tuple(PlanStep.model_validate(item) for item in steps)
    except ValueError as exc:
        raise PlanCompilationError(str(exc)) from exc
    ids = tuple(item.step_id for item in normalized_steps)
    if len(ids) != len(set(ids)):
        raise PlanCompilationError("plan step ids must be unique")
    known = frozenset(ids)
    for item in normalized_steps:
        if len(item.task) > config.max_task_chars:
            raise PlanCompilationError(
                f"plan step '{item.step_id}' exceeds {config.max_task_chars} characters"
            )
        if len(item.depends_on) != len(set(item.depends_on)):
            raise PlanCompilationError(f"plan step '{item.step_id}' has duplicate dependencies")
        if item.step_id in item.depends_on:
            raise PlanCompilationError(f"plan step '{item.step_id}' depends on itself")
        missing = set(item.depends_on) - known
        if missing:
            raise PlanCompilationError(
                f"plan step '{item.step_id}' has unknown dependencies: {', '.join(sorted(missing))}"
            )
    _assert_acyclic(normalized_steps)
    normalized_tools = tuple(dict.fromkeys(item.strip() for item in requested_tools))
    if any(not item for item in normalized_tools):
        raise PlanCompilationError("requested Tool names must not be blank")
    missing_tools = set(normalized_tools) - available_tools
    if missing_tools:
        raise PlanCompilationError(
            f"plan requested unknown Tools: {', '.join(sorted(missing_tools))}"
        )
    return PlanProposal(
        summary=normalized_summary,
        steps=normalized_steps,
        requested_tools=normalized_tools,
    )


def _assert_acyclic(steps: tuple[PlanStep, ...]) -> None:
    dependencies = {item.step_id: set(item.depends_on) for item in steps}
    resolved: set[str] = set()
    while len(resolved) < len(steps):
        ready = {
            step_id
            for step_id, required in dependencies.items()
            if step_id not in resolved and required <= resolved
        }
        if not ready:
            raise PlanCompilationError("plan dependencies must form an acyclic graph")
        resolved.update(ready)


def _require_config(agent: Agent) -> PlanReact:
    if agent.plan_react is None:
        raise PlanCompilationError("Agent does not enable Plan-ReAct")
    return agent.plan_react


def _text_digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _digest(value: Any) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return _text_digest(canonical)


__all__ = [
    "CompiledWorkflow",
    "PlanCompilationError",
    "PlanProposal",
    "PlanSubmissionCollector",
    "PlanningAuthorizer",
    "compile_plan",
    "compiled_workflow",
    "execution_agent",
    "planning_agent",
    "validate_proposal",
]
