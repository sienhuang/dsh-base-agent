from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from dsh_base_agent import (
    Agent,
    ControlPlane,
    PlanReact,
    Principal,
    RunPhase,
    RunStatus,
    RuntimeConfig,
    SideEffect,
    ToolContext,
    create_app,
    tool,
)
from dsh_base_agent.adapters.dsh.runtime import (
    DshEventHandler,
    DshNotificationHandler,
    DshRunResult,
    DshRuntime,
    DshWorkflowRequest,
    DshWorkflowResult,
)
from dsh_base_agent.control import PlanDecisionConflict
from dsh_base_agent.store import SqliteControlStore


@tool
def inspect_order(order_id: str) -> dict[str, str]:
    return {"order_id": order_id}


@tool(side_effect=SideEffect.IDEMPOTENT)
def update_order(order_id: str) -> dict[str, str]:
    return {"order_id": order_id}


class PlanRuntime:
    def __init__(self, agent: Agent, calls: list[tuple[str, Any]]) -> None:
        self.agent = agent
        self.calls = calls

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        del input, on_event
        self.calls.append(
            ("planning_agent_tools", tuple(item.spec.name for item in self.agent.tools))
        )
        submit_plan = next(item for item in self.agent.tools if item.spec.name == "submit_plan")
        await submit_plan.invoke(
            {
                "summary": "Inspect the order, then apply the approved update.",
                "steps": [
                    {
                        "step_id": "inspect",
                        "task": "Inspect order-001.",
                        "depends_on": [],
                    },
                    {
                        "step_id": "update",
                        "task": "Update order-001 using the inspected state.",
                        "depends_on": ["inspect"],
                    },
                ],
                "requested_tools": ["update_order"],
            },
            ToolContext(
                tenant_id="tenant-a",
                principal_id="alice",
                agent_id=self.agent.name,
                run_id="run-test",
                attempt_id="attempt-test",
                tool_name="submit_plan",
                tool_call_id="call-test",
                operation_id="operation-test",
            ),
        )
        if on_notification is not None:
            await on_notification("status.changed", {"status": "idle"})
        return DshRunResult(
            session_id=session_id,
            final_response="The plan is waiting for approval.",
            finish_reason="completed",
        )

    async def execute_workflow(
        self,
        request: DshWorkflowRequest,
        *,
        session_id: str,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshWorkflowResult:
        self.calls.append(
            ("execution_agent_tools", tuple(item.spec.name for item in self.agent.tools))
        )
        self.calls.append(("workflow", request))
        if on_notification is not None:
            await on_notification("workflow.started", {"workflowId": request.workflow_id})
        return DshWorkflowResult(
            session_id=session_id,
            workflow_run_id="workflow-run-1",
            stop_reason="completed",
            agents_started=2,
            value={"steps": [{"stepId": "inspect"}, {"stepId": "update"}]},
        )

    async def close(self) -> None:
        pass


class PlanRuntimeFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
        memory_context_url: str | None = None,
        memory_context_token: str | None = None,
    ) -> DshRuntime:
        del (
            workspace,
            dsh_home,
            attempt_id,
            tool_gateway_url,
            memory_context_url,
            memory_context_token,
        )
        return PlanRuntime(agent, self.calls)


def plan_agent() -> Agent:
    return Agent(
        name="governed-orders",
        prompt="Handle orders.",
        tools=(inspect_order, update_order),
        plan_react=PlanReact(max_steps=4),
    )


def test_plan_react_reserves_internal_submit_plan_tool_name() -> None:
    @tool(name="submit_plan")
    def conflicting_tool(value: str) -> str:
        return value

    with pytest.raises(ValueError, match="reserves"):
        Agent(
            name="conflicting-planner",
            prompt="Plan first.",
            tools=(conflicting_tool,),
            plan_react=PlanReact(),
        )


async def test_plan_react_requires_exact_approval_then_executes_pinned_workflow(
    tmp_path: Path,
) -> None:
    factory = PlanRuntimeFactory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    control.register(plan_agent())
    principal = Principal("tenant-a", "alice")
    await control.start()
    try:
        submitted = await control.submit(
            principal=principal,
            agent_id="governed-orders",
            input="Inspect and update order-001.",
        )
        waiting = await control.wait(submitted.run_id)
        plan = await control.get_plan(principal, submitted.run_id)

        assert waiting.status is RunStatus.WAITING
        assert waiting.phase is RunPhase.WAITING_APPROVAL
        assert waiting.plan_id == plan.plan_id
        assert plan.status.value == "pending"
        assert plan.workflow_script_sha256
        assert plan.workflow_args_sha256

        with pytest.raises(PlanDecisionConflict, match="plan_sha256"):
            await control.approve_plan(
                principal,
                submitted.run_id,
                plan_id=plan.plan_id,
                plan_revision=plan.revision,
                plan_sha256="0" * 64,
            )

        decision = await control.approve_plan(
            principal,
            submitted.run_id,
            plan_id=plan.plan_id,
            plan_revision=plan.revision,
            plan_sha256=plan.plan_sha256,
            comment="approved in test",
        )
        assert decision.run.status is RunStatus.QUEUED
        assert decision.run.phase is RunPhase.EXECUTING
        assert decision.plan.status.value == "approved"

        completed = await control.wait(submitted.run_id)
        attempts = (await control.view(principal, submitted.run_id)).attempts
        events = await control.events(principal, submitted.run_id)

        assert completed.status is RunStatus.SUCCEEDED
        assert completed.phase is RunPhase.COMPLETED
        assert [item.phase for item in attempts] == [RunPhase.PLANNING, RunPhase.EXECUTING]
        assert json.loads(completed.output or "null")["steps"][1]["stepId"] == "update"
        assert ("planning_agent_tools", ("inspect_order", "submit_plan")) in factory.calls
        assert ("execution_agent_tools", ("update_order",)) in factory.calls
        workflow = next(value for kind, value in factory.calls if kind == "workflow")
        assert isinstance(workflow, DshWorkflowRequest)
        assert workflow.script_sha256 == plan.workflow_script_sha256
        assert workflow.args == plan.workflow_args
        assert workflow.args_sha256 == plan.workflow_args_sha256
        assert {item.kind for item in events} >= {
            "plan.proposed",
            "plan.approved",
            "workflow.started",
            "workflow.completed",
            "run.succeeded",
        }
    finally:
        await control.close()


async def test_rejected_plan_never_executes(tmp_path: Path) -> None:
    factory = PlanRuntimeFactory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    control.register(plan_agent())
    principal = Principal("tenant-a", "alice")
    await control.start()
    try:
        submitted = await control.submit(
            principal=principal,
            agent_id="governed-orders",
            input="Inspect and update order-001.",
        )
        await control.wait(submitted.run_id)
        plan = await control.get_plan(principal, submitted.run_id)
        rejected = await control.reject_plan(
            principal,
            submitted.run_id,
            plan_id=plan.plan_id,
            plan_revision=plan.revision,
            plan_sha256=plan.plan_sha256,
            comment="not approved",
        )

        assert rejected.run.status is RunStatus.CANCELLED
        assert rejected.plan.status.value == "rejected"
        assert all(kind != "workflow" for kind, _ in factory.calls)
    finally:
        await control.close()


async def test_plan_react_http_contract(tmp_path: Path) -> None:
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=PlanRuntimeFactory(),
    )
    control.register(plan_agent())
    await control.start()
    app = create_app(control, close_on_shutdown=False)
    headers = {"X-Tenant-ID": "tenant-a", "X-Principal-ID": "alice"}
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/v1/runs",
                headers=headers,
                json={"agent_id": "governed-orders", "input": "Update order-001."},
            )
            assert response.status_code == 202
            run_id = response.json()["run_id"]
            await control.wait(run_id)

            response = await client.get(f"/v1/runs/{run_id}/plan", headers=headers)
            assert response.status_code == 200
            plan = response.json()

            response = await client.post(
                f"/v1/runs/{run_id}/approve",
                headers=headers,
                json={
                    "plan_id": plan["plan_id"],
                    "plan_revision": plan["revision"],
                    "plan_sha256": plan["plan_sha256"],
                },
            )
            assert response.status_code == 202
            assert response.json()["run"]["phase"] == "executing"
            assert response.json()["plan"]["status"] == "approved"

            conflict = await client.post(
                f"/v1/runs/{run_id}/approve",
                headers=headers,
                json={
                    "plan_id": plan["plan_id"],
                    "plan_revision": plan["revision"],
                    "plan_sha256": plan["plan_sha256"],
                },
            )
            assert conflict.status_code == 409
            assert (await control.wait(run_id)).status is RunStatus.SUCCEEDED
    finally:
        await control.close()
