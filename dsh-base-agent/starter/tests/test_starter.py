from __future__ import annotations

from pathlib import Path

import pytest
from dsh_base_agent import (
    AuthorizationDenied,
    Principal,
    RuntimeConfig,
    ToolAuthorization,
    ToolContext,
)

from company_agent.app import build_control, create_starter_app
from company_agent.authorization import StarterAuthorizer
from company_agent.context import render_static_context, static_context_sections
from company_agent.definition import build_agent
from company_agent.tools import get_request_context, query_order


def _tool_context(*, tenant_id: str = "demo-tenant") -> ToolContext:
    return ToolContext(
        tenant_id=tenant_id,
        principal_id="demo-user",
        agent_id="iris-assistant-1",
        run_id="run-1",
        attempt_id="attempt-1",
        tool_name="query_order",
        tool_call_id="call-1",
        operation_id="operation-1",
    )


def test_agent_wires_tools_skill_and_static_context() -> None:
    agent = build_agent()

    assert agent.skills == ("order-support",)
    assert [item.spec.name for item in agent.tools] == ["get_request_context", "query_order"]
    assert "business-boundary" in agent.prompt
    assert "response-contract" in agent.prompt
    assert agent.permissions == frozenset({"orders:read", "context:read"})


def test_skill_bundle_uses_dsh_project_convention() -> None:
    starter_root = Path(__file__).resolve().parents[1]
    skill = starter_root / "workspace" / ".dsh" / "skills" / "order-support" / "SKILL.md"

    assert skill.is_file()
    assert "name: order-support" in skill.read_text(encoding="utf-8")


def test_static_context_rendering_is_deterministic() -> None:
    sections = static_context_sections()

    assert render_static_context(sections) == render_static_context(sections)
    assert "DSH_API_KEY" not in render_static_context(sections)


def test_application_factory_registers_agent_and_uses_starter_workspace(tmp_path) -> None:
    runtime = RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh-home")

    control = build_control(project_root=tmp_path, runtime=runtime)
    app = create_starter_app(project_root=tmp_path, runtime=runtime)

    assert control.workspace == (tmp_path / "workspace").resolve()
    assert tuple(control.agents) == ("iris-assistant-1",)
    assert "/v1/runs" in {route.path for route in app.routes}


async def test_tools_use_tenant_and_principal_context() -> None:
    context = _tool_context()

    order = await query_order.invoke({"order_id": "order-001"}, context)
    request_context = await get_request_context.invoke({}, context)

    assert order == {
        "found": True,
        "order_id": "order-001",
        "status": "paid",
        "shipping_status": "waiting_for_pickup",
    }
    assert request_context["principal_id"] == "demo-user"
    assert request_context["answer_style"] == "concise"


async def test_starter_authorizer_denies_unknown_tenant() -> None:
    agent = build_agent()
    authorizer = StarterAuthorizer()
    principal = Principal("another-tenant", "demo-user")

    with pytest.raises(AuthorizationDenied):
        await authorizer.authorize_run(principal, agent)

    with pytest.raises(AuthorizationDenied):
        await authorizer.authorize_tool(
            ToolAuthorization(
                principal=principal,
                agent=agent,
                run_id="run-1",
                attempt_id="attempt-1",
                tool=query_order,
                arguments={"order_id": "order-001"},
            )
        )
