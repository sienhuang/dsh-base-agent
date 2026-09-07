from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import deepseek_harness
import pytest
from dsh_base_agent import (
    AuthorizationDenied,
    ControlStoreConfig,
    MemoryAuthorization,
    MemorySearchRequest,
    PostgresControlStore,
    Principal,
    RuntimeConfig,
    ToolAuthorization,
    ToolContext,
)

from company_agent.app import build_control, create_starter_app
from company_agent.authorization import StarterAuthorizer
from company_agent.context import render_static_context, static_context_sections
from company_agent.definition import build_agent
from company_agent.memory import search_user_memory
from company_agent.tools import get_request_context, query_order
from company_agent.worker import build_worker


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
    assert [item.spec.name for item in agent.tools] == [
        "get_request_context",
        "query_order",
        "generate_large_report",
    ]
    assert [provider.name for provider in agent.memory_providers] == ["user-memory"]
    assert "business-boundary" in agent.prompt
    assert "response-contract" in agent.prompt
    assert agent.permissions == frozenset({"orders:read", "context:read", "memory:read"})


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
    assert control.auto_execute is True
    assert tuple(control.agents) == ("iris-assistant-1",)
    assert "/v1/runs" in {route.path for route in app.routes}


def test_postgres_switches_api_to_submission_only_and_builds_worker(tmp_path) -> None:
    runtime = RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh-home")
    store_config = ControlStoreConfig(database_url="postgresql://agent:secret@db/agent")

    control = build_control(
        project_root=tmp_path,
        runtime=runtime,
        store_config=store_config,
    )

    assert control.auto_execute is False
    assert isinstance(control.store, PostgresControlStore)

    (tmp_path / ".env").write_text(
        "\n".join(
            (
                "DSH_MODEL=test",
                "DSH_BASE_AGENT_DATABASE_URL=postgresql://agent:secret@db/agent",
            )
        ),
        encoding="utf-8",
    )
    worker = build_worker(project_root=tmp_path)

    assert worker.control.auto_execute is False
    assert isinstance(worker.control.store, PostgresControlStore)


def test_worker_runtime_registers_the_control_workspace_skill_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            (
                "DSH_MODEL=test",
                "DSH_BASE_AGENT_DATABASE_URL=postgresql://agent:secret@db/agent",
            )
        ),
        encoding="utf-8",
    )
    skill_root = tmp_path / "workspace" / ".dsh" / "skills"
    skill_root.mkdir(parents=True)
    captured: dict[str, Any] = {}

    class FakeHarness:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(deepseek_harness, "DeepSeekHarness", FakeHarness)
    worker = build_worker(project_root=tmp_path)
    worker.control.runtime_factory.create(
        build_agent(),
        workspace=worker.control.workspace,
        dsh_home=tmp_path / "dsh-home",
        attempt_id="attempt-worker-1",
        tool_gateway_url="http://127.0.0.1:1234/mcp",
        memory_context_url="http://127.0.0.1:1235/memory/context",
        memory_context_token="test-token",
    )

    patch_paths = cast(tuple[str, ...], captured["patches"])
    patch = json.loads(Path(patch_paths[0]).read_text(encoding="utf-8"))
    assert {
        "id": "skill-filesystem",
        "config": {"customSkillDirs": [str(skill_root.resolve())]},
    } in patch


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


async def test_memory_provider_uses_trusted_tenant_and_principal_context() -> None:
    items = await search_user_memory.search(
        MemorySearchRequest(
            tenant_id="demo-tenant",
            principal_id="demo-user",
            agent_id="iris-assistant-1",
            conversation_id="conversation-1",
            run_id="run-1",
            attempt_id="attempt-1",
            dsh_session_id="session-1",
            query="查询订单",
        )
    )

    assert len(items) == 1
    assert "demo-user" in items[0].content


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

    with pytest.raises(AuthorizationDenied):
        await authorizer.authorize_memory(
            MemoryAuthorization(
                principal=principal,
                agent=agent,
                conversation_id=None,
                run_id="run-1",
                attempt_id="attempt-1",
                provider=search_user_memory,
            )
        )
