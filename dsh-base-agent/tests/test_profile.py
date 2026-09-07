from __future__ import annotations

import json
import stat

from dsh_base_agent import Agent, MemorySearchRequest, memory_provider, tool
from dsh_base_agent.adapters.dsh.profile import DshProfileCompiler


def test_profile_uses_dsh_sdk_and_loopback_mcp(tmp_path) -> None:
    @tool
    def query(value: str) -> str:
        return value

    agent = Agent(
        name="orders",
        prompt="Handle orders.",
        tools=(query,),
        skills=("order-support",),
    )
    (path,) = DshProfileCompiler().compile(
        agent,
        workspace=tmp_path / "workspace",
        dsh_home=tmp_path,
        attempt_id="attempt-1",
        tool_gateway_url="http://127.0.0.1:1234/mcp",
    )
    patch = json.loads(path.read_text(encoding="utf-8"))
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert patch[0] == {"id": "system-prompt", "config": {"persona": "Handle orders."}}
    assert {"id": "tool-bash", "disabled": True} in patch
    assert {"id": "tool-skill", "disabled": True} not in patch
    assert {
        "id": "skill-filesystem",
        "config": {
            "customSkillDirs": [str((tmp_path / "workspace" / ".dsh" / "skills").resolve())]
        },
    } in patch
    mcp = next(item for item in patch if "insert" in item)
    assert mcp["insert"][0]["name"] == "@deepseek-ai/dsh-mcp-client"


def test_profile_disables_skill_tool_when_agent_selects_no_skills(tmp_path) -> None:
    agent = Agent(name="plain", prompt="Be useful.")
    (path,) = DshProfileCompiler().compile(
        agent,
        workspace=tmp_path / "workspace",
        dsh_home=tmp_path,
        attempt_id="attempt-2",
        tool_gateway_url=None,
    )
    patch = json.loads(path.read_text(encoding="utf-8"))
    assert {"id": "tool-skill", "disabled": True} in patch


def test_profile_inserts_authenticated_memory_pre_step_plugin(tmp_path) -> None:
    @memory_provider
    def search_memory(request: MemorySearchRequest) -> list[dict[str, str]]:
        del request
        return []

    agent = Agent(
        name="memory-agent",
        prompt="Use relevant memory.",
        memory_providers=(search_memory,),
    )
    (path,) = DshProfileCompiler().compile(
        agent,
        workspace=tmp_path / "workspace",
        dsh_home=tmp_path,
        attempt_id="attempt-memory",
        tool_gateway_url=None,
        memory_context_url="http://127.0.0.1:1234/memory/context",
        memory_context_token="secret-token",
    )

    patch = json.loads(path.read_text(encoding="utf-8"))
    insertion = next(
        row["insert"][0]
        for row in patch
        if "insert" in row and row["insert"][0]["id"] == "base-agent-memory-context"
    )
    assert insertion["name"].startswith("file://")
    assert insertion["name"].endswith("memory_context_plugin.mjs")
    assert insertion["config"] == {
        "url": "http://127.0.0.1:1234/memory/context",
        "token": "secret-token",
        "maxContextBytes": 16 * 1024,
        "timeoutMs": 10_000,
    }
