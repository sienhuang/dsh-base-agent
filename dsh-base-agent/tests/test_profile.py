from __future__ import annotations

import json

from dsh_base_agent import Agent, tool
from dsh_base_agent.profile import DshProfileCompiler


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
        dsh_home=tmp_path,
        attempt_id="attempt-1",
        tool_gateway_url="http://127.0.0.1:1234/mcp",
    )
    patch = json.loads(path.read_text(encoding="utf-8"))
    assert patch[0] == {"id": "system-prompt", "config": {"persona": "Handle orders."}}
    assert {"id": "tool-bash", "disabled": True} in patch
    assert {"id": "tool-skill", "disabled": True} not in patch
    mcp = next(item for item in patch if "insert" in item)
    assert mcp["insert"][0]["name"] == "@deepseek-ai/dsh-mcp-client"


def test_profile_disables_skill_tool_when_agent_selects_no_skills(tmp_path) -> None:
    agent = Agent(name="plain", prompt="Be useful.")
    (path,) = DshProfileCompiler().compile(
        agent,
        dsh_home=tmp_path,
        attempt_id="attempt-2",
        tool_gateway_url=None,
    )
    patch = json.loads(path.read_text(encoding="utf-8"))
    assert {"id": "tool-skill", "disabled": True} in patch

