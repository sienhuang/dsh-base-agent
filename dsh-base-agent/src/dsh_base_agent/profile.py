"""Compile immutable Agent definitions into DSH SDK Profile patches."""

from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

from dsh_base_agent.agent import Agent


class ProfileCompilationError(RuntimeError):
    pass


_NATIVE_TOOL_ROWS = (
    "tool-bash",
    "tool-pwsh",
    "tool-jobs",
    "tool-fs",
    "tool-fs-search",
    "plan-mode",
    "tool-subagent-control",
    "tool-subagent-list-agents",
    "tool-subagent",
    "tool-subagent-fork",
    "tool-subagent-report",
    "tool-workflow",
    "tool-todo",
    "tool-goal",
    "tool-ralph",
    "tool-str-replace-editor",
    "tool-web",
)


class DshProfileCompiler:
    """Materialize only Host-owned policy on top of DSH's full ``sdk`` Profile."""

    def compile(
        self,
        agent: Agent,
        *,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
    ) -> tuple[Path, ...]:
        if agent.tools and tool_gateway_url is None:
            raise ProfileCompilationError("Agent Tools require a loopback MCP gateway")
        patches: list[dict[str, object]] = [
            {"id": "system-prompt", "config": {"persona": agent.prompt}}
        ]
        patches.extend({"id": row, "disabled": True} for row in _NATIVE_TOOL_ROWS)
        if not agent.skills:
            patches.append({"id": "tool-skill", "disabled": True})
        if tool_gateway_url is not None:
            patches.append(
                {
                    "insert": [
                        {
                            "id": "base-agent-mcp-tools",
                            "name": "@deepseek-ai/dsh-mcp-client",
                            "config": {
                                "serverName": "dsh_base_agent",
                                "transport": "streamable-http",
                                "url": tool_gateway_url,
                            },
                        }
                    ]
                }
            )

        patch_dir = dsh_home / "control-plane-patches"
        patch_dir.mkdir(parents=True, exist_ok=True)
        path = patch_dir / f"{attempt_id}.patch.json"
        serialized = json.dumps(
            patches,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        _atomic_write(path, serialized)
        return (path,)


def _atomic_write(path: Path, content: str) -> None:
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return
    temporary = path.with_suffix(f".{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


__all__ = ["DshProfileCompiler", "ProfileCompilationError"]

