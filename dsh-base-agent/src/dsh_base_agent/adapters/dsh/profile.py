"""Compile immutable Agent definitions into DSH SDK Profile patches."""

from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import uuid4

from dsh_base_agent.sdk.agent import Agent


class ProfileCompilationError(RuntimeError):
    pass


_NATIVE_TOOL_ROWS = (
    "tool-bash",
    # "tool-pwsh",
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
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
        memory_context_url: str | None = None,
        memory_context_token: str | None = None,
    ) -> tuple[Path, ...]:
        if agent.tools and tool_gateway_url is None:
            raise ProfileCompilationError("Agent Tools require a loopback MCP gateway")
        if agent.memory_providers and (
            memory_context_url is None or memory_context_token is None
        ):
            raise ProfileCompilationError(
                "Agent Memory Providers require an authenticated pre-step context gateway"
            )
        patches: list[dict[str, object]] = [
            {"id": "system-prompt", "config": {"persona": agent.prompt}}
        ]
        patches.extend({"id": row, "disabled": True} for row in _NATIVE_TOOL_ROWS)
        if agent.skills:
            # DSH normally resolves project skills from the nearest ancestor that
            # contains ``.git``. A ControlPlane workspace is commonly nested below
            # that repository root (for example ``starter/workspace``), so register
            # its skill root explicitly instead of relying on project discovery.
            skill_root = (workspace.expanduser().resolve() / ".dsh" / "skills").resolve()
            patches.append(
                {
                    "id": "skill-filesystem",
                    "config": {"customSkillDirs": [str(skill_root)]},
                }
            )
        else:
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
        if memory_context_url is not None and memory_context_token is not None:
            plugin = Path(__file__).with_name("memory_context_plugin.mjs").resolve()
            patches.append(
                {
                    "insert": [
                        {
                            "id": "base-agent-memory-context",
                            "name": plugin.as_uri(),
                            "config": {
                                "url": memory_context_url,
                                "token": memory_context_token,
                                "maxContextBytes": 16 * 1024,
                                "timeoutMs": 10_000,
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
        os.chmod(path, 0o600)
        return
    temporary = path.with_suffix(f".{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


__all__ = ["DshProfileCompiler", "ProfileCompilationError"]
