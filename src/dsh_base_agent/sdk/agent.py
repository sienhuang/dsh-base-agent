"""Immutable developer-facing Agent definition."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

from dsh_base_agent.sdk.memory import MemoryProvider
from dsh_base_agent.sdk.plan_react import PlanReact
from dsh_base_agent.sdk.tools import Tool
from dsh_base_agent.sdk.user_input import ASK_USER_TOOL_NAME

_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True, slots=True)
class Agent:
    """Business Agent definition compiled into a DSH Profile patch."""

    name: str
    prompt: str
    tools: tuple[Tool, ...] = ()
    skills: tuple[str, ...] = ()
    version: str = "1.0.0"
    permissions: frozenset[str] = frozenset()
    memory_providers: tuple[MemoryProvider, ...] = ()
    plan_react: PlanReact | None = None
    allow_user_input: bool = False

    def __post_init__(self) -> None:
        if not _NAME.fullmatch(self.name):
            raise ValueError("Agent name must contain only letters, numbers, '.', '_' or '-'")
        if not self.prompt.strip():
            raise ValueError("Agent prompt must not be blank")
        if not self.version.strip():
            raise ValueError("Agent version must not be blank")
        tool_names = [item.spec.name for item in self.tools]
        if len(tool_names) != len(set(tool_names)):
            raise ValueError("Agent tools must have unique names")
        if self.plan_react is not None and "submit_plan" in tool_names:
            raise ValueError("Plan-ReAct reserves the Tool name 'submit_plan'")
        if self.plan_react is not None and self.allow_user_input:
            raise ValueError(
                "Plan-ReAct and durable user input cannot be enabled on the same Agent"
            )
        if ASK_USER_TOOL_NAME in tool_names:
            raise ValueError(f"base-agent reserves the Tool name '{ASK_USER_TOOL_NAME}'")
        if any(not skill.strip() for skill in self.skills):
            raise ValueError("Agent skill names must not be blank")
        if len(self.skills) != len(set(self.skills)):
            raise ValueError("Agent skills must be unique")
        provider_names = [item.name for item in self.memory_providers]
        if len(provider_names) != len(set(provider_names)):
            raise ValueError("Agent Memory Providers must have unique names")
        for item in self.tools:
            missing = item.permissions - self.permissions
            if missing:
                raise ValueError(
                    f"Tool '{item.spec.name}' requires undeclared Agent permissions: "
                    f"{', '.join(sorted(missing))}"
                )
        for provider in self.memory_providers:
            missing = provider.permissions - self.permissions
            if missing:
                raise ValueError(
                    f"Memory Provider '{provider.name}' requires undeclared Agent permissions: "
                    f"{', '.join(sorted(missing))}"
                )

    @property
    def fingerprint(self) -> str:
        payload = {
            "allow_user_input": self.allow_user_input,
            "name": self.name,
            "memory_providers": [
                {
                    "max_results": item.max_results,
                    "name": item.name,
                    "permissions": sorted(item.permissions),
                    "timeout_seconds": item.timeout_seconds,
                    "version": item.version,
                }
                for item in self.memory_providers
            ],
            "permissions": sorted(self.permissions),
            "plan_react": (
                None
                if self.plan_react is None
                else {
                    "max_steps": self.plan_react.max_steps,
                    "max_task_chars": self.plan_react.max_task_chars,
                    "workflow_id": self.plan_react.workflow_id,
                    "workflow_version": self.plan_react.workflow_version,
                }
            ),
            "prompt": self.prompt,
            "skills": list(self.skills),
            "tools": [
                {
                    "confirmation_required": item.confirmation_required,
                    "permissions": sorted(item.permissions),
                    "side_effect": item.side_effect.value,
                    "spec": item.spec.model_dump(mode="json"),
                    "timeout_seconds": item.timeout_seconds,
                }
                for item in self.tools
            ],
            "version": self.version,
        }
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


__all__ = ["Agent"]
