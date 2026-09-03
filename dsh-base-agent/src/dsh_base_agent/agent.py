"""Immutable developer-facing Agent definition."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass

from dsh_base_agent.tools import Tool

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
        if any(not skill.strip() for skill in self.skills):
            raise ValueError("Agent skill names must not be blank")
        if len(self.skills) != len(set(self.skills)):
            raise ValueError("Agent skills must be unique")
        for item in self.tools:
            missing = item.permissions - self.permissions
            if missing:
                raise ValueError(
                    f"Tool '{item.spec.name}' requires undeclared Agent permissions: "
                    f"{', '.join(sorted(missing))}"
                )

    @property
    def fingerprint(self) -> str:
        payload = {
            "name": self.name,
            "permissions": sorted(self.permissions),
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

