"""Tenant-aware authorization seams for Runs and Tool calls."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from dsh_base_agent.sdk.agent import Agent
from dsh_base_agent.sdk.tools import SideEffect, Tool


class AuthorizationDenied(PermissionError):
    pass


@dataclass(frozen=True, slots=True)
class Principal:
    tenant_id: str
    principal_id: str

    def __post_init__(self) -> None:
        if not self.tenant_id.strip() or not self.principal_id.strip():
            raise ValueError("tenant_id and principal_id must not be blank")


@dataclass(frozen=True, slots=True)
class ToolAuthorization:
    principal: Principal
    agent: Agent
    run_id: str
    attempt_id: str
    tool: Tool
    arguments: dict[str, Any]


class Authorizer(Protocol):
    async def authorize_run(self, principal: Principal, agent: Agent) -> None: ...

    async def authorize_tool(self, request: ToolAuthorization) -> None: ...


class ReadOnlyByDefaultAuthorizer:
    """Useful development default that fails closed for mutating Tools."""

    async def authorize_run(self, principal: Principal, agent: Agent) -> None:
        del principal, agent

    async def authorize_tool(self, request: ToolAuthorization) -> None:
        if request.tool.side_effect is not SideEffect.READ_ONLY:
            raise AuthorizationDenied(
                f"Tool '{request.tool.spec.name}' has side effects and requires a custom Authorizer"
            )


__all__ = [
    "AuthorizationDenied",
    "Authorizer",
    "Principal",
    "ReadOnlyByDefaultAuthorizer",
    "ToolAuthorization",
]
