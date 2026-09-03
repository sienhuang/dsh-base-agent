"""Replaceable application authorization policy."""

from __future__ import annotations

from dsh_base_agent import (
    Agent,
    AuthorizationDenied,
    Principal,
    SideEffect,
    ToolAuthorization,
)


class StarterAuthorizer:
    """Development policy for the starter's demo tenant.

    Real applications should query the company's authorization service instead of
    embedding grants in process memory.
    """

    _tenant_permissions = {
        "demo-tenant": frozenset({"orders:read", "context:read"}),
    }

    async def authorize_run(self, principal: Principal, agent: Agent) -> None:
        del agent
        if principal.tenant_id not in self._tenant_permissions:
            raise AuthorizationDenied("the starter only enables the demo-tenant tenant")

    async def authorize_tool(self, request: ToolAuthorization) -> None:
        if request.tool.side_effect is not SideEffect.READ_ONLY:
            raise AuthorizationDenied("the starter only permits read-only Tools")
        granted = self._tenant_permissions.get(request.principal.tenant_id, frozenset())
        missing = request.tool.permissions - granted
        if missing:
            raise AuthorizationDenied(
                f"missing Tool permissions: {', '.join(sorted(missing))}"
            )


__all__ = ["StarterAuthorizer"]

