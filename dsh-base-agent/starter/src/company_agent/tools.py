"""Business Tools exposed to DSH through the base-agent MCP gateway."""

from __future__ import annotations

from typing import Any

from dsh_base_agent import ToolContext, tool

_ORDERS: dict[tuple[str, str], dict[str, str]] = {
    ("demo-tenant", "order-001"): {
        "status": "paid",
        "shipping_status": "waiting_for_pickup",
    },
    ("demo-tenant", "order-002"): {
        "status": "shipped",
        "shipping_status": "in_transit",
    },
}

_PRINCIPAL_PREFERENCES: dict[tuple[str, str], dict[str, str]] = {
    ("demo-tenant", "demo-user"): {
        "locale": "zh-CN",
        "answer_style": "concise",
    }
}


@tool(side_effect=False, permissions=("orders:read",))
def query_order(order_id: str, context: ToolContext) -> dict[str, Any]:
    """Query one order inside the caller's tenant boundary."""

    record = _ORDERS.get((context.tenant_id, order_id))
    if record is None:
        return {"found": False, "order_id": order_id}
    return {"found": True, "order_id": order_id, **record}


@tool(side_effect=False, permissions=("context:read",))
def get_request_context(context: ToolContext) -> dict[str, str]:
    """Load dynamic preferences for the authenticated tenant and principal."""

    preferences = _PRINCIPAL_PREFERENCES.get(
        (context.tenant_id, context.principal_id),
        {"locale": "zh-CN", "answer_style": "default"},
    )
    return {
        "tenant_id": context.tenant_id,
        "principal_id": context.principal_id,
        **preferences,
    }

@tool(side_effect=False, permissions=("orders:read",))
def generate_large_report(context: ToolContext) -> dict[str, Any]:
    """Generate a large report for testing ArtifactStore."""

    return {
        "tenant_id": context.tenant_id,
        "rows": [
            {
                "index": index,
                "order_id": f"order-{index:05d}",
                "detail": "用于测试 ArtifactStore 的订单明细" * 10,
            }
            for index in range(500)
        ],
    }

__all__ = ["get_request_context", "query_order", "generate_large_report"]

