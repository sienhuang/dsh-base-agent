from __future__ import annotations

import pytest
from pydantic import ValidationError

from dsh_base_agent import SideEffect, ToolContext, tool


def test_tool_builds_schema_and_validates_arguments() -> None:
    @tool(side_effect=False)
    def query_order(order_id: str, limit: int = 1) -> dict[str, object]:
        """Query an order."""
        return {"order_id": order_id, "limit": limit}

    assert query_order.spec.name == "query_order"
    assert query_order.spec.description == "Query an order."
    assert query_order.spec.input_schema["required"] == ["order_id"]
    assert query_order.side_effect is SideEffect.READ_ONLY
    assert query_order.validate_arguments({"order_id": "o-1"}) == {
        "order_id": "o-1",
        "limit": 1,
    }
    with pytest.raises(ValidationError):
        query_order.validate_arguments({"order_id": "o-1", "unexpected": True})


async def test_tool_injects_governed_context() -> None:
    received: ToolContext | None = None

    @tool(side_effect=SideEffect.IDEMPOTENT)
    async def refund(order_id: str, context: ToolContext) -> str:
        nonlocal received
        received = context
        return order_id

    context = ToolContext(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        run_id="run-1",
        attempt_id="attempt-1",
        tool_name="refund",
        tool_call_id="call-1",
        operation_id="op-1",
    )
    assert await refund.invoke({"order_id": "o-1"}, context) == "o-1"
    assert received is context
    assert received.idempotency_key == "op-1"


def test_tool_rejects_untyped_parameters() -> None:
    with pytest.raises(TypeError, match="requires a type annotation"):

        @tool
        def invalid(value):  # type: ignore[no-untyped-def]
            return value

