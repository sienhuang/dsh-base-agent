"""Minimal local SDK example; it uses DSH for the actual Agent execution."""

from __future__ import annotations

import asyncio

from dsh_base_agent import Agent, ControlPlane, Principal, RuntimeConfig, tool


@tool(side_effect=False)
def query_order(order_id: str) -> dict[str, str]:
    """Query an order by ID."""

    return {"order_id": order_id, "status": "paid"}


async def main() -> None:
    control = ControlPlane(workspace=".", runtime=RuntimeConfig.from_env())
    control.register(
        Agent(
            name="order-assistant",
            prompt="你是订单助手。回答前必须查询订单。",
            tools=(query_order,),
        )
    )
    try:
        run = await control.submit(
            principal=Principal("demo-tenant", "demo-user"),
            agent_id="order-assistant",
            input="查询订单 order-001。",
            idempotency_key="demo-order-001",
        )
        completed = await control.wait(run.run_id)
        print(completed.output)
    finally:
        await control.close()


if __name__ == "__main__":
    asyncio.run(main())

