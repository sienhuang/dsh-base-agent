"""The immutable Agent definition assembled from application extensions."""

from __future__ import annotations

from dsh_base_agent import Agent

from company_agent.context import render_static_context, static_context_sections
from company_agent.tools import get_request_context, query_order

_BASE_PROMPT = """你是公司订单助手。

处理订单问题时，先加载 order-support Skill，并严格按照 Skill 规定调用工具。
租户或用户相关的动态信息必须通过 get_request_context 获取，不要从 Prompt 猜测。
"""


def build_agent() -> Agent:
    static_context = render_static_context(static_context_sections())
    return Agent(
        name="iris-assistant-1",
        version="1.0.0",
        prompt=f"{_BASE_PROMPT.strip()}\n\n{static_context}",
        tools=(get_request_context, query_order),
        skills=("order-support",),
        permissions=frozenset({"orders:read", "context:read"}),
    )


__all__ = ["build_agent"]

