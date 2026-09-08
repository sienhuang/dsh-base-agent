"""Replaceable read-only adapter for the company's external Memory service."""

from __future__ import annotations

from dsh_base_agent import MemoryItem, MemorySearchRequest, memory_provider


@memory_provider(
    name="user-memory",
    version="1.0.0",
    permissions=("memory:read",),
    timeout_seconds=3.0,
    max_results=3,
)
async def search_user_memory(request: MemorySearchRequest) -> list[MemoryItem]:
    """Retrieve demo Memory scoped by the trusted tenant and principal.

    Replace this body with a call to the company's Memory/RAG search API. Do not
    accept tenant_id or principal_id from model-generated arguments; use the
    identity carried by ``request``.
    """

    if request.tenant_id != "demo-tenant":
        return []
    return [
        MemoryItem(
            memory_id=f"answer-preference:{request.principal_id}",
            source="starter-demo-memory",
            score=1.0,
            content=(
                f"用户 {request.principal_id} 偏好中文回答；订单状态应先给出简短结论，"
                "再给出必要说明。"
            ),
        )
    ]


__all__ = ["search_user_memory"]
