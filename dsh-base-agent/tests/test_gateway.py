from __future__ import annotations

from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from dsh_base_agent import Agent, Principal, ReadOnlyByDefaultAuthorizer, tool
from dsh_base_agent.gateway import (
    GatewayRunContext,
    ToolGateway,
    _EnsureCompleteHttpResponse,
)
from dsh_base_agent.models import EventSource


async def test_gateway_completes_streaming_response_when_mcp_app_returns_early() -> None:
    messages: list[dict[str, Any]] = []

    async def incomplete_app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"event", "more_body": True})

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    app = _EnsureCompleteHttpResponse(incomplete_app)
    await app({"type": "http"}, receive, send)

    assert messages[-1] == {
        "type": "http.response.body",
        "body": b"",
        "more_body": False,
    }


async def test_gateway_does_not_append_to_complete_response() -> None:
    messages: list[dict[str, Any]] = []

    async def complete_app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"done"})

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    app = _EnsureCompleteHttpResponse(complete_app)
    await app({"type": "http"}, receive, send)

    assert messages == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"done"},
    ]


async def test_loopback_gateway_discovers_and_invokes_python_tool() -> None:
    @tool
    def greet(name: str) -> dict[str, str]:
        """Greet one person."""

        return {"message": f"Hello, {name}!"}

    events: list[tuple[EventSource, str, dict[str, Any]]] = []
    audits: list[tuple[str, str, str, dict[str, Any]]] = []

    async def write_event(
        source: EventSource,
        kind: str,
        data: dict[str, Any],
    ) -> None:
        events.append((source, kind, data))

    async def write_audit(
        tool_name: str,
        action: str,
        outcome: str,
        data: dict[str, Any],
    ) -> None:
        audits.append((tool_name, action, outcome, data))

    gateway = ToolGateway(
        agent=Agent(name="greeter", prompt="Greet users.", tools=(greet,)),
        authorizer=ReadOnlyByDefaultAuthorizer(),
        write_event=write_event,
        write_audit=write_audit,
    )
    await gateway.start()
    gateway.bind(
        GatewayRunContext(
            principal=Principal("tenant-a", "alice"),
            run_id="run-1",
            attempt_id="attempt-1",
        )
    )
    try:
        async with streamable_http_client(gateway.url) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                listed = await session.list_tools()
                assert [item.name for item in listed.tools] == ["greet"]
                result = await session.call_tool("greet", {"name": "Ada"})
                assert result.isError is False
                assert result.structuredContent == {"message": "Hello, Ada!"}
    finally:
        gateway.release("attempt-1")
        await gateway.close()

    assert [item[1] for item in events] == ["tool.started", "tool.completed"]
    assert events[0][2]["side_effect"] == "read_only"
    assert events[0][2]["operation_id"] == events[0][2]["idempotency_key"]
    assert [item[1:3] for item in audits] == [
        ("tool.authorize", "allowed"),
        ("tool.execute", "success"),
    ]
