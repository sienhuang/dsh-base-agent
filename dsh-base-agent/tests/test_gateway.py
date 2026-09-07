from __future__ import annotations

import hashlib
from typing import Any

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from dsh_base_agent import Agent, Principal, ReadOnlyByDefaultAuthorizer, tool
from dsh_base_agent.adapters.mcp.gateway import (
    GatewayRunContext,
    ToolGateway,
    _EnsureCompleteHttpResponse,
)
from dsh_base_agent.control.models import ArtifactRecord, EventSource


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


async def test_gateway_externalizes_large_tool_result_as_bounded_artifact_reference() -> None:
    @tool
    def large_result() -> dict[str, str]:
        """Return a result too large for one model observation."""

        return {"report": "x" * 5000}

    events: list[tuple[EventSource, str, dict[str, Any]]] = []
    audits: list[tuple[str, str, str, dict[str, Any]]] = []
    bodies: list[bytes] = []

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

    async def write_artifact(
        name: str,
        media_type: str,
        content: bytes,
    ) -> ArtifactRecord:
        bodies.append(content)
        return ArtifactRecord(
            artifact_id="artifact_large",
            run_id="run-1",
            attempt_id="attempt-1",
            name=name,
            media_type=media_type,
            location="local:objects/ab/artifact_large",
            sha256=hashlib.sha256(content).hexdigest(),
            size_bytes=len(content),
        )

    gateway = ToolGateway(
        agent=Agent(name="reporter", prompt="Create reports.", tools=(large_result,)),
        authorizer=ReadOnlyByDefaultAuthorizer(),
        write_event=write_event,
        write_audit=write_audit,
        write_artifact=write_artifact,
        max_observation_bytes=1024,
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
                result = await session.call_tool("large_result", {})
    finally:
        gateway.release("attempt-1")
        await gateway.close()

    assert result.isError is False
    assert result.structuredContent is not None
    assert result.structuredContent["externalized"] is True
    assert result.structuredContent["observation_complete"] is False
    assert result.structuredContent["artifact"]["artifact_id"] == "artifact_large"
    assert len(bodies) == 1
    assert len(bodies[0]) > 1024
    assert events[-1][2]["artifact_id"] == "artifact_large"
    assert events[-1][2]["externalized"] is True
    assert audits[-1][3]["artifact_id"] == "artifact_large"
