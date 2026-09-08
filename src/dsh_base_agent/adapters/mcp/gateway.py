"""Loopback MCP gateway for application-owned Python Tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import uvicorn
from mcp import types
from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from dsh_base_agent.control.auth import Authorizer, Principal, ToolAuthorization
from dsh_base_agent.control.models import ArtifactRecord, EventSource
from dsh_base_agent.sdk.agent import Agent
from dsh_base_agent.sdk.tools import ToolContext

type EventWriter = Callable[[EventSource, str, dict[str, Any]], Awaitable[None]]
type AuditWriter = Callable[[str, str, str, dict[str, Any]], Awaitable[None]]
type ArtifactWriter = Callable[[str, str, bytes], Awaitable[ArtifactRecord]]


class ToolGatewayError(RuntimeError):
    pass


class _EnsureCompleteHttpResponse:
    """Finish an HTTP response if an upstream streaming ASGI app returns early."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        response_started = False
        response_complete = False

        async def tracked_send(message: Message) -> None:
            nonlocal response_started, response_complete
            await send(message)
            if message["type"] == "http.response.start":
                response_started = True
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                response_complete = True

        await self._app(scope, receive, tracked_send)
        if response_started and not response_complete:
            # MCP's SSE response can return during client/session teardown without
            # emitting its final ASGI body frame. Uvicorn treats that as a broken
            # application response, so complete the response before returning.
            await send({"type": "http.response.body", "body": b"", "more_body": False})


@dataclass(frozen=True, slots=True)
class GatewayRunContext:
    principal: Principal
    run_id: str
    attempt_id: str


@dataclass(frozen=True, slots=True)
class _GatewayBinding:
    context: GatewayRunContext
    write_event: EventWriter
    write_audit: AuditWriter
    write_artifact: ArtifactWriter | None


class ToolGateway:
    """Expose one Agent's selected Tools over loopback Streamable HTTP MCP."""

    def __init__(
        self,
        *,
        agent: Agent,
        authorizer: Authorizer,
        write_event: EventWriter | None = None,
        write_audit: AuditWriter | None = None,
        write_artifact: ArtifactWriter | None = None,
        max_tool_calls: int = 64,
        max_observation_bytes: int = 64 * 1024,
    ) -> None:
        if max_tool_calls <= 0:
            raise ValueError("max_tool_calls must be positive")
        if max_observation_bytes < 1024:
            raise ValueError("max_observation_bytes must be at least 1024")
        self.agent = agent
        self._authorizer = authorizer
        self._default_write_event = write_event
        self._default_write_audit = write_audit
        self._default_write_artifact = write_artifact
        self._max_tool_calls = max_tool_calls
        self._max_observation_bytes = max_observation_bytes
        self._tools = {item.spec.name: item for item in agent.tools}
        self._binding: _GatewayBinding | None = None
        self._call_index = 0
        self._call_lock = asyncio.Lock()
        self._server = Server(f"dsh-base-agent-{agent.name}", version="0.1.0")
        self._manager = StreamableHTTPSessionManager(
            self._server,
            json_response=True,
            stateless=True,
        )
        self._uvicorn: uvicorn.Server | None = None
        self._serve_task: asyncio.Task[None] | None = None
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("127.0.0.1", 0))
            server_socket.listen(128)
            server_socket.setblocking(False)
            host, port = server_socket.getsockname()[:2]
        except BaseException:
            server_socket.close()
            raise
        self._socket = server_socket
        self.url = f"http://{host}:{port}/mcp"
        self._register_handlers()

    async def start(self) -> None:
        if self._serve_task is not None:
            return
        for item in self.agent.tools:
            readiness = await item.check_readiness()
            if not readiness.ready:
                detail = f": {readiness.detail}" if readiness.detail else ""
                raise ToolGatewayError(f"Tool '{item.spec.name}' is not ready{detail}")
        server = uvicorn.Server(
            uvicorn.Config(
                _EnsureCompleteHttpResponse(self._manager.handle_request),
                log_level="warning",
                access_log=False,
                lifespan="off",
                interface="asgi3",
            )
        )
        self._uvicorn = server

        async def serve() -> None:
            async with self._manager.run():
                await server.serve(sockets=[self._socket])

        self._serve_task = asyncio.create_task(serve(), name=f"tool-gateway-{self.agent.name}")
        for _ in range(200):
            if server.started:
                return
            if self._serve_task.done():
                await self._serve_task
                raise ToolGatewayError("Tool gateway exited before startup")
            await asyncio.sleep(0.01)
        await self.close()
        raise TimeoutError("Tool gateway did not start within two seconds")

    def bind(
        self,
        context: GatewayRunContext,
        *,
        write_event: EventWriter | None = None,
        write_audit: AuditWriter | None = None,
        write_artifact: ArtifactWriter | None = None,
    ) -> None:
        if self._binding is not None:
            raise ToolGatewayError("Tool gateway is already bound")
        selected_event_writer = write_event or self._default_write_event
        selected_audit_writer = write_audit or self._default_write_audit
        if selected_event_writer is None or selected_audit_writer is None:
            raise ToolGatewayError("Tool gateway binding requires event and audit writers")
        self._binding = _GatewayBinding(
            context=context,
            write_event=selected_event_writer,
            write_audit=selected_audit_writer,
            write_artifact=write_artifact or self._default_write_artifact,
        )
        self._call_index = 0

    def release(self, attempt_id: str) -> None:
        if self._binding is not None and self._binding.context.attempt_id == attempt_id:
            self._binding = None
            self._call_index = 0

    async def close(self) -> None:
        server = self._uvicorn
        task = self._serve_task
        self._uvicorn = None
        self._serve_task = None
        if server is not None:
            server.should_exit = True
        if task is not None:
            await task
        if self._socket.fileno() != -1:
            self._socket.close()

    def _register_handlers(self) -> None:
        @self._server.list_tools()  # type: ignore[no-untyped-call,untyped-decorator]
        async def list_tools() -> list[types.Tool]:
            return [
                types.Tool(
                    name=item.spec.name,
                    description=item.spec.description,
                    inputSchema=dict(item.spec.input_schema),
                )
                for item in self.agent.tools
            ]

        @self._server.call_tool()  # type: ignore[untyped-decorator]
        async def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
            binding = self._binding
            if binding is None:
                raise ToolGatewayError("no RunAttempt owns this Tool call")
            context = binding.context
            try:
                selected = self._tools[name]
            except KeyError as exc:
                raise ToolGatewayError(f"Tool '{name}' is not registered") from exc
            validated = dict(selected.validate_arguments(arguments))
            async with self._call_lock:
                self._call_index += 1
                operation_index = self._call_index
            if operation_index > self._max_tool_calls:
                raise ToolGatewayError(f"Tool call budget exceeded ({self._max_tool_calls})")
            tool_call_id = f"mcp_{uuid4().hex}"
            operation_id = _operation_id(context.attempt_id, operation_index)
            intent = _digest({"tool_name": name, "arguments": validated})
            common = {
                "tool_name": name,
                "tool_call_id": tool_call_id,
                "operation_id": operation_id,
                "idempotency_key": operation_id,
                "intent_sha256": intent,
                "side_effect": selected.side_effect.value,
                "operation_index": operation_index,
            }
            await binding.write_event(EventSource.TOOL, "tool.started", common)
            try:
                await self._authorizer.authorize_tool(
                    ToolAuthorization(
                        principal=context.principal,
                        agent=self.agent,
                        run_id=context.run_id,
                        attempt_id=context.attempt_id,
                        tool=selected,
                        arguments=validated,
                    )
                )
                await binding.write_audit(
                    name,
                    "tool.authorize",
                    "allowed",
                    {"intent_sha256": intent, "side_effect": selected.side_effect.value},
                )
                result = await asyncio.wait_for(
                    selected.invoke(
                        validated,
                        ToolContext(
                            tenant_id=context.principal.tenant_id,
                            principal_id=context.principal.principal_id,
                            agent_id=self.agent.name,
                            run_id=context.run_id,
                            attempt_id=context.attempt_id,
                            tool_name=name,
                            tool_call_id=tool_call_id,
                            operation_id=operation_id,
                        ),
                    ),
                    timeout=selected.timeout_seconds,
                )
                normalized = _json_value(result)
                encoded = _json_bytes(normalized)
                size = len(encoded)
                artifact: ArtifactRecord | None = None
                observation = normalized
                if size > self._max_observation_bytes:
                    if binding.write_artifact is None:
                        raise ToolGatewayError(
                            f"Tool observation exceeds {self._max_observation_bytes} bytes "
                            "and no ArtifactStore writer is configured"
                        )
                    artifact = await binding.write_artifact(
                        _artifact_name(name, operation_index),
                        "application/json",
                        encoded,
                    )
                    observation = _artifact_observation(
                        artifact,
                        encoded,
                        max_observation_bytes=self._max_observation_bytes,
                    )
            except BaseException as exc:
                await binding.write_event(
                    EventSource.TOOL,
                    "tool.completed",
                    {**common, "status": "error", "error_type": type(exc).__name__},
                )
                await binding.write_audit(
                    name,
                    "tool.execute",
                    "error",
                    {"error_type": type(exc).__name__, "intent_sha256": intent},
                )
                raise
            result_sha256 = artifact.sha256 if artifact is not None else _digest(normalized)
            completion = {
                **common,
                "status": "success",
                "result_sha256": result_sha256,
                "result_size_bytes": size,
                "externalized": artifact is not None,
            }
            if artifact is not None:
                completion["artifact_id"] = artifact.artifact_id
            await binding.write_event(
                EventSource.TOOL,
                "tool.completed",
                completion,
            )
            await binding.write_audit(
                name,
                "tool.execute",
                "success",
                {
                    "intent_sha256": intent,
                    "result_sha256": result_sha256,
                    "result_size_bytes": size,
                    "externalized": artifact is not None,
                    **({"artifact_id": artifact.artifact_id} if artifact is not None else {}),
                },
            )
            return observation if isinstance(observation, dict) else {"result": observation}


def _operation_id(attempt_id: str, operation_index: int) -> str:
    digest = hashlib.sha256(f"{attempt_id}\0tool:{operation_index}".encode()).hexdigest()
    return f"op:{digest}"


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _artifact_name(tool_name: str, operation_index: int) -> str:
    safe_tool_name = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in tool_name
    ).strip("-")
    return f"{(safe_tool_name or 'tool')[:80]}-{operation_index}.json"


def _artifact_observation(
    artifact: ArtifactRecord,
    encoded: bytes,
    *,
    max_observation_bytes: int,
) -> dict[str, Any]:
    preview_bytes = min(4096, max_observation_bytes // 2)
    preview = encoded[:preview_bytes].decode("utf-8", errors="ignore")
    observation: dict[str, Any] = {
        "externalized": True,
        "complete_result_stored": True,
        "observation_complete": False,
        "artifact": {
            "artifact_id": artifact.artifact_id,
            "name": artifact.name,
            "media_type": artifact.media_type,
            "size_bytes": artifact.size_bytes,
            "sha256": artifact.sha256,
        },
        "preview": preview,
        "preview_format": "json-prefix",
        "message": (
            "The complete Tool result was stored as an artifact. "
            "The preview is incomplete and must not be treated as the complete result."
        ),
    }
    if len(_json_bytes(observation)) <= max_observation_bytes:
        return observation
    observation.pop("preview")
    observation.pop("preview_format")
    if len(_json_bytes(observation)) > max_observation_bytes:
        raise ToolGatewayError("Artifact reference exceeds the Tool observation limit")
    return observation


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ToolGatewayError("Tool input and output must be finite JSON") from exc


__all__ = ["GatewayRunContext", "ToolGateway", "ToolGatewayError"]
