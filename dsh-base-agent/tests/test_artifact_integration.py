from __future__ import annotations

import json
from pathlib import Path

from httpx import ASGITransport, AsyncClient
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from dsh_base_agent import Agent, ControlPlane, Principal, RuntimeConfig, create_app, tool
from dsh_base_agent.adapters.dsh.runtime import (
    DshEventHandler,
    DshNotificationHandler,
    DshRunResult,
    DshRuntime,
)
from dsh_base_agent.store import SqliteControlStore


class ToolCallingRuntime:
    def __init__(self, tool_gateway_url: str) -> None:
        self.tool_gateway_url = tool_gateway_url
        self.observation: dict[str, object] | None = None

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        del input, on_event, on_notification
        async with streamable_http_client(self.tool_gateway_url) as (
            read_stream,
            write_stream,
            _,
        ):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool("large_report", {})
        assert result.structuredContent is not None
        self.observation = result.structuredContent
        return DshRunResult(
            session_id=session_id,
            final_response="report stored",
            finish_reason="completed",
        )

    async def close(self) -> None:
        return None


class ToolCallingRuntimeFactory:
    def __init__(self) -> None:
        self.runtime: ToolCallingRuntime | None = None

    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
    ) -> DshRuntime:
        del agent, workspace, dsh_home, attempt_id
        assert tool_gateway_url is not None
        self.runtime = ToolCallingRuntime(tool_gateway_url)
        return self.runtime


async def test_large_tool_result_is_stored_and_downloaded_through_owned_run(tmp_path) -> None:
    @tool
    def large_report() -> dict[str, str]:
        """Build a deliberately large JSON report."""

        return {"report": "订单数据" * 20_000}

    factory = ToolCallingRuntimeFactory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    control.register(
        Agent(
            name="reporter",
            prompt="Create reports.",
            tools=(large_report,),
        )
    )
    principal = Principal("tenant-a", "alice")
    submitted = await control.submit(
        principal=principal,
        agent_id="reporter",
        input="create report",
    )
    completed = await control.wait(submitted.run_id)

    assert completed.output == "report stored"
    assert factory.runtime is not None
    assert factory.runtime.observation is not None
    assert factory.runtime.observation["externalized"] is True
    artifacts = await control.artifacts(principal, submitted.run_id)
    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact.artifact_id == factory.runtime.observation["artifact"]["artifact_id"]  # type: ignore[index]
    record, stream = await control.open_artifact(principal, submitted.run_id, artifact.artifact_id)
    body = b"".join([chunk async for chunk in stream])
    assert record == artifact
    assert json.loads(body) == {"report": "订单数据" * 20_000}

    app = create_app(control, close_on_shutdown=False)
    transport = ASGITransport(app=app)
    headers = {"X-Tenant-ID": "tenant-a", "X-Principal-ID": "alice"}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        downloaded = await client.get(
            f"/v1/runs/{submitted.run_id}/artifacts/{artifact.artifact_id}/content",
            headers=headers,
        )
        assert downloaded.status_code == 200
        assert downloaded.content == body
        assert downloaded.headers["x-artifact-sha256"] == artifact.sha256

        denied = await client.get(
            f"/v1/runs/{submitted.run_id}/artifacts/{artifact.artifact_id}/content",
            headers={"X-Tenant-ID": "tenant-a", "X-Principal-ID": "bob"},
        )
        assert denied.status_code == 403

    await control.close()
