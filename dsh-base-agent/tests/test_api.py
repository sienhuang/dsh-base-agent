from __future__ import annotations

from pathlib import Path

from httpx import ASGITransport, AsyncClient

from dsh_base_agent import Agent, ControlPlane, RuntimeConfig, create_app
from dsh_base_agent.adapters.dsh.runtime import (
    DshEventHandler,
    DshNotificationHandler,
    DshRunResult,
    DshRuntime,
)
from dsh_base_agent.store import SqliteControlStore


class Runtime:
    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        del input, on_event, on_notification
        return DshRunResult(session_id=session_id, final_response="ok", finish_reason="completed")

    async def close(self) -> None:
        pass


class Factory:
    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
        memory_context_url: str | None = None,
        memory_context_token: str | None = None,
    ) -> DshRuntime:
        del (
            agent,
            workspace,
            dsh_home,
            attempt_id,
            tool_gateway_url,
            memory_context_url,
            memory_context_token,
        )
        return Runtime()


async def test_stable_run_http_contract(tmp_path) -> None:
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=Factory(),
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    app = create_app(control, close_on_shutdown=False)
    await control.start()
    transport = ASGITransport(app=app)
    headers = {"X-Tenant-ID": "tenant-a", "X-Principal-ID": "alice"}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/runs",
            headers={**headers, "Idempotency-Key": "request-1"},
            json={"agent_id": "orders", "input": "query order"},
        )
        assert response.status_code == 202
        run_id = response.json()["run_id"]
        await control.wait(run_id)

        response = await client.get(f"/v1/runs/{run_id}", headers=headers)
        assert response.status_code == 200
        assert response.json()["run"]["status"] == "succeeded"
        assert len(response.json()["attempts"]) == 1

        response = await client.get(f"/v1/runs/{run_id}/events", headers=headers)
        assert response.status_code == 200
        assert response.json()[-1]["kind"] == "run.succeeded"
    await control.close()


async def test_conversation_http_contract(tmp_path) -> None:
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=Factory(),
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    app = create_app(control, close_on_shutdown=False)
    await control.start()
    transport = ASGITransport(app=app)
    headers = {"X-Tenant-ID": "tenant-a", "X-Principal-ID": "alice"}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/conversations",
            headers=headers,
            json={"agent_id": "orders"},
        )
        assert response.status_code == 201
        conversation_id = response.json()["conversation_id"]
        assert "dsh_session_id" not in response.json()
        assert "dsh_home_key" not in response.json()

        response = await client.post(
            f"/v1/conversations/{conversation_id}/runs",
            headers={**headers, "Idempotency-Key": "message-1"},
            json={"input": "query order"},
        )
        assert response.status_code == 202
        run_id = response.json()["run_id"]
        await control.wait(run_id)

        response = await client.get(
            f"/v1/conversations/{conversation_id}/runs",
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json()[0]["conversation_id"] == conversation_id
        assert response.json()[0]["sequence"] == 1

        denied = await client.get(
            f"/v1/conversations/{conversation_id}",
            headers={"X-Tenant-ID": "tenant-a", "X-Principal-ID": "bob"},
        )
        assert denied.status_code == 403
    await control.close()
