from __future__ import annotations

from pathlib import Path

from httpx import ASGITransport, AsyncClient
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from dsh_base_agent import (
    Agent,
    AttemptStatus,
    ControlPlane,
    Principal,
    RequiredActionRecord,
    RequiredActionSource,
    RequiredActionStatus,
    RequiredActionType,
    RunAttempt,
    RunRecord,
    RunStatus,
    RuntimeConfig,
    create_app,
)
from dsh_base_agent.adapters.dsh.runtime import (
    DshEventHandler,
    DshNotificationHandler,
    DshRunResult,
    DshRuntime,
)
from dsh_base_agent.control.models import ConversationRecord
from dsh_base_agent.store import SqliteControlStore


class Runtime:
    def __init__(self, calls: list[tuple[str, str]]) -> None:
        self.calls = calls

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        del on_event, on_notification
        self.calls.append((input, session_id))
        return DshRunResult(session_id=session_id, final_response="ok", finish_reason="completed")

    async def close(self) -> None:
        pass


class Factory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

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
        return Runtime(self.calls)


class AskingRuntime:
    def __init__(self, factory: AskingFactory, tool_gateway_url: str) -> None:
        self.factory = factory
        self.tool_gateway_url = tool_gateway_url

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        del on_event, on_notification
        self.factory.calls.append((input, session_id))
        if len(self.factory.calls) == 1:
            async with streamable_http_client(self.tool_gateway_url) as streams:
                async with ClientSession(streams[0], streams[1]) as session:
                    await session.initialize()
                    result = await session.call_tool(
                        "ask_user_question",
                        {
                            "questions": [
                                {
                                    "id": "shipping",
                                    "question": "Choose shipping.",
                                    "options": [
                                        {"label": "standard"},
                                        {"label": "express"},
                                    ],
                                }
                            ]
                        },
                    )
                    assert result.isError is False
            return DshRunResult(
                session_id=session_id,
                final_response="",
                finish_reason="completed",
            )
        return DshRunResult(
            session_id=session_id,
            final_response="Shipping selected.",
            finish_reason="completed",
        )

    async def close(self) -> None:
        pass


class AskingFactory:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

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
        del agent, workspace, dsh_home, attempt_id, memory_context_url, memory_context_token
        assert tool_gateway_url is not None
        return AskingRuntime(self, tool_gateway_url)


async def test_stable_run_http_contract(tmp_path) -> None:
    factory = Factory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
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
    factory = Factory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
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


async def test_model_tool_creates_durable_action_then_same_session_continues(tmp_path) -> None:
    factory = AskingFactory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    agent = Agent(name="interactive", prompt="Ask when needed.", allow_user_input=True)
    control.register(agent)
    principal = Principal("tenant-a", "alice")
    await control.start()
    try:
        conversation = await control.create_conversation(
            principal=principal,
            agent_id=agent.name,
        )
        submitted = await control.submit_to_conversation(
            principal=principal,
            conversation_id=conversation.conversation_id,
            input="Arrange shipping.",
        )
        waiting = await control.wait(submitted.run_id)
        view = await control.view(principal, submitted.run_id)

        assert waiting.status is RunStatus.WAITING
        assert view.required_action is not None
        action = view.required_action
        await control.resume(
            principal,
            submitted.run_id,
            action_id=action.action_id,
            expected_revision=waiting.revision,
            response={"answers": [{"id": "shipping", "selected": ["express"]}]},
        )
        completed = await control.wait(submitted.run_id)
        persisted_action = await control.store.get_required_action(action.action_id)

        assert completed.status is RunStatus.SUCCEEDED
        assert persisted_action.status is RequiredActionStatus.CONSUMED
        assert len(factory.calls) == 2
        assert factory.calls[0][1] == factory.calls[1][1] == conversation.dsh_session_id
        assert '"selected":["express"]' in factory.calls[1][0]
    finally:
        await control.close()


async def test_run_view_and_resume_drive_required_action_continuation(tmp_path) -> None:
    agent = Agent(
        name="orders",
        prompt="Handle orders.",
        allow_user_input=True,
    )
    store = SqliteControlStore(tmp_path / "control.db")
    await store.initialize()
    conversation = ConversationRecord(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id=agent.name,
        agent_version=agent.version,
        agent_fingerprint=agent.fingerprint,
        dsh_home_key="b" * 64,
        dsh_session_id="session-shared",
    )
    await store.create_conversation(conversation)
    queued, _ = await store.create_conversation_run(
        conversation.conversation_id,
        RunRecord(
            conversation_id=conversation.conversation_id,
            tenant_id="tenant-a",
            principal_id="alice",
            agent_id=agent.name,
            agent_version=agent.version,
            agent_fingerprint=agent.fingerprint,
            input="Deploy the service.",
        ),
        idempotency_key=None,
        request_digest="request",
    )
    attempt = RunAttempt(
        run_id=queued.run_id,
        number=1,
        dsh_session_id=conversation.dsh_session_id,
        status=AttemptStatus.RUNNING,
    )
    running = queued.model_copy(
        update={
            "status": RunStatus.RUNNING,
            "revision": 2,
            "attempt_count": 1,
            "active_attempt_id": attempt.attempt_id,
        }
    )
    await store.begin_attempt(running, attempt, expected_run_revision=1)
    action = RequiredActionRecord(
        run_id=queued.run_id,
        attempt_id=attempt.attempt_id,
        conversation_id=conversation.conversation_id,
        tenant_id="tenant-a",
        principal_id="alice",
        type=RequiredActionType.USER_INPUT,
        source=RequiredActionSource.MODEL,
        payload={
            "questions": [
                {
                    "id": "environment",
                    "question": "Choose an environment.",
                    "options": [{"label": "dev"}, {"label": "prod"}],
                    "multi_select": False,
                }
            ]
        },
    )
    waiting, _ = await store.enter_required_action(action)
    await store.close()

    factory = Factory()
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=factory,
    )
    control.register(agent)
    app = create_app(control, close_on_shutdown=False)
    await control.start()
    transport = ASGITransport(app=app)
    headers = {"X-Tenant-ID": "tenant-a", "X-Principal-ID": "alice"}
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        standalone = await client.post(
            "/v1/runs",
            headers=headers,
            json={"agent_id": "orders", "input": "ask me something"},
        )
        assert standalone.status_code == 409

        response = await client.get(f"/v1/runs/{queued.run_id}", headers=headers)
        assert response.status_code == 200
        assert response.json()["run"]["status"] == "waiting"
        assert response.json()["required_action"]["action_id"] == action.action_id

        response = await client.post(
            f"/v1/runs/{queued.run_id}/resume",
            headers=headers,
            json={
                "action_id": action.action_id,
                "expected_revision": waiting.revision,
                "response": {
                    "answers": [{"id": "environment", "selected": ["dev"]}]
                },
            },
        )
        assert response.status_code == 200
        assert response.json()["required_action"] is None

        repeated = await client.post(
            f"/v1/runs/{queued.run_id}/resume",
            headers=headers,
            json={
                "action_id": action.action_id,
                "expected_revision": waiting.revision,
                "response": {
                    "answers": [{"id": "environment", "selected": ["dev"]}]
                },
            },
        )
        assert repeated.status_code == 409

    completed = await control.wait(queued.run_id)
    view = await control.view(Principal("tenant-a", "alice"), queued.run_id)
    assert completed.status is RunStatus.SUCCEEDED
    assert len(view.attempts) == 2
    assert view.attempts[0].dsh_session_id == view.attempts[1].dsh_session_id
    assert factory.calls[0][1] == conversation.dsh_session_id
    assert "action_id: " + action.action_id in factory.calls[0][0]
    assert '"selected":["dev"]' in factory.calls[0][0]
    await control.close()
