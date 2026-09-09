from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from dsh_base_agent.adapters.dsh.runtime import (
    DshWorkflowRequest,
    _OfficialDshRuntime,
)


@dataclass
class Notification:
    method: str
    payload: dict[str, Any]


class Harness:
    def run(
        self,
        input: str,
        *,
        session_id: str,
        on_notification: Any,
    ) -> Any:
        del input
        on_notification(Notification("status.changed", {"status": "running"}))
        on_notification(
            Notification(
                "session.event",
                {
                    "sessionId": session_id,
                    "event": {"seq": 1, "type": "turn/start", "data": {}},
                },
            )
        )
        on_notification(
            Notification(
                "session.event",
                {
                    "sessionId": "another-session",
                    "event": {"seq": 2, "type": "turn/end", "data": {}},
                },
            )
        )
        return SimpleNamespace(
            session_id=session_id,
            final_response="done",
            finish_reason="completed",
            events=(),
        )

    def close(self) -> None:
        return None


async def test_runtime_streams_all_notifications_but_only_current_session_events() -> None:
    runtime = _OfficialDshRuntime(Harness())
    notifications: list[tuple[str, dict[str, Any]]] = []
    events: list[dict[str, Any]] = []

    async def on_notification(method: str, payload: dict[str, Any]) -> None:
        notifications.append((method, payload))

    async def on_event(event: dict[str, Any]) -> None:
        events.append(event)

    await runtime.run(
        "hello",
        session_id="session-1",
        on_event=on_event,
        on_notification=on_notification,
    )

    assert [method for method, _ in notifications] == [
        "status.changed",
        "session.event",
        "session.event",
    ]
    assert [event["type"] for event in events] == ["turn/start"]


async def test_runtime_cancellation_closes_harness_to_stop_background_run() -> None:
    class BlockingHarness:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.stopped = threading.Event()
            self.closed = False

        def run(
            self,
            input: str,
            *,
            session_id: str,
            on_notification: Any,
        ) -> Any:
            del input, session_id, on_notification
            self.started.set()
            self.stopped.wait(timeout=1)
            return SimpleNamespace(
                session_id="session-1",
                final_response="",
                finish_reason="completed",
                events=(),
            )

        def close(self) -> None:
            self.closed = True
            self.stopped.set()

    harness = BlockingHarness()
    runtime = _OfficialDshRuntime(harness)
    running = asyncio.create_task(runtime.run("hello", session_id="session-1"))
    assert await asyncio.to_thread(harness.started.wait, 0.5)

    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert harness.closed is True
    assert harness.stopped.is_set()


async def test_runtime_executes_workflow_through_host_rpc_and_streams_notifications() -> None:
    captured: dict[str, Any] = {}

    class Client:
        def request(
            self,
            method: str,
            params: dict[str, Any],
            *,
            response_model: Any,
            timeout_seconds: float,
            on_notification: Any,
        ) -> Any:
            captured.update(
                method=method,
                params=params,
                response_model=response_model,
                timeout_seconds=timeout_seconds,
            )
            on_notification(Notification("workflow.started", {"id": "workflow-1"}))
            return response_model.model_validate(
                {
                    "runId": "workflow-1",
                    "stopReason": "completed",
                    "agentsStarted": 2,
                    "value": {"answer": 391},
                }
            )

    class WorkflowHarness:
        def __init__(self) -> None:
            self.client = Client()
            self.config = SimpleNamespace(request_timeout_seconds=123.0)
            self.started = False

        def start(self) -> None:
            self.started = True

        def close(self) -> None:
            pass

    harness = WorkflowHarness()
    runtime = _OfficialDshRuntime(harness)
    notifications: list[tuple[str, dict[str, Any]]] = []

    async def on_notification(method: str, payload: dict[str, Any]) -> None:
        notifications.append((method, payload))

    result = await runtime.execute_workflow(
        DshWorkflowRequest(
            workflow_id="parallel-facts",
            workflow_version="1.0.0",
            script="return 391",
            script_sha256="a" * 64,
            args={"steps": [{"stepId": "calculate"}]},
            args_sha256=(
                "377c15be2ec3d963b07f4dd8872e7d979f6aaf89cc76e095c155cff5e0c45fb9"
            ),
        ),
        session_id="session-workflow-1",
        on_notification=on_notification,
    )

    assert harness.started is True
    assert captured["method"] == "base-agent/workflow.execute"
    assert captured["params"]["sessionId"] == "session-workflow-1"
    assert captured["params"]["scriptSha256"] == "a" * 64
    assert captured["params"]["argsSha256"] == (
        "377c15be2ec3d963b07f4dd8872e7d979f6aaf89cc76e095c155cff5e0c45fb9"
    )
    assert captured["timeout_seconds"] == 123.0
    assert notifications == [("workflow.started", {"id": "workflow-1"})]
    assert result.workflow_run_id == "workflow-1"
    assert result.value == {"answer": 391}
