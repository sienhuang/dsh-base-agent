from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from dsh_base_agent.adapters.dsh.runtime import _OfficialDshRuntime


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
