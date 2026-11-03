"""Exercise durable model-generated user input over a Conversation Run."""

from __future__ import annotations

import json
import os
import time
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BASE_URL = os.environ.get("STARTER_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
HEADERS = {
    "Content-Type": "application/json",
    "X-Tenant-ID": os.environ.get("STARTER_TENANT_ID", "demo-tenant"),
    "X-Principal-ID": os.environ.get("STARTER_PRINCIPAL_ID", "demo-user"),
}


def request(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    payload = None if body is None else json.dumps(body).encode()
    call = Request(f"{BASE_URL}{path}", data=payload, headers=HEADERS, method=method)
    try:
        with urlopen(call, timeout=30) as response:  # noqa: S310 - explicit local test target
            return json.load(response)
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"{method} {path} failed: HTTP {exc.code}: {detail}") from exc


def wait_for_view(
    run_id: str,
    statuses: set[str],
    timeout_seconds: float = 300,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        view = request("GET", f"/v1/runs/{run_id}")
        if view["run"]["status"] in statuses:
            return view
        time.sleep(1)
    raise TimeoutError(f"Run {run_id} did not reach {sorted(statuses)}")


def answer_for(question: dict[str, Any]) -> dict[str, Any]:
    options = question.get("options") or []
    if options:
        return {"id": question["id"], "selected": [options[0]["label"]]}
    return {
        "id": question["id"],
        "custom": os.environ.get("STARTER_USER_ANSWER", "普通配送"),
    }


def main() -> None:
    conversation = request(
        "POST",
        "/v1/conversations",
        {"agent_id": "iris-user-input-1", "metadata": {"smoke_test": "user-input"}},
    )
    conversation_id = conversation["conversation_id"]
    submitted = request(
        "POST",
        f"/v1/conversations/{conversation_id}/runs",
        {
            "input": (
                "请帮我安排 order-001 的配送。我还没有提供配送方式；请先通过 "
                "ask_user_question 询问我选择普通配送还是加急配送。"
            ),
            "metadata": {"smoke_test": "user-input"},
        },
    )
    run_id = submitted["run_id"]
    print(f"conversation: {conversation_id}")
    print(f"submitted: {run_id}")

    waiting = wait_for_view(run_id, {"waiting", "failed", "cancelled"})
    if waiting["run"]["status"] != "waiting" or waiting["required_action"] is None:
        raise RuntimeError(json.dumps(waiting, ensure_ascii=False, indent=2))
    action = waiting["required_action"]
    print("required action:")
    print(json.dumps(action, ensure_ascii=False, indent=2))

    request(
        "POST",
        f"/v1/runs/{run_id}/resume",
        {
            "action_id": action["action_id"],
            "expected_revision": waiting["run"]["revision"],
            "response": {
                "answers": [answer_for(question) for question in action["payload"]["questions"]]
            },
        },
    )
    finished = wait_for_view(run_id, {"succeeded", "failed", "cancelled"})
    print("final run:")
    print(json.dumps(finished, ensure_ascii=False, indent=2))
    if finished["run"]["status"] != "succeeded":
        raise RuntimeError(f"Run ended as {finished['run']['status']}")


if __name__ == "__main__":
    main()
