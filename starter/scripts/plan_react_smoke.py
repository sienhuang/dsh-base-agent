"""Exercise planning, explicit approval, and deterministic Workflow execution."""

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


def wait_for(run_id: str, statuses: set[str], timeout_seconds: float = 300) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        view = request("GET", f"/v1/runs/{run_id}")
        if view["run"]["status"] in statuses:
            return view
        time.sleep(1)
    raise TimeoutError(f"Run {run_id} did not reach {sorted(statuses)}")


def main() -> None:
    submitted = request(
        "POST",
        "/v1/runs",
        {
            "agent_id": "iris-plan-react-1",
            "input": "查询 order-001，并给出订单状态和物流状态。",
            "metadata": {"smoke_test": "plan-react"},
        },
    )
    run_id = submitted["run_id"]
    print(f"submitted: {run_id}")

    waiting_view = wait_for(run_id, {"waiting", "failed", "cancelled"})
    waiting = waiting_view["run"]
    if waiting["status"] != "waiting":
        raise RuntimeError(json.dumps(waiting, ensure_ascii=False, indent=2))

    plan = request("GET", f"/v1/runs/{run_id}/plan")
    print("proposed plan:")
    print(json.dumps(plan, ensure_ascii=False, indent=2))
    action = waiting_view["required_action"]
    if action is None or action["type"] != "plan_approval":
        raise RuntimeError("waiting Plan-ReAct Run has no plan_approval RequiredAction")

    request(
        "POST",
        f"/v1/runs/{run_id}/resume",
        {
            "action_id": action["action_id"],
            "expected_revision": waiting["revision"],
            "response": {
                "decision": "approve",
                "plan_id": plan["plan_id"],
                "plan_revision": plan["revision"],
                "plan_sha256": plan["plan_sha256"],
                "comment": "starter smoke test approval",
            },
        },
    )
    finished = wait_for(run_id, {"succeeded", "failed", "cancelled"})["run"]
    print("final run:")
    print(json.dumps(finished, ensure_ascii=False, indent=2))
    if finished["status"] != "succeeded":
        raise RuntimeError(f"Plan-ReAct Run ended as {finished['status']}")


if __name__ == "__main__":
    main()
