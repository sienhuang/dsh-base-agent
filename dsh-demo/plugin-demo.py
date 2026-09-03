import os
from pathlib import Path

from deepseek_harness import DeepSeekHarness


def show_notification(notification):
    if notification.method == "session.status":
        status = notification.payload.get("status")
        print(f"[status] {status}", flush=True)
        return

    if notification.method == "session.event":
        event = notification.payload.get("event")
        if not isinstance(event, dict):
            return

        event_type = event.get("type")
        if isinstance(event_type, str) and (
            event_type.startswith("turn/")
            or event_type.startswith("tool/")
        ):
            print(f"[event] {event_type}", flush=True)


root = Path(__file__).resolve().parent

with DeepSeekHarness(
    dsh_home=str((root / "dsh-home").resolve()),
    cwd=str((root / "workspace").resolve()),
    profile="sdk-minimal",
    provider="deepseek-official",
    model=os.environ["DSH_MODEL"],
    max_tokens=4096,
    request_timeout_seconds=180,
) as harness:
    result = harness.run(
        (
            '必须调用名为 greet 的工具，参数 name 使用 "Iris"。'
            "不要通过 Bash 或普通文本模拟工具调用。"
            "调用完成后，原样返回工具给出的问候语。"
        ),
        session_id="plugin-demo-001",
        on_notification=show_notification,
    )

print("finish_reason:", result.finish_reason)
print("final_response:", result.final_response)