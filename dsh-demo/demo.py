import os
from pathlib import Path

from deepseek_harness import DeepSeekHarness

root = Path(__file__).resolve().parent
workspace = (root / "workspace").resolve()
dsh_home = (root / "dsh-home").resolve()

with DeepSeekHarness(
    dsh_home=str(dsh_home),
    cwd=str(workspace),
    profile="sdk-minimal",
    provider="deepseek-official",
    model=os.environ["DSH_MODEL"],
    max_tokens=4096,
) as harness:
    result = harness.run(
            (
                "读取当前 workspace 中的 note.txt，"
                "然后创建 result.md，按照 note.txt 的要求作答。"
                "不要修改其他文件。完成后简要告诉我做了什么。"
            ),
            session_id="demo-001",
      )
    print("finish_reason:", result.finish_reason)
    print("final_response:")
    print(result.final_response)
    print("root events:", len(result.events))
    print("notifications:", len(result.notifications))