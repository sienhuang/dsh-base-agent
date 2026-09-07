"""Starter entrypoint for the independent PostgreSQL execution Worker."""

from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

from dsh_base_agent import (
    ArtifactStoreConfig,
    ControlPlane,
    ControlStoreConfig,
    KafkaNotificationConfig,
    PostgresControlStore,
    RuntimeConfig,
    WorkerConfig,
    WorkerService,
    create_notification_publisher,
)

from company_agent.authorization import StarterAuthorizer
from company_agent.definition import build_agent


def build_worker(*, project_root: str | Path | None = None) -> WorkerService:
    root = Path(project_root or Path.cwd()).expanduser().resolve()
    workspace = root / "workspace"
    store = ControlStoreConfig.from_env(env_file=root / ".env").create(
        workspace=workspace
    )
    if not isinstance(store, PostgresControlStore):
        raise RuntimeError("company-agent-worker requires DSH_BASE_AGENT_DATABASE_URL")
    control = ControlPlane(
        workspace=workspace,
        runtime=RuntimeConfig.from_env(env_file=root / ".env"),
        store=store,
        artifact_store_config=ArtifactStoreConfig.from_env(env_file=root / ".env"),
        notification_publisher=create_notification_publisher(
            KafkaNotificationConfig.from_env(env_file=root / ".env")
        ),
        authorizer=StarterAuthorizer(),
        auto_execute=False,
    )
    control.register(build_agent())
    return WorkerService(
        control,
        WorkerConfig.from_env(env_file=root / ".env"),
    )


async def _serve(root: Path) -> None:
    worker = build_worker(project_root=root)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for item in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(item, stop.set)
    running = asyncio.create_task(worker.run_forever(), name="company-agent-worker")
    stopping = asyncio.create_task(stop.wait(), name="company-agent-worker-signal")
    try:
        done, _ = await asyncio.wait(
            (running, stopping),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if running in done:
            await running
    finally:
        stopping.cancel()
        await worker.close()
        await asyncio.gather(running, stopping, return_exceptions=True)


def main() -> None:
    root = Path(os.environ.get("STARTER_ROOT", Path.cwd())).expanduser().resolve()
    asyncio.run(_serve(root))


__all__ = ["build_worker", "main"]
