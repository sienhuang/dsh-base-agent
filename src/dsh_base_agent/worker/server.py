"""Environment-driven entrypoint for the independent execution Worker."""

from __future__ import annotations

import asyncio
import json
import os
import signal
from pathlib import Path
from typing import Any

from dsh_base_agent.adapters.dsh.runtime import RuntimeConfig
from dsh_base_agent.adapters.kafka import (
    KafkaNotificationConfig,
    create_notification_publisher,
)
from dsh_base_agent.artifacts import ArtifactStoreConfig
from dsh_base_agent.control.plane import ControlPlane
from dsh_base_agent.sdk.agent import Agent
from dsh_base_agent.store import ControlStoreConfig, PostgresControlStore
from dsh_base_agent.worker.service import WorkerConfig, WorkerService


def build_worker_from_env() -> WorkerService:
    workspace = Path(os.environ.get("DSH_BASE_AGENT_WORKSPACE", ".")).resolve()
    store = ControlStoreConfig.from_env().create(workspace=workspace)
    if not isinstance(store, PostgresControlStore):
        raise RuntimeError("the independent Worker requires DSH_BASE_AGENT_DATABASE_URL")
    control = ControlPlane(
        workspace=workspace,
        runtime=RuntimeConfig.from_env(),
        store=store,
        artifact_store_config=ArtifactStoreConfig.from_env(),
        notification_publisher=create_notification_publisher(KafkaNotificationConfig.from_env()),
        auto_execute=False,
    )
    definitions = os.environ.get("DSH_BASE_AGENT_DEFINITIONS")
    if definitions:
        payload: Any = json.loads(Path(definitions).read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("DSH_BASE_AGENT_DEFINITIONS must contain a JSON array")
        for item in payload:
            if not isinstance(item, dict):
                raise ValueError("each Agent definition must be an object")
            control.register(
                Agent(
                    name=str(item["name"]),
                    version=str(item.get("version", "1.0.0")),
                    prompt=str(item["prompt"]),
                    skills=tuple(str(value) for value in item.get("skills", [])),
                )
            )
    return WorkerService(control, WorkerConfig.from_env())


async def _serve() -> None:
    worker = build_worker_from_env()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for item in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(item, stop.set)
    running = asyncio.create_task(worker.run_forever(), name="dsh-base-agent-worker")
    stopping = asyncio.create_task(stop.wait(), name="dsh-base-agent-worker-signal")
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
    asyncio.run(_serve())


__all__ = ["build_worker_from_env", "main"]
