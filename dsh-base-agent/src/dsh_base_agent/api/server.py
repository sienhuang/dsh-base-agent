"""Minimal environment-driven ASGI launcher."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI

from dsh_base_agent.adapters.dsh.runtime import RuntimeConfig
from dsh_base_agent.adapters.kafka import (
    KafkaNotificationConfig,
    create_notification_publisher,
)
from dsh_base_agent.api.app import create_app
from dsh_base_agent.artifacts import ArtifactStoreConfig
from dsh_base_agent.control.plane import ControlPlane
from dsh_base_agent.sdk.agent import Agent
from dsh_base_agent.store import ControlStoreConfig


def build_from_env() -> FastAPI:
    workspace = Path(os.environ.get("DSH_BASE_AGENT_WORKSPACE", ".")).resolve()
    store_config = ControlStoreConfig.from_env()
    auto_execute = store_config.database_url is None
    control = ControlPlane(
        workspace=workspace,
        runtime=RuntimeConfig.from_env(),
        store_config=store_config,
        artifact_store_config=ArtifactStoreConfig.from_env(),
        notification_publisher=(
            create_notification_publisher(KafkaNotificationConfig.from_env())
            if auto_execute
            else None
        ),
        auto_execute=auto_execute,
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
    return create_app(control)


def main() -> None:
    uvicorn.run(
        build_from_env(),
        host=os.environ.get("DSH_BASE_AGENT_HOST", "127.0.0.1"),
        port=int(os.environ.get("DSH_BASE_AGENT_PORT", "8000")),
    )


__all__ = ["build_from_env", "main"]
