"""FastAPI application factory and local server entrypoint."""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn
from dsh_base_agent import (
    ArtifactStoreConfig,
    ControlPlane,
    ControlStoreConfig,
    KafkaNotificationConfig,
    RuntimeConfig,
    create_app,
    create_notification_publisher,
)
from fastapi import FastAPI

from company_agent.authorization import StarterAuthorizer
from company_agent.definition import build_agent


def build_control(
    *,
    project_root: str | Path | None = None,
    runtime: RuntimeConfig | None = None,
    store_config: ControlStoreConfig | None = None,
    artifact_store_config: ArtifactStoreConfig | None = None,
) -> ControlPlane:
    root = Path(project_root or Path.cwd()).expanduser().resolve()
    workspace = root / "workspace"
    config = runtime or RuntimeConfig.from_env(env_file=root / ".env")
    resolved_store_config = store_config or ControlStoreConfig.from_env(
        env_file=root / ".env"
    )
    auto_execute = resolved_store_config.database_url is None
    control = ControlPlane(
        workspace=workspace,
        runtime=config,
        store_config=resolved_store_config,
        artifact_store_config=artifact_store_config
        or ArtifactStoreConfig.from_env(env_file=root / ".env"),
        notification_publisher=(
            create_notification_publisher(
                KafkaNotificationConfig.from_env(env_file=root / ".env")
            )
            if auto_execute
            else None
        ),
        authorizer=StarterAuthorizer(),
        auto_execute=auto_execute,
    )
    control.register(build_agent())
    return control


def create_starter_app(
    *,
    project_root: str | Path | None = None,
    runtime: RuntimeConfig | None = None,
    store_config: ControlStoreConfig | None = None,
    artifact_store_config: ArtifactStoreConfig | None = None,
) -> FastAPI:
    return create_app(
        build_control(
            project_root=project_root,
            runtime=runtime,
            store_config=store_config,
            artifact_store_config=artifact_store_config,
        )
    )


def main() -> None:
    root = Path(os.environ.get("STARTER_ROOT", Path.cwd())).expanduser().resolve()
    uvicorn.run(
        create_starter_app(project_root=root),
        host=os.environ.get("STARTER_HOST", "127.0.0.1"),
        port=int(os.environ.get("STARTER_PORT", "8000")),
    )


__all__ = ["build_control", "create_starter_app", "main"]
