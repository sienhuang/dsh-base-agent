"""Environment-driven ArtifactStore configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from dotenv import dotenv_values

from dsh_base_agent.artifacts.store import ArtifactStore, LocalArtifactStore


@dataclass(frozen=True, slots=True)
class ArtifactStoreConfig:
    """Select the artifact body backend independently from ControlStore."""

    backend: Literal["local"] = "local"
    local_root: Path | None = None
    max_object_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.max_object_bytes <= 0:
            raise ValueError("ArtifactStore max_object_bytes must be positive")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "DSH_BASE_AGENT_ARTIFACT_",
        env_file: str | Path | None = ".env",
    ) -> ArtifactStoreConfig:
        file_values = {} if env_file is None else dotenv_values(dotenv_path=env_file)
        values = {key: value for key, value in file_values.items() if value is not None}
        values.update(os.environ)
        backend = values.get(f"{prefix}BACKEND", "local").strip().lower()
        if backend != "local":
            raise ValueError(f"unsupported ArtifactStore backend: {backend}")
        raw_root = values.get(f"{prefix}LOCAL_ROOT")
        return cls(
            backend="local",
            local_root=Path(raw_root).expanduser() if raw_root else None,
            max_object_bytes=int(values.get(f"{prefix}MAX_OBJECT_BYTES", str(64 * 1024 * 1024))),
        )

    def create(self, *, workspace: str | Path) -> ArtifactStore:
        workspace_path = Path(workspace).expanduser().resolve()
        configured = self.local_root
        if configured is None:
            root = workspace_path / ".dsh-base-agent" / "artifacts"
        elif configured.is_absolute():
            root = configured
        else:
            root = workspace_path / configured
        return LocalArtifactStore(root, max_object_bytes=self.max_object_bytes)


__all__ = ["ArtifactStoreConfig"]
