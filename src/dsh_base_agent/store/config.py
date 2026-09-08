"""Environment-driven selection of the control-plane persistence backend."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values

from dsh_base_agent.store.sqlite import ControlStore, SqliteControlStore


@dataclass(frozen=True, slots=True)
class ControlStoreConfig:
    """Configuration kept separate from DSH's ``RuntimeConfig``."""

    database_url: str | None = field(default=None, repr=False)
    postgres_schema: str = "dsh_base_agent"
    postgres_pool_min_size: int = 1
    postgres_pool_max_size: int = 10
    postgres_command_timeout_seconds: float = 30.0
    postgres_statement_cache_size: int = 100

    def __post_init__(self) -> None:
        if self.database_url is not None and not self.database_url.startswith(
            ("postgresql://", "postgres://")
        ):
            raise ValueError("DSH_BASE_AGENT_DATABASE_URL must be a PostgreSQL URL")
        if self.postgres_pool_min_size < 0:
            raise ValueError("postgres_pool_min_size cannot be negative")
        if self.postgres_pool_max_size < 1:
            raise ValueError("postgres_pool_max_size must be positive")
        if self.postgres_pool_min_size > self.postgres_pool_max_size:
            raise ValueError("postgres_pool_min_size cannot exceed postgres_pool_max_size")
        if self.postgres_command_timeout_seconds <= 0:
            raise ValueError("postgres_command_timeout_seconds must be positive")
        if self.postgres_statement_cache_size < 0:
            raise ValueError("postgres_statement_cache_size cannot be negative")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "DSH_BASE_AGENT_",
        env_file: str | Path | None = ".env",
    ) -> ControlStoreConfig:
        """Read database configuration without mutating ``os.environ``."""

        file_values = {} if env_file is None else dotenv_values(dotenv_path=env_file)
        values = {key: value for key, value in file_values.items() if value is not None}
        values.update(os.environ)
        return cls(
            database_url=values.get(f"{prefix}DATABASE_URL") or None,
            postgres_schema=values.get(f"{prefix}DATABASE_SCHEMA", "dsh_base_agent"),
            postgres_pool_min_size=int(values.get(f"{prefix}DATABASE_POOL_MIN_SIZE", "1")),
            postgres_pool_max_size=int(values.get(f"{prefix}DATABASE_POOL_MAX_SIZE", "10")),
            postgres_command_timeout_seconds=float(
                values.get(f"{prefix}DATABASE_COMMAND_TIMEOUT_SECONDS", "30")
            ),
            postgres_statement_cache_size=int(
                values.get(f"{prefix}DATABASE_STATEMENT_CACHE_SIZE", "100")
            ),
        )

    def create(self, *, workspace: str | Path) -> ControlStore:
        """Create PostgreSQL when configured, otherwise retain local SQLite."""

        if self.database_url is None:
            database = Path(workspace).expanduser().resolve() / ".dsh-base-agent" / "control.db"
            return SqliteControlStore(database)

        from dsh_base_agent.store.postgres import PostgresControlStore

        return PostgresControlStore(
            self.database_url,
            schema=self.postgres_schema,
            min_pool_size=self.postgres_pool_min_size,
            max_pool_size=self.postgres_pool_max_size,
            command_timeout_seconds=self.postgres_command_timeout_seconds,
            statement_cache_size=self.postgres_statement_cache_size,
        )


__all__ = ["ControlStoreConfig"]
