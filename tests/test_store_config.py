from __future__ import annotations

import os

import pytest

from dsh_base_agent import (
    ControlPlane,
    ControlStoreConfig,
    PostgresControlStore,
    RuntimeConfig,
    SqliteControlStore,
)


def test_store_config_defaults_to_workspace_sqlite(tmp_path) -> None:
    config = ControlStoreConfig()

    store = config.create(workspace=tmp_path)

    assert isinstance(store, SqliteControlStore)
    assert store.path == (tmp_path / ".dsh-base-agent" / "control.db").resolve()


def test_store_config_reads_postgres_dotenv_without_mutating_environment(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "DSH_BASE_AGENT_DATABASE_URL=postgresql://agent:secret@db/agent",
                "DSH_BASE_AGENT_DATABASE_SCHEMA=company_agents",
                "DSH_BASE_AGENT_DATABASE_POOL_MIN_SIZE=2",
                "DSH_BASE_AGENT_DATABASE_POOL_MAX_SIZE=12",
                "DSH_BASE_AGENT_DATABASE_COMMAND_TIMEOUT_SECONDS=15.5",
                "DSH_BASE_AGENT_DATABASE_STATEMENT_CACHE_SIZE=0",
            )
        ),
        encoding="utf-8",
    )
    for name in (
        "DSH_BASE_AGENT_DATABASE_URL",
        "DSH_BASE_AGENT_DATABASE_SCHEMA",
        "DSH_BASE_AGENT_DATABASE_POOL_MIN_SIZE",
        "DSH_BASE_AGENT_DATABASE_POOL_MAX_SIZE",
        "DSH_BASE_AGENT_DATABASE_COMMAND_TIMEOUT_SECONDS",
        "DSH_BASE_AGENT_DATABASE_STATEMENT_CACHE_SIZE",
    ):
        monkeypatch.delenv(name, raising=False)

    config = ControlStoreConfig.from_env(env_file=env_file)
    store = config.create(workspace=tmp_path)

    assert isinstance(store, PostgresControlStore)
    assert store.schema == "company_agents"
    assert store.min_pool_size == 2
    assert store.max_pool_size == 12
    assert store.command_timeout_seconds == 15.5
    assert store.statement_cache_size == 0
    assert "secret" not in repr(config)
    assert "DSH_BASE_AGENT_DATABASE_URL" not in os.environ


def test_store_config_rejects_non_postgres_database_url() -> None:
    with pytest.raises(ValueError, match="PostgreSQL URL"):
        ControlStoreConfig(database_url="mysql://db/agent")


def test_control_plane_rejects_inline_postgres_execution(tmp_path) -> None:
    store = PostgresControlStore("postgresql://agent:secret@db/agent")

    with pytest.raises(ValueError, match="requires auto_execute=False"):
        ControlPlane(
            workspace=tmp_path,
            runtime=RuntimeConfig(
                provider="test",
                model="test",
                dsh_home=tmp_path / "dsh-home",
            ),
            store=store,
            auto_execute=True,
        )
