from __future__ import annotations

import os

import pytest

from dsh_base_agent import RuntimeConfig


def test_runtime_config_reads_dotenv_without_mutating_environment(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / "runtime.env"
    env_file.write_text(
        "\n".join(
            (
                "DSH_PROVIDER=custom-provider",
                "DSH_MODEL=glm-from-file",
                "DSH_HOME=state/dsh-home",
                "DSH_BASE_URL=http://localhost:9000",
                "DSH_API_KEY=file-secret",
                "DSH_MAX_TOKENS=8192",
                "DSH_REQUEST_TIMEOUT_SECONDS=12.5",
                "DSH_INITIALIZE_TIMEOUT_SECONDS=7",
                "DSH_SHUTDOWN_TIMEOUT_SECONDS=2",
                "DSH_BIN=/opt/dsh/bin/dsh",
            )
        ),
        encoding="utf-8",
    )
    for name in (
        "DSH_PROVIDER",
        "DSH_MODEL",
        "DSH_HOME",
        "DSH_BASE_URL",
        "DSH_API_KEY",
        "DSH_MAX_TOKENS",
        "DSH_REQUEST_TIMEOUT_SECONDS",
        "DSH_INITIALIZE_TIMEOUT_SECONDS",
        "DSH_SHUTDOWN_TIMEOUT_SECONDS",
        "DSH_BIN",
    ):
        monkeypatch.delenv(name, raising=False)

    config = RuntimeConfig.from_env(env_file=env_file)

    assert config.provider == "custom-provider"
    assert config.model == "glm-from-file"
    assert str(config.dsh_home) == "state/dsh-home"
    assert config.base_url == "http://localhost:9000"
    assert config.api_key == "file-secret"
    assert config.max_tokens == 8192
    assert config.request_timeout_seconds == 12.5
    assert config.initialize_timeout_seconds == 7.0
    assert config.shutdown_timeout_seconds == 2.0
    assert config.dsh_bin == "/opt/dsh/bin/dsh"
    assert "DSH_MODEL" not in os.environ


def test_process_environment_overrides_dotenv(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "DSH_MODEL=model-from-file\nDSH_API_KEY=key-from-file\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("DSH_MODEL", "model-from-process")
    monkeypatch.setenv("DSH_API_KEY", "key-from-process")

    config = RuntimeConfig.from_env(env_file=env_file)

    assert config.model == "model-from-process"
    assert config.api_key == "key-from-process"


def test_runtime_config_reads_dotenv_from_current_directory(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("DSH_MODEL=glm-default-file\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    config = RuntimeConfig.from_env()

    assert config.model == "glm-default-file"


def test_runtime_config_can_disable_dotenv(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("DSH_MODEL=ignored\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="DSH_MODEL is required"):
        RuntimeConfig.from_env(env_file=None)
