"""Narrow asynchronous adapter around the official synchronous DSH SDK."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from dotenv import dotenv_values

from dsh_base_agent.agent import Agent
from dsh_base_agent.profile import DshProfileCompiler

type JsonObject = dict[str, Any]
type DshEventHandler = Callable[[JsonObject], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    provider: str
    model: str
    dsh_home: Path
    base_url: str | None = None
    api_key: str | None = field(default=None, repr=False)
    reasoning_effort: str | None = None
    max_tokens: int | None = 4096
    request_timeout_seconds: float | None = 300.0
    initialize_timeout_seconds: float = 30.0
    shutdown_timeout_seconds: float = 5.0
    dsh_bin: str | None = None

    def __post_init__(self) -> None:
        if not self.provider.strip() or not self.model.strip():
            raise ValueError("provider and model must not be blank")
        if self.max_tokens is not None and self.max_tokens <= 0:
            raise ValueError("max_tokens must be positive")
        if self.initialize_timeout_seconds <= 0 or self.shutdown_timeout_seconds <= 0:
            raise ValueError("runtime timeouts must be positive")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "DSH_",
        env_file: str | Path | None = ".env",
    ) -> RuntimeConfig:
        """Build configuration from a dotenv file and the process environment.

        Process environment variables take precedence over values from ``env_file``.
        Passing ``None`` disables dotenv loading. Reading a dotenv file does not mutate
        ``os.environ``.
        """
        values = _environment(env_file)
        model = values.get(f"{prefix}MODEL")
        if not model:
            raise ValueError(f"{prefix}MODEL is required")
        return cls(
            provider=values.get(f"{prefix}PROVIDER", "deepseek-official"),
            model=model,
            dsh_home=Path(values.get(f"{prefix}HOME", ".dsh-base-agent/dsh-home")),
            base_url=values.get(f"{prefix}BASE_URL") or values.get("DEEPSEEK_BASE_URL"),
            api_key=values.get(f"{prefix}API_KEY") or values.get("DEEPSEEK_API_KEY"),
            reasoning_effort=values.get(f"{prefix}REASONING_EFFORT"),
            max_tokens=_optional_int(values.get(f"{prefix}MAX_TOKENS"), default=4096),
            request_timeout_seconds=_optional_float(
                values.get(f"{prefix}REQUEST_TIMEOUT_SECONDS"), default=300.0
            ),
            initialize_timeout_seconds=_float(
                values.get(f"{prefix}INITIALIZE_TIMEOUT_SECONDS"), default=30.0
            ),
            shutdown_timeout_seconds=_float(
                values.get(f"{prefix}SHUTDOWN_TIMEOUT_SECONDS"), default=5.0
            ),
            dsh_bin=values.get(f"{prefix}BIN"),
        )


@dataclass(frozen=True, slots=True)
class DshRunResult:
    session_id: str
    final_response: str
    finish_reason: str | None
    events: tuple[JsonObject, ...] = ()


@runtime_checkable
class DshRuntime(Protocol):
    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
    ) -> DshRunResult: ...

    async def close(self) -> None: ...


class DshRuntimeFactory(Protocol):
    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
    ) -> DshRuntime: ...


class OfficialDshRuntimeFactory:
    """Create runtimes that delegate all Agent execution to DeepSeek Harness."""

    def __init__(
        self,
        config: RuntimeConfig,
        *,
        compiler: DshProfileCompiler | None = None,
    ) -> None:
        self.config = config
        self.compiler = compiler or DshProfileCompiler()

    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
    ) -> DshRuntime:
        from deepseek_harness import DeepSeekHarness  # type: ignore[import-untyped]

        resolved_home = dsh_home.expanduser().resolve()
        resolved_home.mkdir(parents=True, exist_ok=True)
        patches = self.compiler.compile(
            agent,
            dsh_home=resolved_home,
            attempt_id=attempt_id,
            tool_gateway_url=tool_gateway_url,
        )
        harness = DeepSeekHarness(
            dsh_home=str(resolved_home),
            cwd=str(workspace.expanduser().resolve()),
            profile="sdk", 
            patches=tuple(str(path.resolve()) for path in patches),
            provider=self.config.provider,
            model=self.config.model,
            reasoning_effort=self.config.reasoning_effort,
            max_tokens=self.config.max_tokens,
            base_url=self.config.base_url,
            api_key=self.config.api_key,
            dsh_bin=self.config.dsh_bin,
            initialize_timeout_seconds=self.config.initialize_timeout_seconds,
            request_timeout_seconds=self.config.request_timeout_seconds,
            shutdown_timeout_seconds=self.config.shutdown_timeout_seconds,
        )
        return _OfficialDshRuntime(harness)


class _OfficialDshRuntime:
    def __init__(self, harness: Any) -> None:
        self._harness = harness
        self._closed = False

    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
    ) -> DshRunResult:
        if self._closed:
            raise RuntimeError("DSH Runtime is closed")
        loop = asyncio.get_running_loop()
        pending: asyncio.Queue[JsonObject] = asyncio.Queue()

        def receive(notification: Any) -> None:
            if notification.method != "session.event":
                return
            payload = notification.payload
            if payload.get("sessionId") != session_id:
                return
            event = payload.get("event")
            if isinstance(event, dict):
                loop.call_soon_threadsafe(pending.put_nowait, dict(event))

        worker = asyncio.create_task(
            asyncio.to_thread(
                self._harness.run,
                input,
                session_id=session_id,
                on_notification=receive,
            )
        )
        try:
            while not worker.done():
                try:
                    event = await asyncio.wait_for(pending.get(), timeout=0.05)
                except TimeoutError:
                    continue
                if on_event is not None:
                    await on_event(event)
            result = await worker
            while not pending.empty():
                event = pending.get_nowait()
                if on_event is not None:
                    await on_event(event)
            return DshRunResult(
                session_id=result.session_id,
                final_response=result.final_response,
                finish_reason=result.finish_reason,
                events=tuple(dict(event) for event in result.events),
            )
        finally:
            if not worker.done():
                worker.cancel()
                with suppress(asyncio.CancelledError):
                    await worker

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await asyncio.to_thread(self._harness.close)


def _optional_int(value: str | None, *, default: int | None) -> int | None:
    return default if value is None else int(value)


def _optional_float(value: str | None, *, default: float | None) -> float | None:
    return default if value is None else float(value)


def _float(value: str | None, *, default: float) -> float:
    return default if value is None else float(value)


def _environment(env_file: str | Path | None) -> dict[str, str]:
    file_values = {} if env_file is None else dotenv_values(dotenv_path=env_file)
    values = {key: value for key, value in file_values.items() if value is not None}
    values.update(os.environ)
    return values


__all__ = [
    "DshEventHandler",
    "DshRunResult",
    "DshRuntime",
    "DshRuntimeFactory",
    "OfficialDshRuntimeFactory",
    "RuntimeConfig",
]
