"""Read-only Memory retrieval extension points for pre-step context."""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, get_type_hints, overload, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

_PROVIDER_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


class MemoryItem(BaseModel):
    """One bounded piece of externally managed Memory."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    memory_id: str = Field(min_length=1, max_length=256)
    content: str = Field(min_length=1, max_length=16_384)
    source: str | None = Field(default=None, max_length=256)
    score: float | None = None


@dataclass(frozen=True, slots=True)
class MemorySearchRequest:
    """Trusted identity and Turn data supplied by the ControlPlane."""

    tenant_id: str
    principal_id: str
    agent_id: str
    conversation_id: str | None
    run_id: str
    attempt_id: str
    dsh_session_id: str
    query: str


@dataclass(frozen=True, slots=True)
class MemoryReadiness:
    ready: bool
    detail: str = ""


@runtime_checkable
class MemoryProvider(Protocol):
    """A read-only adapter over an application-owned Memory search service."""

    @property
    def name(self) -> str: ...

    @property
    def version(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def timeout_seconds(self) -> float: ...

    @property
    def max_results(self) -> int: ...

    async def check_readiness(self) -> MemoryReadiness: ...

    async def search(self, request: MemorySearchRequest) -> tuple[MemoryItem, ...]: ...


type MemorySearchFunction = Callable[
    [MemorySearchRequest],
    Sequence[MemoryItem | Mapping[str, Any]]
    | Awaitable[Sequence[MemoryItem | Mapping[str, Any]]],
]
type MemoryReadinessCheck = Callable[
    [], bool | MemoryReadiness | Awaitable[bool | MemoryReadiness]
]


class FunctionMemoryProvider:
    """Compile one Python search function into a bounded Memory Provider."""

    def __init__(
        self,
        function: MemorySearchFunction,
        *,
        name: str | None = None,
        version: str = "1.0.0",
        permissions: frozenset[str] = frozenset(),
        timeout_seconds: float = 5.0,
        max_results: int = 3,
        readiness: MemoryReadinessCheck | None = None,
    ) -> None:
        provider_name = name or function.__name__
        if not _PROVIDER_NAME.fullmatch(provider_name):
            raise ValueError(
                "Memory Provider name must contain only letters, numbers, '.', '_' or '-'"
            )
        if not version.strip():
            raise ValueError("Memory Provider version must not be blank")
        if timeout_seconds <= 0:
            raise ValueError("Memory Provider timeout_seconds must be greater than zero")
        if not 1 <= max_results <= 20:
            raise ValueError("Memory Provider max_results must be between 1 and 20")
        _validate_signature(function)
        self._function = function
        self._name = provider_name
        self._version = version
        self._permissions = permissions
        self._timeout_seconds = timeout_seconds
        self._max_results = max_results
        self._readiness = readiness

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def permissions(self) -> frozenset[str]:
        return self._permissions

    @property
    def timeout_seconds(self) -> float:
        return self._timeout_seconds

    @property
    def max_results(self) -> int:
        return self._max_results

    async def check_readiness(self) -> MemoryReadiness:
        if self._readiness is None:
            return MemoryReadiness(True)
        value = self._readiness()
        if inspect.isawaitable(value):
            value = await value
        return value if isinstance(value, MemoryReadiness) else MemoryReadiness(bool(value))

    async def search(self, request: MemorySearchRequest) -> tuple[MemoryItem, ...]:
        if inspect.iscoroutinefunction(self._function):
            result = await self._function(request)
        else:
            result = await asyncio.to_thread(self._function, request)
        if isinstance(result, (str, bytes)) or not isinstance(result, Sequence):
            raise TypeError("Memory Provider must return a sequence of MemoryItem values")
        return tuple(MemoryItem.model_validate(item) for item in result[: self.max_results])


@overload
def memory_provider(function: MemorySearchFunction, /) -> FunctionMemoryProvider: ...


@overload
def memory_provider(
    function: None = None,
    /,
    *,
    name: str | None = None,
    version: str = "1.0.0",
    permissions: tuple[str, ...] = (),
    timeout_seconds: float = 5.0,
    max_results: int = 3,
    readiness: MemoryReadinessCheck | None = None,
) -> Callable[[MemorySearchFunction], FunctionMemoryProvider]: ...


def memory_provider(
    function: MemorySearchFunction | None = None,
    /,
    *,
    name: str | None = None,
    version: str = "1.0.0",
    permissions: tuple[str, ...] = (),
    timeout_seconds: float = 5.0,
    max_results: int = 3,
    readiness: MemoryReadinessCheck | None = None,
) -> FunctionMemoryProvider | Callable[[MemorySearchFunction], FunctionMemoryProvider]:
    """Declare a read-only Memory search function used by the DSH pre-step bridge."""

    def decorate(candidate: MemorySearchFunction) -> FunctionMemoryProvider:
        return FunctionMemoryProvider(
            candidate,
            name=name,
            version=version,
            permissions=frozenset(permissions),
            timeout_seconds=timeout_seconds,
            max_results=max_results,
            readiness=readiness,
        )

    return decorate(function) if function is not None else decorate


def _validate_signature(function: MemorySearchFunction) -> None:
    parameters = tuple(inspect.signature(function).parameters.values())
    if len(parameters) != 1:
        raise TypeError("Memory Provider functions must accept one MemorySearchRequest")
    parameter = parameters[0]
    if parameter.kind not in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD):
        raise TypeError("Memory Provider request must be a positional parameter")
    annotation = get_type_hints(function).get(parameter.name, parameter.annotation)
    if annotation is not MemorySearchRequest:
        raise TypeError("Memory Provider request must be annotated as MemorySearchRequest")


__all__ = [
    "FunctionMemoryProvider",
    "MemoryItem",
    "MemoryProvider",
    "MemoryReadiness",
    "MemorySearchFunction",
    "MemorySearchRequest",
    "memory_provider",
]
