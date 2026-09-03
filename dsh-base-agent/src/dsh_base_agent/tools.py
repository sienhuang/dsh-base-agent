"""Typed Python Tools compiled into MCP schemas and governed executions."""

from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, cast, get_type_hints, overload, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, create_model

_TOOL_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


class SideEffect(StrEnum):
    READ_ONLY = "read_only"
    IDEMPOTENT = "idempotent"
    UNSAFE = "unsafe"


class ToolSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    input_schema: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ToolContext:
    tenant_id: str
    principal_id: str
    agent_id: str
    run_id: str
    attempt_id: str
    tool_name: str
    tool_call_id: str
    operation_id: str

    @property
    def idempotency_key(self) -> str:
        return self.operation_id


@dataclass(frozen=True, slots=True)
class ToolReadiness:
    ready: bool
    detail: str = ""


@runtime_checkable
class Tool(Protocol):
    @property
    def spec(self) -> ToolSpec: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def timeout_seconds(self) -> float: ...

    @property
    def side_effect(self) -> SideEffect: ...

    @property
    def confirmation_required(self) -> bool: ...

    def validate_arguments(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]: ...

    async def check_readiness(self) -> ToolReadiness: ...

    async def invoke(self, arguments: Mapping[str, Any], context: ToolContext) -> Any: ...


type ReadinessCheck = Callable[[], bool | ToolReadiness | Awaitable[bool | ToolReadiness]]


class FunctionTool:
    """A normal Python callable presented to DSH as a typed MCP Tool."""

    def __init__(
        self,
        function: Callable[..., Any],
        *,
        name: str | None = None,
        description: str | None = None,
        permissions: frozenset[str] = frozenset(),
        timeout_seconds: float = 30.0,
        side_effect: SideEffect = SideEffect.READ_ONLY,
        confirmation_required: bool = False,
        readiness: ReadinessCheck | None = None,
    ) -> None:
        tool_name = name or function.__name__
        if not _TOOL_NAME.fullmatch(tool_name):
            raise ValueError("tool name must contain only letters, numbers, '.', '_' or '-'")
        if timeout_seconds <= 0:
            raise ValueError("tool timeout_seconds must be greater than zero")
        if confirmation_required and side_effect is SideEffect.READ_ONLY:
            raise ValueError("a read-only Tool must not require confirmation")
        arguments_model, context_parameter = _arguments_model(function)
        self._function = function
        self._arguments_model = arguments_model
        self._context_parameter = context_parameter
        self._spec = ToolSpec(
            name=tool_name,
            description=description or inspect.getdoc(function) or f"Execute {tool_name}.",
            input_schema=arguments_model.model_json_schema(),
        )
        self._permissions = permissions
        self._timeout_seconds = timeout_seconds
        self._side_effect = side_effect
        self._confirmation_required = confirmation_required
        self._readiness = readiness

    @property
    def spec(self) -> ToolSpec:
        return self._spec

    @property
    def permissions(self) -> frozenset[str]:
        return self._permissions

    @property
    def timeout_seconds(self) -> float:
        return self._timeout_seconds

    @property
    def side_effect(self) -> SideEffect:
        return self._side_effect

    @property
    def confirmation_required(self) -> bool:
        return self._confirmation_required

    def validate_arguments(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._arguments_model.model_validate(dict(arguments)).model_dump()

    async def check_readiness(self) -> ToolReadiness:
        if self._readiness is None:
            return ToolReadiness(True)
        value = self._readiness()
        if inspect.isawaitable(value):
            value = await value
        return value if isinstance(value, ToolReadiness) else ToolReadiness(bool(value))

    async def invoke(self, arguments: Mapping[str, Any], context: ToolContext) -> Any:
        keywords = dict(self.validate_arguments(arguments))
        if self._context_parameter is not None:
            keywords[self._context_parameter] = context
        if inspect.iscoroutinefunction(self._function):
            return await self._function(**keywords)
        return await asyncio.to_thread(self._function, **keywords)


@overload
def tool(function: Callable[..., Any], /) -> FunctionTool: ...


@overload
def tool(
    function: None = None,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    permissions: tuple[str, ...] = (),
    timeout_seconds: float = 30.0,
    side_effect: bool | SideEffect = False,
    confirmation_required: bool = False,
    readiness: ReadinessCheck | None = None,
) -> Callable[[Callable[..., Any]], FunctionTool]: ...


def tool(
    function: Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    description: str | None = None,
    permissions: tuple[str, ...] = (),
    timeout_seconds: float = 30.0,
    side_effect: bool | SideEffect = False,
    confirmation_required: bool = False,
    readiness: ReadinessCheck | None = None,
) -> FunctionTool | Callable[[Callable[..., Any]], FunctionTool]:
    """Compile a Python callable into a governed Tool.

    ``side_effect=False`` is read-only and ``True`` is unsafe. Use
    ``SideEffect.IDEMPOTENT`` when the downstream service honors the supplied
    ``ToolContext.idempotency_key``.
    """

    resolved_side_effect = (
        side_effect
        if isinstance(side_effect, SideEffect)
        else SideEffect.UNSAFE if side_effect else SideEffect.READ_ONLY
    )

    def decorate(candidate: Callable[..., Any]) -> FunctionTool:
        return FunctionTool(
            candidate,
            name=name,
            description=description,
            permissions=frozenset(permissions),
            timeout_seconds=timeout_seconds,
            side_effect=resolved_side_effect,
            confirmation_required=confirmation_required,
            readiness=readiness,
        )

    return decorate(function) if function is not None else decorate


def _arguments_model(function: Callable[..., Any]) -> tuple[type[BaseModel], str | None]:
    fields: dict[str, tuple[Any, Any]] = {}
    context_parameter: str | None = None
    hints = get_type_hints(function)
    for parameter in inspect.signature(function).parameters.values():
        if parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD):
            raise TypeError("tool functions cannot declare *args or **kwargs")
        if parameter.kind is parameter.POSITIONAL_ONLY:
            raise TypeError("tool functions cannot declare positional-only parameters")
        annotation = hints.get(parameter.name, parameter.annotation)
        if annotation is ToolContext:
            if context_parameter is not None:
                raise TypeError("tool functions may accept only one ToolContext")
            context_parameter = parameter.name
            continue
        if annotation is inspect.Signature.empty:
            raise TypeError(f"tool parameter '{parameter.name}' requires a type annotation")
        default = ... if parameter.default is inspect.Signature.empty else parameter.default
        fields[parameter.name] = (annotation, default)
    config = ConfigDict(extra="forbid")
    model = create_model(
        f"{function.__name__.title()}Arguments",
        __config__=config,
        **cast(dict[str, Any], fields),
    )
    return cast(type[BaseModel], model), context_parameter


__all__ = [
    "FunctionTool",
    "SideEffect",
    "Tool",
    "ToolContext",
    "ToolReadiness",
    "ToolSpec",
    "tool",
]
