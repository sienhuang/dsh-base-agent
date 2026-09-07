"""Request authentication at the stable HTTP API boundary."""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlsplit

import httpx
from dotenv import dotenv_values

from dsh_base_agent.control.auth import Principal

_NICK = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}")
_MAX_CREDENTIAL_BYTES = 8 * 1024
_MAX_RESPONSE_BYTES = 64 * 1024


class RequestAuthenticationError(RuntimeError):
    """Base error translated to a stable HTTP authentication response."""

    status_code: int
    code: str
    retryable: bool

    def __init__(self, message: str, *, status_code: int, code: str, retryable: bool) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.retryable = retryable


class Unauthenticated(RequestAuthenticationError):
    def __init__(self, message: str = "A valid caller credential is required") -> None:
        super().__init__(
            message,
            status_code=401,
            code="UNAUTHENTICATED",
            retryable=False,
        )


class AuthenticationProviderUnavailable(RequestAuthenticationError):
    def __init__(
        self,
        message: str = "The authentication provider is temporarily unavailable",
    ) -> None:
        super().__init__(
            message,
            status_code=503,
            code="AUTH_PROVIDER_UNAVAILABLE",
            retryable=True,
        )


@dataclass(frozen=True, slots=True)
class RequestCredentials:
    """Untrusted credentials extracted from one HTTP request."""

    tenant_id: str | None = None
    principal_id: str | None = None
    moa_token: str | None = None
    authorization: str | None = None


class RequestAuthenticator(Protocol):
    async def authenticate(self, credentials: RequestCredentials) -> Principal: ...

    async def close(self) -> None: ...


class HeaderRequestAuthenticator:
    """Explicit development mode retaining the original identity headers."""

    async def authenticate(self, credentials: RequestCredentials) -> Principal:
        tenant_id = _bounded_header(credentials.tenant_id)
        principal_id = _bounded_header(credentials.principal_id)
        if tenant_id is None or principal_id is None:
            raise Unauthenticated("X-Tenant-ID and X-Principal-ID are required")
        return Principal(tenant_id=tenant_id, principal_id=principal_id)

    async def close(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class ApiAuthenticationConfig:
    """Select development headers or trusted MOA authentication for the API."""

    enabled: bool = False
    tenant_id: str = ""
    moa_auth_url: str = "https://login.moa.moonton.net"
    moa_project_id: int = 0
    connect_timeout_seconds: float = 0.5
    request_timeout_seconds: float = 1.0
    max_in_flight: int = 32

    def __post_init__(self) -> None:
        if not self.enabled:
            return
        if not self.tenant_id.strip():
            raise ValueError(
                "DSH_BASE_AGENT_AUTH_TENANT_ID is required when authentication is enabled"
            )
        if self.moa_project_id < 1:
            raise ValueError(
                "DSH_BASE_AGENT_MOA_PROJECT_ID must be positive when authentication is enabled"
            )
        _validate_auth_url(self.moa_auth_url)
        if self.connect_timeout_seconds <= 0 or self.request_timeout_seconds <= 0:
            raise ValueError("MOA authentication timeouts must be positive")
        if self.max_in_flight < 1 or self.max_in_flight > 10_000:
            raise ValueError("MOA authentication max_in_flight must be between 1 and 10000")

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "DSH_BASE_AGENT_",
        env_file: str | Path | None = ".env",
    ) -> ApiAuthenticationConfig:
        file_values = {} if env_file is None else dotenv_values(dotenv_path=env_file)
        values = {key: value for key, value in file_values.items() if value is not None}
        values.update(os.environ)
        enabled = _boolean(values.get(f"{prefix}AUTH_ENABLED"), default=False)
        if not enabled:
            return cls(enabled=False)
        return cls(
            enabled=True,
            tenant_id=values.get(f"{prefix}AUTH_TENANT_ID", ""),
            moa_auth_url=values.get(
                f"{prefix}MOA_AUTH_URL", "https://login.moa.moonton.net"
            ),
            moa_project_id=int(values.get(f"{prefix}MOA_PROJECT_ID", "0")),
            connect_timeout_seconds=float(
                values.get(f"{prefix}MOA_CONNECT_TIMEOUT_SECONDS", "0.5")
            ),
            request_timeout_seconds=float(
                values.get(f"{prefix}MOA_REQUEST_TIMEOUT_SECONDS", "1")
            ),
            max_in_flight=int(values.get(f"{prefix}MOA_MAX_IN_FLIGHT", "32")),
        )

    def create(self) -> RequestAuthenticator:
        if not self.enabled:
            return HeaderRequestAuthenticator()
        return MoaRequestAuthenticator(self)


class MoaRequestAuthenticator:
    """Validate an X-MOA-Token and derive a trusted company Principal."""

    def __init__(
        self,
        config: ApiAuthenticationConfig,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not config.enabled:
            raise ValueError("MoaRequestAuthenticator requires authentication to be enabled")
        self.config = config
        self._check_token_url = config.moa_auth_url.rstrip("/") + "/api/checktoken"
        self._in_flight = asyncio.Semaphore(config.max_in_flight)
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                config.request_timeout_seconds,
                connect=config.connect_timeout_seconds,
            ),
            follow_redirects=False,
        )

    async def authenticate(self, credentials: RequestCredentials) -> Principal:
        if _normalized(credentials.authorization) is not None:
            raise Unauthenticated("Authorization and X-MOA-Token cannot be combined")
        token = _valid_credential(credentials.moa_token)
        if token is None:
            raise Unauthenticated("X-MOA-Token is required")
        if self._in_flight.locked():
            raise AuthenticationProviderUnavailable()
        async with self._in_flight:
            payload = await self._verify(token)
        result = payload.get("result")
        if not isinstance(result, dict):
            raise AuthenticationProviderUnavailable()
        uid = _bounded_text(result.get("uid"), max_length=256)
        nick = _canonical_nick(result.get("nick"))
        project = result.get("project")
        if (
            uid is None
            or nick is None
            or type(project) is not int
            or project != self.config.moa_project_id
        ):
            raise Unauthenticated()
        return Principal(
            tenant_id=self.config.tenant_id.strip(),
            principal_id=f"user:{nick}",
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _verify(self, token: str) -> dict[str, object]:
        request = self._client.build_request(
            "POST",
            self._check_token_url,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json={"token": token, "project_id": self.config.moa_project_id},
        )
        response: httpx.Response | None = None
        try:
            async with asyncio.timeout(self.config.request_timeout_seconds):
                response = await self._client.send(request, stream=True)
                body = await _bounded_response_body(response)
        except TimeoutError as exc:
            raise AuthenticationProviderUnavailable() from exc
        except httpx.HTTPError as exc:
            raise AuthenticationProviderUnavailable() from exc
        finally:
            if response is not None:
                await response.aclose()

        if response.status_code == 429 or response.status_code >= 500:
            raise AuthenticationProviderUnavailable()
        if response.status_code < 200 or response.status_code >= 300:
            raise Unauthenticated()
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthenticationProviderUnavailable() from exc
        if not isinstance(payload, dict) or type(payload.get("code")) is not int:
            raise AuthenticationProviderUnavailable()
        if payload["code"] != 0:
            raise Unauthenticated()
        return payload


async def _bounded_response_body(response: httpx.Response) -> bytes:
    body = bytearray()
    async for chunk in response.aiter_bytes():
        if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
            raise AuthenticationProviderUnavailable()
        body.extend(chunk)
    return bytes(body)


def _valid_credential(value: str | None) -> str | None:
    normalized = _normalized(value)
    if normalized is None or len(normalized.encode("utf-8")) > _MAX_CREDENTIAL_BYTES:
        return None
    return None if _contains_control(normalized) else normalized


def _bounded_header(value: str | None) -> str | None:
    normalized = _normalized(value)
    if normalized is None or len(normalized) > 256 or _contains_control(normalized):
        return None
    return normalized


def _bounded_text(value: object, *, max_length: int) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized or len(normalized) > max_length or _contains_control(normalized):
        return None
    return normalized


def _canonical_nick(value: object) -> str | None:
    nick = _bounded_text(value, max_length=128)
    if nick is None:
        return None
    canonical = nick.lower()
    return canonical if _NICK.fullmatch(canonical) is not None else None


def _normalized(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _contains_control(value: str) -> bool:
    return any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)


def _validate_auth_url(value: str) -> None:
    try:
        parsed = urlsplit(value)
    except ValueError as exc:
        raise ValueError("DSH_BASE_AGENT_MOA_AUTH_URL is invalid") from exc
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    secure = parsed.scheme == "https"
    loopback_http = parsed.scheme == "http" and loopback
    if parsed.hostname is None or not (secure or loopback_http):
        raise ValueError(
            "DSH_BASE_AGENT_MOA_AUTH_URL must use HTTPS; HTTP is only allowed for loopback tests"
        )


def _boolean(value: str | None, *, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError("DSH_BASE_AGENT_AUTH_ENABLED must be a boolean")


__all__ = [
    "ApiAuthenticationConfig",
    "AuthenticationProviderUnavailable",
    "HeaderRequestAuthenticator",
    "MoaRequestAuthenticator",
    "RequestAuthenticationError",
    "RequestAuthenticator",
    "RequestCredentials",
    "Unauthenticated",
]
