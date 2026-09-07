from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from httpx import ASGITransport, AsyncClient, MockTransport, Response

from dsh_base_agent import Agent, ControlPlane, RuntimeConfig, create_app
from dsh_base_agent.adapters.dsh.runtime import (
    DshEventHandler,
    DshNotificationHandler,
    DshRunResult,
    DshRuntime,
)
from dsh_base_agent.api.authentication import (
    ApiAuthenticationConfig,
    AuthenticationProviderUnavailable,
    HeaderRequestAuthenticator,
    MoaRequestAuthenticator,
    RequestCredentials,
    Unauthenticated,
)
from dsh_base_agent.store import SqliteControlStore


class Runtime:
    async def run(
        self,
        input: str,
        *,
        session_id: str,
        on_event: DshEventHandler | None = None,
        on_notification: DshNotificationHandler | None = None,
    ) -> DshRunResult:
        del input, on_event, on_notification
        return DshRunResult(session_id=session_id, final_response="ok", finish_reason="completed")

    async def close(self) -> None:
        return None


class Factory:
    def create(
        self,
        agent: Agent,
        *,
        workspace: Path,
        dsh_home: Path,
        attempt_id: str,
        tool_gateway_url: str | None,
        memory_context_url: str | None = None,
        memory_context_token: str | None = None,
    ) -> DshRuntime:
        del (
            agent,
            workspace,
            dsh_home,
            attempt_id,
            tool_gateway_url,
            memory_context_url,
            memory_context_token,
        )
        return Runtime()


def _configuration(**changes: object) -> ApiAuthenticationConfig:
    values: dict[str, object] = {
        "enabled": True,
        "tenant_id": "company",
        "moa_auth_url": "https://login.moa.example",
        "moa_project_id": 1564,
        "connect_timeout_seconds": 0.5,
        "request_timeout_seconds": 1.0,
        "max_in_flight": 32,
    }
    values.update(changes)
    return ApiAuthenticationConfig(**values)  # type: ignore[arg-type]


def _moa_client(
    body: dict[str, object] | bytes,
    *,
    status_code: int = 200,
    capture: list[httpx.Request] | None = None,
) -> AsyncClient:
    def respond(request: httpx.Request) -> Response:
        if capture is not None:
            capture.append(request)
        content = body if isinstance(body, bytes) else json.dumps(body).encode()
        return Response(status_code, content=content, headers={"Content-Type": "application/json"})

    return AsyncClient(transport=MockTransport(respond))


async def test_development_header_authenticator_retains_local_mode() -> None:
    authenticator = HeaderRequestAuthenticator()

    principal = await authenticator.authenticate(
        RequestCredentials(tenant_id="tenant-a", principal_id="alice")
    )

    assert principal.tenant_id == "tenant-a"
    assert principal.principal_id == "alice"
    with pytest.raises(Unauthenticated):
        await authenticator.authenticate(RequestCredentials())


def test_authentication_config_defaults_to_disabled_and_reads_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = (
        "DSH_BASE_AGENT_AUTH_ENABLED",
        "DSH_BASE_AGENT_AUTH_TENANT_ID",
        "DSH_BASE_AGENT_MOA_AUTH_URL",
        "DSH_BASE_AGENT_MOA_PROJECT_ID",
        "DSH_BASE_AGENT_MOA_CONNECT_TIMEOUT_SECONDS",
        "DSH_BASE_AGENT_MOA_REQUEST_TIMEOUT_SECONDS",
        "DSH_BASE_AGENT_MOA_MAX_IN_FLIGHT",
    )
    for name in names:
        monkeypatch.delenv(name, raising=False)
    assert ApiAuthenticationConfig.from_env(env_file=None).enabled is False

    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "DSH_BASE_AGENT_AUTH_ENABLED=true",
                "DSH_BASE_AGENT_AUTH_TENANT_ID=company",
                "DSH_BASE_AGENT_MOA_AUTH_URL=https://login.moa.example",
                "DSH_BASE_AGENT_MOA_PROJECT_ID=1564",
                "DSH_BASE_AGENT_MOA_CONNECT_TIMEOUT_SECONDS=0.25",
                "DSH_BASE_AGENT_MOA_REQUEST_TIMEOUT_SECONDS=0.75",
                "DSH_BASE_AGENT_MOA_MAX_IN_FLIGHT=8",
            )
        ),
        encoding="utf-8",
    )

    config = ApiAuthenticationConfig.from_env(env_file=env_file)

    assert config.enabled is True
    assert config.tenant_id == "company"
    assert config.moa_project_id == 1564
    assert config.connect_timeout_seconds == 0.25
    assert config.request_timeout_seconds == 0.75
    assert config.max_in_flight == 8
    assert isinstance(config.create(), MoaRequestAuthenticator)


def test_enabled_authentication_requires_trusted_configuration() -> None:
    with pytest.raises(ValueError, match="AUTH_TENANT_ID"):
        ApiAuthenticationConfig(enabled=True, moa_project_id=1564)
    with pytest.raises(ValueError, match="MOA_PROJECT_ID"):
        ApiAuthenticationConfig(enabled=True, tenant_id="company")
    with pytest.raises(ValueError, match="HTTPS"):
        _configuration(moa_auth_url="http://moa.example")


async def test_moa_authenticator_uses_only_verified_response_identity() -> None:
    captured: list[httpx.Request] = []
    client = _moa_client(
        {
            "code": 0,
            "result": {
                "uid": "u-42",
                "project": 1564,
                "name": "Forged-proof user",
                "nick": "Alice.Chen",
            },
        },
        capture=captured,
    )
    authenticator = MoaRequestAuthenticator(_configuration(), client=client)

    principal = await authenticator.authenticate(
        RequestCredentials(
            tenant_id="forged-tenant",
            principal_id="forged-user",
            moa_token="secret-token",
        )
    )

    assert principal.tenant_id == "company"
    assert principal.principal_id == "user:alice.chen"
    assert len(captured) == 1
    assert captured[0].url == "https://login.moa.example/api/checktoken"
    assert json.loads(captured[0].content) == {"token": "secret-token", "project_id": 1564}
    await client.aclose()


async def test_moa_authenticator_distinguishes_invalid_identity_and_provider_failure() -> None:
    wrong_project_client = _moa_client(
        {"code": 0, "result": {"uid": "u-42", "project": 999, "nick": "alice"}}
    )
    wrong_project = MoaRequestAuthenticator(_configuration(), client=wrong_project_client)
    with pytest.raises(Unauthenticated):
        await wrong_project.authenticate(RequestCredentials(moa_token="token"))
    await wrong_project_client.aclose()

    unavailable_client = _moa_client({}, status_code=503)
    unavailable = MoaRequestAuthenticator(_configuration(), client=unavailable_client)
    with pytest.raises(AuthenticationProviderUnavailable):
        await unavailable.authenticate(RequestCredentials(moa_token="token"))
    await unavailable_client.aclose()


async def test_moa_enabled_api_needs_no_identity_headers(tmp_path: Path) -> None:
    moa_client = _moa_client(
        {"code": 0, "result": {"uid": "u-42", "project": 1564, "nick": "alice"}}
    )
    authenticator = MoaRequestAuthenticator(_configuration(), client=moa_client)
    control = ControlPlane(
        workspace=tmp_path,
        runtime=RuntimeConfig(provider="test", model="test", dsh_home=tmp_path / "dsh"),
        store=SqliteControlStore(tmp_path / "control.db"),
        runtime_factory=Factory(),
    )
    control.register(Agent(name="orders", prompt="Handle orders."))
    app = create_app(control, authenticator=authenticator, close_on_shutdown=False)
    await control.start()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/v1/runs",
            headers={
                "X-MOA-Token": "token",
                "X-Tenant-ID": "forged-tenant",
                "X-Principal-ID": "forged-user",
            },
            json={"agent_id": "orders", "input": "query order"},
        )
        assert response.status_code == 202
        assert response.json()["tenant_id"] == "company"
        assert response.json()["principal_id"] == "user:alice"

        missing = await client.post(
            "/v1/runs",
            json={"agent_id": "orders", "input": "query order"},
        )
        assert missing.status_code == 401
        assert missing.json()["detail"]["code"] == "UNAUTHENTICATED"

    await control.close()
    await moa_client.aclose()
