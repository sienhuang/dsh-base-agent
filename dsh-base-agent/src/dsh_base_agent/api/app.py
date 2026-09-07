"""Stable company-facing HTTP contract, independent from DSH JSON-RPC."""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import StreamingResponse

from dsh_base_agent.api.authentication import (
    HeaderRequestAuthenticator,
    RequestAuthenticationError,
    RequestAuthenticator,
    RequestCredentials,
)
from dsh_base_agent.artifacts import ArtifactNotFoundError
from dsh_base_agent.control.auth import AuthorizationDenied, Principal
from dsh_base_agent.control.models import (
    ArtifactRecord,
    ConversationStatus,
    RunAttempt,
    RunEvent,
    RunRecord,
)
from dsh_base_agent.control.plane import (
    AgentNotFoundError,
    ControlPlane,
    ConversationAccessDenied,
    RunAccessDenied,
    RunResumeUnsupported,
    RunTransitionError,
    RunView,
)
from dsh_base_agent.store import (
    ConversationNotFoundError,
    IdempotencyConflictError,
    RunNotFoundError,
)


class CreateRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(min_length=1)
    input: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateConversationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateConversationRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: str = Field(min_length=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ResumeRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: str = Field(min_length=1)


class RunResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run: RunRecord
    attempts: tuple[RunAttempt, ...] = ()


class ConversationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    conversation_id: str
    agent_id: str
    agent_version: str
    status: ConversationStatus
    next_sequence: int
    revision: int
    blocked_reason: str | None
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime


OptionalTenantHeader = Annotated[str | None, Header(alias="X-Tenant-ID")]
OptionalPrincipalHeader = Annotated[str | None, Header(alias="X-Principal-ID")]
OptionalMoaTokenHeader = Annotated[str | None, Header(alias="X-MOA-Token")]
OptionalAuthorizationHeader = Annotated[str | None, Header(alias="Authorization")]


def create_app(
    control: ControlPlane,
    *,
    authenticator: RequestAuthenticator | None = None,
    close_on_shutdown: bool = True,
) -> FastAPI:
    selected_authenticator = authenticator or HeaderRequestAuthenticator()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            await control.start()
            yield
        finally:
            if close_on_shutdown:
                await control.close()
            await selected_authenticator.close()

    app = FastAPI(title="dsh-base-agent", version="0.1.0", lifespan=lifespan)
    app.state.request_authenticator = selected_authenticator

    async def authenticated_principal(
        tenant_id: OptionalTenantHeader = None,
        principal_id: OptionalPrincipalHeader = None,
        moa_token: OptionalMoaTokenHeader = None,
        authorization: OptionalAuthorizationHeader = None,
    ) -> Principal:
        try:
            return await selected_authenticator.authenticate(
                RequestCredentials(
                    tenant_id=tenant_id,
                    principal_id=principal_id,
                    moa_token=moa_token,
                    authorization=authorization,
                )
            )
        except RequestAuthenticationError as exc:
            raise HTTPException(
                status_code=exc.status_code,
                detail={
                    "code": exc.code,
                    "message": str(exc),
                    "retryable": exc.retryable,
                },
            ) from exc

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready() -> dict[str, str]:
        await control.start()
        return {"status": "ready"}

    @app.post("/v1/runs", status_code=status.HTTP_202_ACCEPTED)
    async def create_run(
        request: CreateRunRequest,
        principal: Annotated[Principal, Depends(authenticated_principal)],
        idempotency_key: Annotated[
            str | None,
            Header(alias="Idempotency-Key", min_length=1, max_length=256),
        ] = None,
    ) -> RunRecord:
        try:
            return await control.submit(
                principal=principal,
                agent_id=request.agent_id,
                input=request.input,
                idempotency_key=idempotency_key,
                metadata=request.metadata,
            )
        except AgentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except AuthorizationDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except IdempotencyConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post(
        "/v1/conversations",
        status_code=status.HTTP_201_CREATED,
    )
    async def create_conversation(
        request: CreateConversationRequest,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> ConversationResponse:
        try:
            conversation = await control.create_conversation(
                principal=principal,
                agent_id=request.agent_id,
                metadata=request.metadata,
            )
            return ConversationResponse.model_validate(conversation)
        except AgentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except AuthorizationDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/v1/conversations/{conversation_id}")
    async def get_conversation(
        conversation_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> ConversationResponse:
        try:
            return ConversationResponse.model_validate(
                await control.get_conversation(
                    principal,
                    conversation_id,
                )
            )
        except ConversationNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConversationAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.post(
        "/v1/conversations/{conversation_id}/runs",
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_conversation_run(
        conversation_id: str,
        request: CreateConversationRunRequest,
        principal: Annotated[Principal, Depends(authenticated_principal)],
        idempotency_key: Annotated[
            str | None,
            Header(alias="Idempotency-Key", min_length=1, max_length=256),
        ] = None,
    ) -> RunRecord:
        try:
            return await control.submit_to_conversation(
                principal=principal,
                conversation_id=conversation_id,
                input=request.input,
                idempotency_key=idempotency_key,
                metadata=request.metadata,
            )
        except ConversationNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConversationAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except AuthorizationDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except RunTransitionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/v1/conversations/{conversation_id}/runs")
    async def list_conversation_runs(
        conversation_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> tuple[RunRecord, ...]:
        try:
            return await control.conversation_runs(
                principal,
                conversation_id,
            )
        except ConversationNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ConversationAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/v1/runs/{run_id}")
    async def get_run(
        run_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> RunResponse:
        view = await _view(control, principal, run_id)
        return RunResponse(run=view.run, attempts=view.attempts)

    @app.get("/v1/runs/{run_id}/events")
    async def get_events(
        run_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
        after: Annotated[int, Query(ge=0)] = 0,
    ) -> tuple[RunEvent, ...]:
        try:
            return await control.events(
                principal,
                run_id,
                after=after,
            )
        except RunNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RunAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.post("/v1/runs/{run_id}/cancel")
    async def cancel_run(
        run_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> RunRecord:
        return await _transition(control.cancel, principal, run_id)

    @app.post("/v1/runs/{run_id}/retry", status_code=status.HTTP_202_ACCEPTED)
    async def retry_run(
        run_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> RunRecord:
        return await _transition(control.retry, principal, run_id)

    @app.post("/v1/runs/{run_id}/resume")
    async def resume_run(
        run_id: str,
        request: ResumeRunRequest,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> RunRecord:
        try:
            return await control.resume(
                principal,
                run_id,
                input=request.input,
            )
        except RunResumeUnsupported as exc:
            raise HTTPException(status_code=501, detail=str(exc)) from exc
        except RunTransitionError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except RunNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RunAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/v1/runs/{run_id}/artifacts")
    async def get_artifacts(
        run_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> tuple[ArtifactRecord, ...]:
        try:
            return await control.artifacts(principal, run_id)
        except RunNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RunAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc

    @app.get("/v1/runs/{run_id}/artifacts/{artifact_id}/content")
    async def get_artifact_content(
        run_id: str,
        artifact_id: str,
        principal: Annotated[Principal, Depends(authenticated_principal)],
    ) -> StreamingResponse:
        try:
            record, content = await control.open_artifact(
                principal,
                run_id,
                artifact_id,
            )
        except (RunNotFoundError, ArtifactNotFoundError) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except RunAccessDenied as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        return StreamingResponse(
            content,
            media_type=record.media_type,
            headers={
                "Content-Length": str(record.size_bytes),
                "Content-Disposition": (
                    "attachment; filename*=UTF-8''" + quote(record.name, safe="")
                ),
                "X-Artifact-SHA256": record.sha256,
            },
        )

    return app


async def _view(control: ControlPlane, principal: Principal, run_id: str) -> RunView:
    try:
        return await control.view(principal, run_id)
    except RunNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RunAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


async def _transition(
    operation: Callable[[Principal, str], Awaitable[RunRecord]],
    principal: Principal,
    run_id: str,
) -> RunRecord:
    try:
        return await operation(principal, run_id)
    except RunNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RunAccessDenied as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except RunTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


__all__ = [
    "CreateConversationRequest",
    "CreateConversationRunRequest",
    "CreateRunRequest",
    "ConversationResponse",
    "ResumeRunRequest",
    "RunResponse",
    "create_app",
]
