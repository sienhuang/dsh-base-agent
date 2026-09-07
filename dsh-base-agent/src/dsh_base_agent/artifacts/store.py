"""Durable artifact bodies kept outside the control database."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

_ARTIFACT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
_LOCAL_LOCATION_PREFIX = "local:"


class ArtifactStoreError(RuntimeError):
    """Base error for artifact body storage."""


class ArtifactNotFoundError(ArtifactStoreError):
    """The referenced artifact body does not exist."""


class ArtifactTooLargeError(ArtifactStoreError):
    """An artifact body exceeds the configured local object limit."""


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    """Storage result used to construct the control-plane ArtifactRecord."""

    location: str
    sha256: str
    size_bytes: int


@runtime_checkable
class ArtifactStore(Protocol):
    """Backend-neutral storage contract for immutable artifact bodies."""

    async def initialize(self) -> None: ...

    async def put(self, *, artifact_id: str, content: bytes) -> StoredArtifact: ...

    async def open(
        self,
        location: str,
        *,
        chunk_size: int = 64 * 1024,
    ) -> AsyncIterator[bytes]: ...

    async def delete(self, location: str) -> None: ...

    async def close(self) -> None: ...


class LocalArtifactStore:
    """Immutable local-file artifact backend for development or a shared PVC.

    ``location`` is an opaque logical key. Absolute host paths are never stored in
    PostgreSQL or returned through the company API.
    """

    def __init__(self, root: str | Path, *, max_object_bytes: int = 64 * 1024 * 1024) -> None:
        if max_object_bytes <= 0:
            raise ValueError("max_object_bytes must be positive")
        self.root = Path(root).expanduser().resolve()
        self.max_object_bytes = max_object_bytes
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True, mode=0o700)
        self._initialized = True

    async def put(self, *, artifact_id: str, content: bytes) -> StoredArtifact:
        if not self._initialized:
            raise ArtifactStoreError("ArtifactStore is not initialized")
        if not _ARTIFACT_ID.fullmatch(artifact_id):
            raise ArtifactStoreError("artifact_id contains unsafe characters")
        immutable_content = bytes(content)
        if len(immutable_content) > self.max_object_bytes:
            raise ArtifactTooLargeError(
                f"artifact body is {len(immutable_content)} bytes; "
                f"limit is {self.max_object_bytes} bytes"
            )
        digest = hashlib.sha256(immutable_content).hexdigest()
        relative = Path("objects") / digest[:2] / artifact_id
        target = self._resolve_relative(relative)
        await asyncio.to_thread(_write_exclusive, target, immutable_content)
        return StoredArtifact(
            location=f"{_LOCAL_LOCATION_PREFIX}{relative.as_posix()}",
            sha256=digest,
            size_bytes=len(immutable_content),
        )

    async def open(
        self,
        location: str,
        *,
        chunk_size: int = 64 * 1024,
    ) -> AsyncIterator[bytes]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        path = await asyncio.to_thread(self._existing_path, location)
        return self._stream(path, chunk_size)

    async def delete(self, location: str) -> None:
        path = self._location_path(location)
        try:
            await asyncio.to_thread(path.unlink)
        except FileNotFoundError:
            return

    async def close(self) -> None:
        self._initialized = False

    async def _stream(self, path: Path, chunk_size: int) -> AsyncIterator[bytes]:
        stream = await asyncio.to_thread(path.open, "rb")
        try:
            while chunk := await asyncio.to_thread(stream.read, chunk_size):
                yield chunk
        finally:
            await asyncio.to_thread(stream.close)

    def _existing_path(self, location: str) -> Path:
        path = self._location_path(location)
        if not path.is_file():
            raise ArtifactNotFoundError(f"artifact body '{location}' was not found")
        return path

    def _location_path(self, location: str) -> Path:
        if not location.startswith(_LOCAL_LOCATION_PREFIX):
            raise ArtifactStoreError("LocalArtifactStore received an unsupported location")
        raw = location.removeprefix(_LOCAL_LOCATION_PREFIX)
        relative = Path(raw)
        if relative.is_absolute() or not relative.parts:
            raise ArtifactStoreError("artifact location is invalid")
        return self._resolve_relative(relative)

    def _resolve_relative(self, relative: Path) -> Path:
        candidate = (self.root / relative).resolve()
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:
            raise ArtifactStoreError("artifact location escapes the configured root") from exc
        return candidate


def _write_exclusive(target: Path, content: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor: int | None = None
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise ArtifactStoreError(f"artifact body '{target.name}' already exists") from exc
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        try:
            target.unlink()
        except FileNotFoundError:
            pass
        raise


__all__ = [
    "ArtifactNotFoundError",
    "ArtifactStore",
    "ArtifactStoreError",
    "ArtifactTooLargeError",
    "LocalArtifactStore",
    "StoredArtifact",
]
