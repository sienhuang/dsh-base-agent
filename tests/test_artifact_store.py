from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from dsh_base_agent.artifacts import (
    ArtifactNotFoundError,
    ArtifactStoreConfig,
    ArtifactStoreError,
    ArtifactTooLargeError,
    LocalArtifactStore,
)


async def test_local_artifact_store_writes_streams_and_deletes_immutable_body(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path / "artifact-root")
    await store.initialize()
    content = b'{"message":"hello"}'

    stored = await store.put(artifact_id="artifact_123", content=content)

    assert stored.location.startswith("local:objects/")
    assert str(tmp_path) not in stored.location
    assert stored.sha256 == hashlib.sha256(content).hexdigest()
    assert stored.size_bytes == len(content)
    stream = await store.open(stored.location, chunk_size=4)
    assert b"".join([chunk async for chunk in stream]) == content

    with pytest.raises(ArtifactStoreError, match="already exists"):
        await store.put(artifact_id="artifact_123", content=content)

    await store.delete(stored.location)
    with pytest.raises(ArtifactNotFoundError):
        await store.open(stored.location)
    await store.close()


async def test_local_artifact_store_rejects_location_escape(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path / "artifact-root")
    await store.initialize()

    with pytest.raises(ArtifactStoreError, match="escapes"):
        await store.open("local:../../secret")


async def test_local_artifact_store_rejects_body_over_hard_limit(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path / "artifact-root", max_object_bytes=4)
    await store.initialize()

    with pytest.raises(ArtifactTooLargeError, match="limit is 4 bytes"):
        await store.put(artifact_id="artifact_large", content=b"12345")


def test_artifact_store_config_resolves_relative_root_inside_workspace(tmp_path) -> None:
    config = ArtifactStoreConfig(local_root=Path("custom-artifacts"))
    store = config.create(workspace=tmp_path)

    assert isinstance(store, LocalArtifactStore)
    assert store.root == (tmp_path / "custom-artifacts").resolve()
