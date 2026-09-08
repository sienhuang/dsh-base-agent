"""Best-effort direct publisher for raw DSH runtime notifications.

Kafka is an observation stream here, not the control-plane source of truth.  The
publisher intentionally does not use the PostgreSQL outbox and therefore accepts
that an in-memory message can be lost when the process exits unexpectedly.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable
from uuid import uuid4

from dotenv import dotenv_values

type JsonObject = dict[str, Any]

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class KafkaNotificationConfig:
    """Environment-driven configuration for the direct Kafka stream."""

    bootstrap_servers: tuple[str, ...] = ()
    topic: str = "dsh.notifications.v1"
    client_id: str = "dsh-base-agent"
    queue_capacity: int = 4096
    request_timeout_ms: int = 30_000
    close_timeout_seconds: float = 10.0
    max_message_bytes: int = 900_000
    required: bool = False

    def __post_init__(self) -> None:
        if any(not server.strip() for server in self.bootstrap_servers):
            raise ValueError("Kafka bootstrap servers must not be blank")
        if not self.topic.strip() or not self.client_id.strip():
            raise ValueError("Kafka topic and client_id must not be blank")
        if self.queue_capacity <= 0:
            raise ValueError("Kafka queue_capacity must be positive")
        if self.request_timeout_ms <= 0:
            raise ValueError("Kafka request_timeout_ms must be positive")
        if self.close_timeout_seconds <= 0:
            raise ValueError("Kafka close_timeout_seconds must be positive")
        if self.max_message_bytes < 1024:
            raise ValueError("Kafka max_message_bytes must be at least 1024")
        if self.required and not self.enabled:
            raise ValueError("required Kafka publishing needs bootstrap servers")

    @property
    def enabled(self) -> bool:
        return bool(self.bootstrap_servers)

    @classmethod
    def from_env(
        cls,
        *,
        prefix: str = "DSH_BASE_AGENT_KAFKA_",
        env_file: str | Path | None = ".env",
    ) -> KafkaNotificationConfig:
        file_values = {} if env_file is None else dotenv_values(dotenv_path=env_file)
        values = {key: value for key, value in file_values.items() if value is not None}
        values.update(os.environ)
        raw_servers = values.get(f"{prefix}BOOTSTRAP_SERVERS", "")
        servers = tuple(item.strip() for item in raw_servers.split(",") if item.strip())
        return cls(
            bootstrap_servers=servers,
            topic=values.get(f"{prefix}TOPIC", "dsh.notifications.v1"),
            client_id=values.get(f"{prefix}CLIENT_ID", "dsh-base-agent"),
            queue_capacity=int(values.get(f"{prefix}QUEUE_CAPACITY", "4096")),
            request_timeout_ms=int(values.get(f"{prefix}REQUEST_TIMEOUT_MS", "30000")),
            close_timeout_seconds=float(values.get(f"{prefix}CLOSE_TIMEOUT_SECONDS", "10")),
            max_message_bytes=int(values.get(f"{prefix}MAX_MESSAGE_BYTES", "900000")),
            required=_boolean(values.get(f"{prefix}REQUIRED", "false")),
        )


@dataclass(frozen=True, slots=True)
class DshNotificationEnvelope:
    """Business identity attached to one unmodified DSH notification payload."""

    tenant_id: str
    principal_id: str
    agent_id: str
    run_id: str
    attempt_id: str
    dsh_session_id: str
    method: str
    payload: JsonObject
    conversation_id: str | None = None
    notification_id: str = field(default_factory=lambda: f"notification_{uuid4().hex}")
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    schema_version: int = 1

    @property
    def partition_key(self) -> bytes:
        return f"{self.tenant_id}:{self.dsh_session_id}".encode()

    def serialize(self, *, max_message_bytes: int) -> bytes:
        document = self._document(payload=self.payload)
        encoded = _encode(document)
        if len(encoded) <= max_message_bytes:
            return encoded
        payload_bytes = _encode(self.payload)
        bounded = self._document(
            payload={
                "oversized": True,
                "original_payload_size_bytes": len(payload_bytes),
                "original_payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            }
        )
        return _encode(bounded)

    def _document(self, *, payload: JsonObject) -> JsonObject:
        return {
            "schema_version": self.schema_version,
            "notification_id": self.notification_id,
            "tenant_id": self.tenant_id,
            "principal_id": self.principal_id,
            "agent_id": self.agent_id,
            "conversation_id": self.conversation_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "dsh_session_id": self.dsh_session_id,
            "method": self.method,
            "payload": payload,
            "occurred_at": self.occurred_at.isoformat(),
        }


@dataclass(slots=True)
class PublisherStats:
    accepted: int = 0
    published: int = 0
    failed: int = 0
    dropped: int = 0


@runtime_checkable
class NotificationPublisher(Protocol):
    async def start(self) -> None: ...

    async def publish(self, notification: DshNotificationEnvelope) -> bool: ...

    async def close(self) -> None: ...


class _KafkaProducer(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
    ) -> Any: ...


class NullNotificationPublisher:
    """Zero-cost publisher used when Kafka is not configured."""

    async def start(self) -> None:
        return None

    async def publish(self, notification: DshNotificationEnvelope) -> bool:
        del notification
        return False

    async def close(self) -> None:
        return None


async def publish_notifications(
    queue: asyncio.Queue[DshNotificationEnvelope | None],
    producer: _KafkaProducer,
    *,
    topic: str,
    max_message_bytes: int,
    stats: PublisherStats,
    logger: logging.Logger = _LOGGER,
) -> None:
    """Drain a notification queue into Kafka until the ``None`` sentinel arrives.

    Individual Kafka failures are counted and logged, then discarded.  They never
    escape into the DSH execution path.
    """

    while True:
        notification = await queue.get()
        try:
            if notification is None:
                return
            try:
                await producer.send_and_wait(
                    topic,
                    value=notification.serialize(max_message_bytes=max_message_bytes),
                    key=notification.partition_key,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                stats.failed += 1
                logger.exception(
                    "failed to publish DSH notification %s for session %s",
                    notification.notification_id,
                    notification.dsh_session_id,
                )
            else:
                stats.published += 1
        finally:
            queue.task_done()


class KafkaNotificationPublisher:
    """Bounded, non-blocking, best-effort Kafka notification publisher."""

    def __init__(
        self,
        config: KafkaNotificationConfig,
        *,
        producer_factory: Callable[[], _KafkaProducer] | None = None,
    ) -> None:
        if not config.enabled:
            raise ValueError("KafkaNotificationPublisher requires bootstrap servers")
        self.config = config
        self.stats = PublisherStats()
        self._queue: asyncio.Queue[DshNotificationEnvelope | None] = asyncio.Queue(
            maxsize=config.queue_capacity
        )
        self._producer_factory = producer_factory or self._default_producer
        self._producer: _KafkaProducer | None = None
        self._worker: asyncio.Task[None] | None = None
        self._started = False
        self._accepting = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        producer = self._producer_factory()
        try:
            await producer.start()
        except Exception:
            self.stats.failed += 1
            _LOGGER.exception("failed to start Kafka notification producer")
            if self.config.required:
                self._started = False
                raise
            return
        self._producer = producer
        self._accepting = True
        self._worker = asyncio.create_task(
            publish_notifications(
                self._queue,
                producer,
                topic=self.config.topic,
                max_message_bytes=self.config.max_message_bytes,
                stats=self.stats,
            ),
            name="dsh-kafka-notification-publisher",
        )

    async def publish(self, notification: DshNotificationEnvelope) -> bool:
        if not self._accepting:
            self.stats.dropped += 1
            return False
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            self.stats.dropped += 1
            _LOGGER.warning(
                "dropping DSH notification %s because the Kafka queue is full",
                notification.notification_id,
            )
            return False
        self.stats.accepted += 1
        return True

    async def close(self) -> None:
        if not self._started:
            return
        self._accepting = False
        worker = self._worker
        producer = self._producer
        if worker is not None:
            try:
                await asyncio.wait_for(
                    self._queue.join(),
                    timeout=self.config.close_timeout_seconds,
                )
            except TimeoutError:
                remaining = self._queue.qsize()
                self.stats.dropped += remaining
                _LOGGER.warning(
                    "Kafka notification shutdown timed out with %s queued messages",
                    remaining,
                )
                worker.cancel()
            else:
                self._queue.put_nowait(None)
            try:
                await worker
            except asyncio.CancelledError:
                pass
        if producer is not None:
            try:
                await producer.stop()
            except Exception:
                self.stats.failed += 1
                _LOGGER.exception("failed to stop Kafka notification producer")
        self._producer = None
        self._worker = None
        self._started = False

    def _default_producer(self) -> _KafkaProducer:
        from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]

        return cast(
            _KafkaProducer,
            AIOKafkaProducer(
                bootstrap_servers=list(self.config.bootstrap_servers),
                client_id=self.config.client_id,
                acks=0,
                enable_idempotence=False,
                request_timeout_ms=self.config.request_timeout_ms,
            ),
        )


def create_notification_publisher(config: KafkaNotificationConfig) -> NotificationPublisher:
    if not config.enabled:
        return NullNotificationPublisher()
    return KafkaNotificationPublisher(config)


def _encode(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode()


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return repr(value)


def _boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


__all__ = [
    "DshNotificationEnvelope",
    "KafkaNotificationConfig",
    "KafkaNotificationPublisher",
    "NotificationPublisher",
    "NullNotificationPublisher",
    "PublisherStats",
    "create_notification_publisher",
    "publish_notifications",
]
