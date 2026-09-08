from __future__ import annotations

import json
import os
from typing import Any

import pytest

from dsh_base_agent import (
    DshNotificationEnvelope,
    KafkaNotificationConfig,
    KafkaNotificationPublisher,
)


class FakeProducer:
    def __init__(self, *, error: BaseException | None = None) -> None:
        self.error = error
        self.started = False
        self.stopped = False
        self.messages: list[tuple[str, bytes | None, bytes | None]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
    ) -> Any:
        if self.error is not None:
            raise self.error
        self.messages.append((topic, value, key))
        return object()


def notification(*, payload: dict[str, Any] | None = None) -> DshNotificationEnvelope:
    return DshNotificationEnvelope(
        tenant_id="tenant-a",
        principal_id="alice",
        agent_id="orders",
        conversation_id="conversation-1",
        run_id="run-1",
        attempt_id="attempt-1",
        dsh_session_id="session-1",
        method="session.event",
        payload=payload or {"event": {"type": "turn/start"}},
    )


def test_kafka_config_reads_dotenv_without_mutating_environment(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            (
                "DSH_BASE_AGENT_KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092",
                "DSH_BASE_AGENT_KAFKA_TOPIC=company.dsh.notifications.v1",
                "DSH_BASE_AGENT_KAFKA_QUEUE_CAPACITY=12",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("DSH_BASE_AGENT_KAFKA_BOOTSTRAP_SERVERS", raising=False)

    config = KafkaNotificationConfig.from_env(env_file=env_file)

    assert config.bootstrap_servers == ("kafka-1:9092", "kafka-2:9092")
    assert config.topic == "company.dsh.notifications.v1"
    assert config.queue_capacity == 12
    assert "DSH_BASE_AGENT_KAFKA_BOOTSTRAP_SERVERS" not in os.environ


async def test_publisher_sends_enriched_notification_and_flushes_on_close() -> None:
    producer = FakeProducer()
    publisher = KafkaNotificationPublisher(
        KafkaNotificationConfig(bootstrap_servers=("kafka:9092",)),
        producer_factory=lambda: producer,
    )

    await publisher.start()
    accepted = await publisher.publish(notification())
    await publisher.close()

    assert accepted is True
    assert producer.started is True
    assert producer.stopped is True
    assert publisher.stats.accepted == 1
    assert publisher.stats.published == 1
    assert publisher.stats.failed == 0
    topic, value, key = producer.messages[0]
    assert topic == "dsh.notifications.v1"
    assert key == b"tenant-a:session-1"
    assert value is not None
    document = json.loads(value)
    assert document["run_id"] == "run-1"
    assert document["attempt_id"] == "attempt-1"
    assert document["payload"]["event"]["type"] == "turn/start"


async def test_publish_failure_is_counted_without_escaping() -> None:
    publisher = KafkaNotificationPublisher(
        KafkaNotificationConfig(bootstrap_servers=("kafka:9092",)),
        producer_factory=lambda: FakeProducer(error=RuntimeError("offline")),
    )

    await publisher.start()
    assert await publisher.publish(notification()) is True
    await publisher.close()

    assert publisher.stats.accepted == 1
    assert publisher.stats.published == 0
    assert publisher.stats.failed == 1


def test_oversized_payload_is_replaced_by_hash_summary() -> None:
    encoded = notification(payload={"result": "x" * 5000}).serialize(max_message_bytes=1024)
    document = json.loads(encoded)

    assert document["payload"]["oversized"] is True
    assert document["payload"]["original_payload_size_bytes"] > 1024
    assert len(document["payload"]["original_payload_sha256"]) == 64
