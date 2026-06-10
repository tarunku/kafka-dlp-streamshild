"""
Integration test configuration.

These tests run against the real GCP environment used in the POC:
  - GCP Project:   vetsource-496203
  - Kafka Topic:   prescription-events
  - Schema Registry subject: prescription-events-value

Prerequisites (same as the POC):
  - Run on the GCE VM with vm-producer-sa attached (ADC available)
  - OR set GOOGLE_APPLICATION_CREDENTIALS to a service account key file
  - All Secret Manager secrets must be populated (see POC setup guide)

Run:
    pytest tests/integration/ -v

Skip:
    pytest tests/unit/ -v   (skips integration tests entirely)
"""

import time

import pytest
from confluent_kafka import Consumer as ConfluentConsumer, Producer as ConfluentProducer

from streamshield import GCPConfig, SDKConfig
from streamshield.auth.gcp import GCPAuth

# GCP project and topic for integration tests — mirrors the POC environment
INTEGRATION_PROJECT_ID = "vetsource-496203"
INTEGRATION_TOPIC      = "prescription-events"
INTEGRATION_SUBJECT    = "prescription-events-value"
DLQ_TOPIC              = "prescription-events.dlq"


@pytest.fixture(scope="session")
def integration_config() -> SDKConfig:
    """
    SDKConfig wired to the POC GCP project.
    Secrets are loaded from GCP Secret Manager automatically.
    """
    return SDKConfig(
        gcp=GCPConfig(
            project_id=INTEGRATION_PROJECT_ID,
            use_secret_manager=True,
            bootstrap_servers_secret="kafka-bootstrap-servers",
            schema_registry_url_secret="schema-registry-url",
        )
    )


@pytest.fixture(scope="session")
def gcp_auth() -> GCPAuth:
    """Shared GCPAuth for building raw Kafka configs in tests that bypass the SDK."""
    return GCPAuth(project_id=INTEGRATION_PROJECT_ID)


@pytest.fixture(scope="session")
def bootstrap_servers(gcp_auth: GCPAuth) -> str:
    """Kafka bootstrap address resolved once from Secret Manager."""
    return gcp_auth.get_secret("kafka-bootstrap-servers")


@pytest.fixture(scope="session")
def raw_kafka_producer(gcp_auth: GCPAuth, bootstrap_servers: str) -> ConfluentProducer:
    """
    Raw confluent_kafka.Producer that bypasses the SDK entirely.
    Used to inject malformed/invalid bytes into topics for DLQ and
    deserialization failure tests.
    """
    cfg = gcp_auth.build_kafka_config(bootstrap_servers)
    producer = ConfluentProducer(cfg)
    yield producer
    producer.flush(timeout=10.0)


@pytest.fixture
def raw_consumer_factory(gcp_auth: GCPAuth, bootstrap_servers: str):
    """
    Factory that creates raw confluent_kafka.Consumer instances.
    Each call returns a new consumer with the given group_id and offset reset policy.
    All consumers created by this fixture are closed on test teardown.

    Usage:
        consumer = raw_consumer_factory("my-group-id", auto_offset_reset="latest")
    """
    consumers: list[ConfluentConsumer] = []

    def _make(group_id: str, auto_offset_reset: str = "latest") -> ConfluentConsumer:
        cfg = gcp_auth.build_kafka_config(bootstrap_servers, extra={
            "group.id":             group_id,
            "auto.offset.reset":    auto_offset_reset,
            "enable.auto.commit":   "false",
            "session.timeout.ms":   "30000",
            "heartbeat.interval.ms": "3000",
        })
        c = ConfluentConsumer(cfg)
        consumers.append(c)
        return c

    yield _make

    for c in consumers:
        try:
            c.close()
        except Exception:
            pass
