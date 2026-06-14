"""
Integration tests: TopicAdmin operations against the real Kafka cluster.

Covers:
  1. topic_exists() — True for known topic.
  2. topic_exists() — False for non-existent topic (UUID name, guaranteed fresh).
  3. describe_topic() — returns correct partition count and replication factor.
  4. describe_topic() — raises TopicNotFoundError for unknown topic.
  5. delete_topic() without confirm=True — raises ValueError (no broker call).
  6. list_topics() — includes prescription-events and its DLQ.
  7. create_topic() — idempotent: second call returns created=False, no exception.
  8. create_dlq_topic() — idempotent: inherits source topic partition count.

Note on GCP Managed Kafka auto-topic-creation:
  GCP Managed Kafka with auto.create.topics.enable=true will create a topic when a
  targeted metadata request (list_topics(topic=name)) is issued for a non-existent name.
  Tests that need a "non-existent" topic therefore use uuid4()-based names to guarantee
  freshness, and describe_topic() was updated to call topic_exists() (all-topics fetch)
  before issuing any targeted request.

All tests run against terraform-testing-498903.  No mocking.
"""

from __future__ import annotations

import uuid

import pytest

from streamshield import TopicAdmin
from streamshield.errors.exceptions import TopicNotFoundError
from tests.integration.conftest import DLQ_TOPIC, INTEGRATION_TOPIC


def _fresh_nonexistent_name() -> str:
    """Return a topic name that is guaranteed not to exist on the broker."""
    return f"nonexistent-{uuid.uuid4().hex}"


class TestTopicExists:
    def test_returns_true_for_known_topic(self, integration_config):
        admin = TopicAdmin(integration_config)
        assert admin.topic_exists(INTEGRATION_TOPIC) is True

    def test_returns_true_for_dlq_topic(self, integration_config):
        admin = TopicAdmin(integration_config)
        assert admin.topic_exists(DLQ_TOPIC) is True

    def test_returns_false_for_nonexistent_topic(self, integration_config):
        admin = TopicAdmin(integration_config)
        assert admin.topic_exists(_fresh_nonexistent_name()) is False


class TestDescribeTopic:
    def test_describe_returns_correct_partition_count(self, integration_config):
        """prescription-events was created with 3 partitions."""
        admin = TopicAdmin(integration_config)
        meta = admin.describe_topic(INTEGRATION_TOPIC)

        assert meta.name       == INTEGRATION_TOPIC
        assert meta.partitions == 3
        assert meta.replication_factor >= 1

    def test_describe_dlq_topic_returns_metadata(self, integration_config):
        """DLQ topic must exist and have a replication factor."""
        admin = TopicAdmin(integration_config)
        meta = admin.describe_topic(DLQ_TOPIC)

        assert meta.name == DLQ_TOPIC
        assert meta.partitions >= 1
        assert meta.replication_factor >= 1

    def test_describe_nonexistent_topic_raises_topic_not_found(self, integration_config):
        admin = TopicAdmin(integration_config)
        with pytest.raises(TopicNotFoundError):
            admin.describe_topic(_fresh_nonexistent_name())


class TestDeleteTopic:
    def test_delete_without_confirm_raises_value_error(self, integration_config):
        """
        Passing confirm=False must raise ValueError immediately — no AdminClient
        call is made.  This is a safety guard against accidental deletion.
        """
        admin = TopicAdmin(integration_config)
        with pytest.raises(ValueError, match="confirm=True"):
            admin.delete_topic(INTEGRATION_TOPIC, confirm=False)

    def test_delete_nonexistent_topic_raises_topic_not_found(self, integration_config):
        """
        Even with confirm=True, deleting a topic that doesn't exist raises
        TopicNotFoundError before sending any AdminClient request.
        """
        admin = TopicAdmin(integration_config)
        with pytest.raises(TopicNotFoundError):
            admin.delete_topic(_fresh_nonexistent_name(), confirm=True)


class TestListTopics:
    def test_list_includes_source_and_dlq_topics(self, integration_config):
        admin = TopicAdmin(integration_config)
        topics = admin.list_topics()

        assert INTEGRATION_TOPIC in topics, \
            f"{INTEGRATION_TOPIC} not found in list_topics()"
        assert DLQ_TOPIC in topics, \
            f"{DLQ_TOPIC} not found in list_topics()"

    def test_list_excludes_internal_kafka_topics(self, integration_config):
        """Internal topics starting with __ (e.g. __consumer_offsets) must be excluded."""
        admin = TopicAdmin(integration_config)
        topics = admin.list_topics()

        internal = [t for t in topics if t.startswith("__")]
        assert internal == [], f"list_topics() returned internal topics: {internal}"


class TestCreateTopicIdempotent:
    def test_create_existing_topic_returns_created_false(self, integration_config):
        """
        Calling create_topic() for a topic that already exists must return
        TopicCreationResult(created=False) — no exception, idempotent.
        """
        admin = TopicAdmin(integration_config)
        result = admin.create_topic(INTEGRATION_TOPIC, partitions=3)

        assert result.name    == INTEGRATION_TOPIC
        assert result.created is False  # already existed

    def test_create_dlq_topic_is_idempotent(self, integration_config):
        """
        create_dlq_topic() for an already-existing DLQ topic must succeed.
        The DLQ topic partition count should match (or be compatible with) the source.
        """
        admin = TopicAdmin(integration_config)
        result = admin.create_dlq_topic(INTEGRATION_TOPIC)

        # The call must succeed (not raise) regardless of whether the topic existed
        assert result.name == DLQ_TOPIC
        # Partition count must be >= 1 (matches source topic or falls back to 3)
        assert result.partitions >= 1

    def test_create_dlq_topic_inherits_source_partition_count(self, integration_config):
        """
        create_dlq_topic() reads the source topic's partition count and uses it.
        prescription-events has 3 partitions → DLQ must also have 3.
        """
        admin  = TopicAdmin(integration_config)
        result = admin.create_dlq_topic(INTEGRATION_TOPIC)

        source_meta = admin.describe_topic(INTEGRATION_TOPIC)
        # DLQ was created with the source's partition count (or already exists with 3)
        assert result.partitions == source_meta.partitions
