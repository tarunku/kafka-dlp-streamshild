"""
Topic administration — create, inspect, and delete Kafka topics.

TopicAdmin wraps confluent_kafka.admin.AdminClient with a clean interface.
It is used by infrastructure teams to set up topics before deployment, and
internally by DLQRouter to ensure the DLQ topic exists on first use.

Safety rules:
  - delete_topic() requires confirm=True. Without it, ValueError is raised
    immediately — no AdminClient call is made.
  - create_topic() does not fail if the topic already exists (idempotent).
  - Topics are NEVER auto-created by the producer or consumer.
"""

from __future__ import annotations

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from streamshield.auth.gcp import GCPAuth
from streamshield.config import SDKConfig
from streamshield.errors.exceptions import (
    AuthenticationError,
    TopicCreationError,
    TopicNotFoundError,
)
from streamshield.observability.logging import topic_logger
from streamshield.schema.models import TopicCreationResult, TopicMetadata


class TopicAdmin:
    """
    Administrative operations for Kafka topics.

    Usage:
        admin = TopicAdmin(config)
        admin.create_topic("my-events", partitions=3)
        admin.create_dlq_topic("my-events")   # creates "my-events.dlq"
        print(admin.topic_exists("my-events"))
    """

    def __init__(self, config: SDKConfig):
        config.validate()
        self._config = config

        self._auth = GCPAuth(
            project_id=config.gcp.project_id,
            token_refresh_buffer_s=config.gcp.token_refresh_buffer_s,
        )

        bootstrap_servers = self._resolve_bootstrap_servers()
        kafka_cfg = self._auth.build_kafka_config(bootstrap_servers)
        self._admin = AdminClient(kafka_cfg)
        topic_logger.info("TopicAdmin initialised — bootstrap=%s", bootstrap_servers)

    def _resolve_bootstrap_servers(self) -> str:
        if self._config.gcp.bootstrap_servers:
            return self._config.gcp.bootstrap_servers
        if self._config.gcp.use_secret_manager:
            return self._auth.get_secret(self._config.gcp.bootstrap_servers_secret)
        raise AuthenticationError("No bootstrap_servers configured and use_secret_manager=False.")

    def create_topic(
        self,
        name: str,
        partitions: int = 3,
        replication_factor: int = 3,
        config: dict[str, str] | None = None,
    ) -> TopicCreationResult:
        """
        Create a Kafka topic.

        If the topic already exists, this method returns successfully without
        raising — it is safe to call from Terraform-like scripts that run repeatedly.

        Args:
            name:               Topic name.
            partitions:         Number of partitions.
            replication_factor: Replication factor (must be <= number of brokers).
            config:             Optional topic-level config (e.g. retention.ms).

        Returns:
            TopicCreationResult(name, partitions, replication_factor, created)
            created=False when the topic already existed.

        Raises:
            TopicCreationError on unexpected broker errors.
        """
        topic_logger.info(
            "Creating topic: name=%s partitions=%d replication=%d",
            name, partitions, replication_factor,
        )

        new_topic = NewTopic(
            topic              = name,
            num_partitions     = partitions,
            replication_factor = replication_factor,
            config             = config or {},
        )

        futures = self._admin.create_topics([new_topic])

        for topic_name, future in futures.items():
            try:
                future.result()
                topic_logger.info("Topic created: %s", topic_name)
                return TopicCreationResult(
                    name=topic_name,
                    partitions=partitions,
                    replication_factor=replication_factor,
                    created=True,
                )
            except KafkaException as exc:
                # Error code 36 = TOPIC_ALREADY_EXISTS — treat as success
                if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                    topic_logger.info("Topic already exists: %s — skipping creation.", topic_name)
                    return TopicCreationResult(
                        name=topic_name,
                        partitions=partitions,
                        replication_factor=replication_factor,
                        created=False,
                    )
                raise TopicCreationError(
                    f"Failed to create topic '{topic_name}': {exc}",
                    safe_context={"topic": topic_name},
                ) from exc

        # Unreachable but keeps type checker happy
        return TopicCreationResult(name=name, partitions=partitions, replication_factor=replication_factor, created=False)

    def create_dlq_topic(self, source_topic: str) -> TopicCreationResult:
        """
        Create a Dead Letter Queue topic for a given source topic.

        The DLQ topic is named: {source_topic}{DLQConfig.topic_suffix}
        Default: "my-events" → "my-events.dlq"

        The DLQ topic inherits the partition count of the source topic (if
        the source exists). Falls back to 3 partitions if the source is not found.
        """
        dlq_name = f"{source_topic}{self._config.dlq.topic_suffix}"

        # Try to match the source topic's partition count
        partitions = 3
        try:
            src_metadata = self.describe_topic(source_topic)
            partitions = src_metadata.partitions
        except TopicNotFoundError:
            topic_logger.warning(
                "Source topic '%s' not found when creating DLQ — defaulting to 3 partitions.",
                source_topic,
            )

        topic_logger.info("Creating DLQ topic: %s (partitions=%d)", dlq_name, partitions)

        return self.create_topic(
            name               = dlq_name,
            partitions         = partitions,
            replication_factor = 3,
            config             = {
                # DLQ messages retained for 7 days — gives ops time to replay or inspect
                "retention.ms": "604800000",
                "cleanup.policy": "delete",
            },
        )

    def topic_exists(self, name: str) -> bool:
        """Return True if the topic exists in the cluster."""
        cluster_metadata = self._admin.list_topics(timeout=10)
        return name in cluster_metadata.topics

    def describe_topic(self, name: str) -> TopicMetadata:
        """
        Return metadata for a topic.

        Raises:
            TopicNotFoundError if the topic does not exist.
        """
        # Check existence using the all-topics list first.  Targeted list_topics(topic=name)
        # can trigger auto-creation on brokers where auto.create.topics.enable=true, so we
        # avoid calling it for a topic that doesn't exist.
        if not self.topic_exists(name):
            raise TopicNotFoundError(
                f"Topic '{name}' not found.",
                safe_context={"topic": name},
            )

        cluster_metadata = self._admin.list_topics(topic=name, timeout=10)
        topic_meta = cluster_metadata.topics[name]
        partition_count = len(topic_meta.partitions)

        # Replication factor: number of replicas on the first partition
        replication_factor = 1
        if topic_meta.partitions:
            first_partition = list(topic_meta.partitions.values())[0]
            replication_factor = len(first_partition.replicas)

        return TopicMetadata(
            name               = name,
            partitions         = partition_count,
            replication_factor = replication_factor,
        )

    def delete_topic(self, name: str, confirm: bool = False) -> None:
        """
        Delete a Kafka topic. IRREVERSIBLE.

        Requires confirm=True. Without it, ValueError is raised before any
        AdminClient call is made — this prevents accidental deletions.

        Args:
            name:    Topic to delete.
            confirm: Must be True to proceed.

        Raises:
            ValueError if confirm=False.
            TopicNotFoundError if the topic does not exist.
        """
        if not confirm:
            raise ValueError(
                f"Pass confirm=True to delete topic '{name}'. "
                "This operation is irreversible and will destroy all messages in the topic."
            )

        if not self.topic_exists(name):
            raise TopicNotFoundError(
                f"Cannot delete topic '{name}' — it does not exist.",
                safe_context={"topic": name},
            )

        futures = self._admin.delete_topics([name])
        for topic_name, future in futures.items():
            try:
                future.result()
                topic_logger.info("Deleted topic: %s", topic_name)
            except KafkaException as exc:
                raise TopicCreationError(
                    f"Failed to delete topic '{topic_name}': {exc}",
                    safe_context={"topic": topic_name},
                ) from exc

    def list_topics(self) -> list[str]:
        """Return the names of all topics in the cluster (excluding internal topics)."""
        cluster_metadata = self._admin.list_topics(timeout=10)
        return [
            name for name in cluster_metadata.topics
            if not name.startswith("__")  # exclude internal Kafka topics like __consumer_offsets
        ]
