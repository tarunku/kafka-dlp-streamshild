"""
StreamShield Example: Kafka Avro → GCS (NDJSON) consumer.

Reads Avro-serialised messages from Kafka, batches them, and writes
newline-delimited JSON (NDJSON) files to GCS. File naming matches the
GCP Managed Kafka Connect GCS Sink convention so the existing
GCS → Pub/Sub → Snowpipe pipeline picks up both connector-written and
SDK-written files identically.

Reliability contract:
  - Offsets commit ONLY after every GCS write in the batch succeeds.
  - GCS blob names are deterministic (partition + first offset) — replaying
    the same messages after a crash overwrites the same blob with identical
    content, so delivery is effectively idempotent.
  - If GCS write fails, offsets are NOT committed. The consumer restarts from
    the last committed offset and retries the entire batch.
  - Bad Avro messages (deserialization failures) are logged and skipped; the
    offset is committed so the consumer always advances past poison messages.
  - enable.auto.commit is always False (SDK invariant).

GCS file layout:
  gs://{GCS_BUCKET}/{GCS_PREFIX}{topic}-{partition}-{first_offset:012d}.json

Each file contains one JSON object per line (NDJSON). Snowpipe uses $1:field_name
to extract columns — field names must match the Avro schema field names exactly.

Run:
    python3 examples/gcs_consumer.py

IAM required on the running service account:
  - roles/managedkafka.client            (Kafka consumer)
  - roles/secretmanager.secretAccessor   (bootstrap + schema registry URLs)
  - roles/storage.objectCreator          (write objects to GCS bucket)

Install:
    pip install -e ".[gcs]"
"""

import json
import logging
import time
from collections import defaultdict
from pathlib import Path

from google.cloud import storage

from streamshield import ConsumedMessage, KafkaConsumer, SDKConfig
from streamshield.errors.exceptions import DeserializationFailedError
from streamshield.observability.logging import configure_json_logging

configure_json_logging(level=logging.INFO)
log = logging.getLogger("streamshield.gcs_consumer")

# ── Configuration ──────────────────────────────────────────────────────────────

_CONFIG_FILE = Path(__file__).parent / "streamshield-config.yaml"
config = SDKConfig.from_yaml(str(_CONFIG_FILE))

TOPIC             = "prescription-events"
GROUP_ID          = "gcs-delivery-consumer"
GCS_BUCKET        = "kafka-poc-gcs-landing"
GCS_PREFIX        = "prescription-events-"   # matches connector file.name.prefix convention

# Flush triggers — mirrors the connector's flush.size and rotate.interval.ms
FLUSH_SIZE        = 10      # write to GCS after this many records (across all partitions)
ROTATE_INTERVAL_S = 60.0   # or after this many seconds, whichever comes first

# ── GCS client ─────────────────────────────────────────────────────────────────

gcs_client = storage.Client()
bucket     = gcs_client.bucket(GCS_BUCKET)


# ── GCS write ──────────────────────────────────────────────────────────────────

def write_batch_to_gcs(batch: list[ConsumedMessage]) -> None:
    """
    Write a batch of messages to GCS as NDJSON, one file per partition.

    Blob name format:
        {GCS_PREFIX}{topic}-{partition}-{first_offset:012d}.json

    Matches the GCS Sink Connector naming convention so Snowpipe treats both
    connector-written and SDK-written files identically.

    Args:
        batch: Messages to write. May span multiple partitions.

    Raises:
        google.api_core.exceptions.GoogleAPIError: if any GCS upload fails.
        The caller must NOT commit offsets if this raises — the batch will
        be retried in full from the last committed offset on restart.
    """
    by_partition: dict[int, list[ConsumedMessage]] = defaultdict(list)
    for msg in batch:
        by_partition[msg.partition].append(msg)

    for partition, msgs in sorted(by_partition.items()):
        first_offset = msgs[0].offset
        blob_name    = f"{GCS_PREFIX}{msgs[0].topic}-{partition}-{first_offset:012d}.json"

        # One JSON object per line — no outer array, no schema envelope.
        # default=str handles Avro types (datetime, Decimal, bytes) that are
        # not natively JSON-serialisable.
        ndjson = "\n".join(json.dumps(m.value, default=str) for m in msgs) + "\n"

        bucket.blob(blob_name).upload_from_string(
            ndjson,
            content_type="application/json",
        )

        log.info(
            "Written gs://%s/%s  partition=%d  offsets=%d–%d  records=%d",
            GCS_BUCKET, blob_name, partition,
            msgs[0].offset, msgs[-1].offset, len(msgs),
        )


# ── Consumer loop ──────────────────────────────────────────────────────────────

log.info(
    "Starting GCS consumer  topic=%s  group=%s  bucket=gs://%s  "
    "flush_size=%d  rotate_interval=%.0fs",
    TOPIC, GROUP_ID, GCS_BUCKET, FLUSH_SIZE, ROTATE_INTERVAL_S,
)

with KafkaConsumer(config, group_id=GROUP_ID) as consumer:
    consumer.subscribe([TOPIC])

    batch:       list[ConsumedMessage] = []
    batch_start: float                 = time.monotonic()

    try:
        while True:
            # ── Poll one message ───────────────────────────────────────────────
            try:
                msg = consumer.poll(timeout=1.0, detokenize=False)
            except DeserializationFailedError as exc:
                # Avro bytes are corrupt or the schema_id is unknown.
                # Commit and advance — for full DLQ routing use consumer.process().
                log.error("Deserialization failed — skipping message: %s", exc)
                consumer.commit()
                continue

            if msg is not None:
                batch.append(msg)

            # ── Flush decision ─────────────────────────────────────────────────
            elapsed = time.monotonic() - batch_start
            if not batch or (len(batch) < FLUSH_SIZE and elapsed < ROTATE_INTERVAL_S):
                continue

            # ── Write then commit ──────────────────────────────────────────────
            # write_batch_to_gcs raises on any GCS failure → we skip commit →
            # the exception propagates, the process exits, and the supervisor
            # restarts it from the last committed offset.
            write_batch_to_gcs(batch)

            consumer.commit()
            log.info(
                "Offsets committed  records=%d  elapsed=%.1fs",
                len(batch), elapsed,
            )

            batch       = []
            batch_start = time.monotonic()

    except KeyboardInterrupt:
        # Flush whatever accumulated before Ctrl-C.
        if batch:
            log.info("Interrupted — flushing %d remaining records.", len(batch))
            write_batch_to_gcs(batch)
            consumer.commit()

log.info("GCS consumer stopped.")
