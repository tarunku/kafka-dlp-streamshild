# Step 08 — Create Kafka Topics

Topics are where messages live in Kafka — think of a topic as a named channel or queue. Producers write messages *to* a topic; consumers read messages *from* a topic. In this POC we create one topic (`raw-events`) that carries order events from the producer to the consumer.

---

## 1. Navigate to Topics in the Kafka Cluster

1. In the GCP Console, open the **Navigation Menu** (three-line icon, top-left).

2. Scroll down to the **"Analytics"** section and click **"Managed Service for Apache Kafka"**.

   > **Can't find it?** Use the search bar at the top of the Console and type `Managed Kafka` — it will appear in the results.

3. In the Managed Kafka page, click on `poc-kafka-cluster` to open the cluster detail view.

4. Click the **"Topics"** tab near the top of the cluster detail page.

---

## 2. Create the `raw-events` Topic

1. Click **"Create Topic"** (top-right of the Topics tab).

2. Fill in the topic configuration:

   - **Topic name**: `raw-events`

     > Use this exact name — the producer and consumer scripts in Step 09 hardcode this value.

   - **Partitions**: `3`

     > **What are partitions?** A partition is an ordered, immutable log of messages. Kafka splits a topic into multiple partitions so that multiple consumers can read in parallel. 3 partitions means up to 3 consumers can process messages simultaneously. For this POC, 3 is a good starting point.

   - **Replication factor**: `3`

     > **What is replication factor?** Each partition is copied to this many brokers. With a replication factor of 3 (matching the number of brokers in a standard Managed Kafka cluster), every partition has a copy on every broker. This means the topic survives a broker failure without data loss.

   - **Retention (ms)**: `604800000`

     > This is 7 days expressed in milliseconds. Here's the conversion:
     > - 7 days × 24 hours × 60 minutes × 60 seconds × 1000 ms = **604,800,000 ms**
     >
     > After 7 days, old messages are automatically deleted to free up storage. Adjust this for production based on your storage budget and replay requirements.
     >
     > If the console provides a human-readable duration picker (e.g., "7 days"), use that instead and skip the numeric value.

   - **Cleanup policy**: `delete`

     > The `delete` policy removes messages older than the retention period. The alternative (`compact`) keeps only the latest message per key — not needed for this POC.

   - Leave all other settings (segment size, max message size, etc.) as their **defaults**.

3. Click **"Create"** (or **"Save"**).

---

## 3. Verify the Topic Was Created

After creation, you should be returned to the Topics list. Confirm the following:

| Field | Expected value |
|---|---|
| Topic name | `raw-events` |
| Partitions | `3` |
| Status | **Active** (green) |

Click on `raw-events` to open its detail view and confirm the partition count and replication factor shown match what you entered.

> **If the topic does not appear after 30 seconds**, refresh the page. Topic creation is near-instant on Managed Kafka, so a missing topic usually means a page cache issue.

---

## 4. (Optional) Future Topics — Phase 2 Additions

Do **not** create these now. They are listed here as a reference for when the POC expands:

- **`dlq-events`** — Dead Letter Queue. Messages that fail processing (e.g., schema validation errors, downstream timeouts) are routed here instead of being silently dropped. Allows reprocessing failed events without re-running the full pipeline.

- **`enriched-events`** — Post-tokenization events. After the consumer processes a raw order event (e.g., replaces PII with tokens), it publishes the enriched version to this topic for downstream analytics consumers.

Both topics would follow the same creation steps above, with appropriate retention and partition settings for their expected volume.

---

## 5. What's Next

The `raw-events` topic is ready to receive messages. Next:

- **Step 09** — Write and run the Avro producer and consumer Python scripts on the GCE VM
