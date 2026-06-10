# Step 12 — End-to-End Validation

This is the moment of truth. With the Kafka cluster running, Dataflow consuming from `raw-events`, and Snowflake waiting for rows — you will publish 10 test events from the producer, watch each system handle them in real time, and confirm the full pipeline is working correctly.

---

## Pre-flight Checklist

Before running the producer, confirm every prior step is complete. If any item is unchecked, return to the indicated step and resolve it first.

- [ ] GCP project `kafka-poc` created and all required APIs enabled (Step 01)
- [ ] VPC `poc-vpc` and subnet `poc-subnet` created with Private Google Access enabled (Step 02)
- [ ] Firewall rules in place (internal traffic, SSH, Kafka port 9092) (Step 02)
- [ ] Service accounts `dataflow-pipeline-sa` and `vm-producer-sa` created with correct IAM roles (Step 03)
- [ ] Kafka cluster `poc-kafka-cluster` status = **Active** (Step 04)
- [ ] Topic `raw-events` created with 3 partitions (Step 04)
- [ ] Confluent Schema Registry running, `OrderEvent` Avro schema registered, API key created (Step 05)
- [ ] All 10 secrets created and populated in Secret Manager (Steps 06 + 10)
- [ ] GCE VM `poc-dev-vm` running, Python virtual environment set up, `producer.py` deployed (Step 07)
- [ ] Snowflake: `POC_WH`, `POC_DB`, `KAFKA_INGEST` schema, and `ORDER_EVENTS` table all created (Step 10)
- [ ] Dataflow job `kafka-to-snowflake-poc` status = **Running** (Step 11)

---

## Step 1 — Publish Events from the Producer

1. Open the GCP Console and navigate to **Compute Engine > VM Instances**.
2. Click **SSH** next to `poc-dev-vm` to open a browser-based terminal (or connect via VS Code Remote SSH).
3. In the terminal, activate the virtual environment and run the producer:

```bash
cd ~/kafka-poc
source venv/bin/activate
python3 producer.py
```

4. **Expected output** — you should see 10 lines similar to:

```
Delivered: order_id=ORD-001 to [raw-events partition=1 offset=0]
Delivered: order_id=ORD-002 to [raw-events partition=0 offset=0]
Delivered: order_id=ORD-003 to [raw-events partition=2 offset=0]
...
Delivered: order_id=ORD-010 to [raw-events partition=1 offset=3]
Producer finished. 10 events published.
```

5. The producer script exits cleanly after all 10 events are delivered. If you see errors instead, jump to the [Troubleshooting Guide](#troubleshooting-guide) below.

> **What the producer does:** `producer.py` reads the Kafka bootstrap servers and Schema Registry credentials from Secret Manager, serializes 10 `OrderEvent` Avro messages, and publishes them to the `raw-events` topic. Each message is confirmed as delivered before the next is sent.

---

## Step 2 — Verify Events in Kafka

1. In the GCP Console, navigate to **Managed Service for Apache Kafka** (search in the top bar).
2. Click on cluster **`poc-kafka-cluster`**.
3. Click **Topics** in the left panel.
4. Click on **`raw-events`**.
5. In the topic detail view, check the **Message count** or **Offset** metrics — they should now show `10` (or reflect an increase of 10 from before you ran the producer).

> **Note:** The Managed Kafka console may show a short delay (up to 30 seconds) before metrics refresh. If the count still shows 0, wait briefly and refresh the page.

---

## Step 3 — Watch Dataflow Process Events

1. In the GCP Console, navigate to **Dataflow > Jobs**.
2. Click on **`kafka-to-snowflake-poc`** to open the job detail page.
3. In the pipeline graph, look at the step metrics on each node:
   - The **"Read from Kafka"** step should show **elements added: 10**
   - The **"Write to Snowflake"** step should show **elements written: 10**
4. Check the **System lag** metric — it should be near `0s` for this low-volume POC workload.
5. Scroll down to the **Logs** panel. Confirm there are no `ERROR` entries. Informational logs about Kafka consumer group rebalancing are normal.

> **Processing delay:** Dataflow operates in micro-batches by default. There may be a 10–30 second lag between the producer delivering events to Kafka and Dataflow flushing them to Snowflake. This is normal — system lag measures how far behind the pipeline is, not absolute time.

---

## Step 4 — Confirm Data in Snowflake

1. Open **Snowsight** (your Snowflake UI) in the browser.
2. Click **Worksheets** in the left nav and open a worksheet.
3. Set context: **Warehouse** = `POC_WH`, **Database** = `POC_DB`, **Schema** = `KAFKA_INGEST`.
4. Run the following query:

```sql
SELECT *
FROM POC_DB.KAFKA_INGEST.ORDER_EVENTS
ORDER BY INGESTED_AT DESC
LIMIT 20;
```

5. **Expected result:** 10 rows returned, with each row containing values in all columns matching the Avro schema fields:
   - `ORDER_ID` — e.g., `ORD-001`
   - `CUSTOMER_ID` — e.g., `CUST-42`
   - `PRODUCT_ID` — e.g., `PROD-007`
   - `AMOUNT` — a numeric float value
   - `CURRENCY` — e.g., `USD`
   - `TIMESTAMP` — a Unix epoch long value
   - `STATUS` — e.g., `PLACED`
   - `INGESTED_AT` — a `TIMESTAMP_NTZ` showing when Dataflow inserted the row

6. If rows are visible, the end-to-end pipeline is working correctly.

---

## Success Criteria Summary

| Check | Expected Result |
|---|---|
| Producer script output | Exits cleanly with 10 `"Delivered"` confirmation lines |
| Kafka topic message count | `raw-events` shows 10 messages |
| Dataflow elements added | 10 elements processed, system lag near `0s` |
| Snowflake row count | 10 rows in `POC_DB.KAFKA_INGEST.ORDER_EVENTS` |

If all four checks pass, the POC pipeline is fully functional end-to-end.

---

## Troubleshooting Guide

### Problem: Producer fails with authentication error

- **Check 1:** In the GCP Console, go to **IAM & Admin > IAM**. Find `vm-producer-sa@kafka-poc.iam.gserviceaccount.com` and confirm it has the **Managed Kafka Client** role assigned.
- **Check 2:** Confirm the VM `poc-dev-vm` is in subnet `poc-subnet` of VPC `poc-vpc` — the same VPC as the Kafka cluster. Go to **Compute Engine > VM Instances**, click `poc-dev-vm`, and verify the network interface.

---

### Problem: Producer fails with "Secret not found" or permission denied on Secret Manager

- **Check 1:** In **Secret Manager**, verify that each secret name matches exactly what the producer script expects — names are case-sensitive and must not contain trailing spaces.
- **Check 2:** In **IAM & Admin > IAM**, confirm `vm-producer-sa` has the **Secret Manager Secret Accessor** role at the project level (not just on individual secrets).

---

### Problem: Schema Registry returns 401 Unauthorized

- **Check 1:** In **Secret Manager**, open the `schema-registry-api-key` and `schema-registry-api-secret` secrets and verify the values are correct and not truncated.
- **Check 2:** Log in to **Confluent Cloud** and confirm the Schema Registry cluster status is **Active** and the API key is enabled (not revoked).

---

### Problem: Dataflow job is stuck at "Starting" or fails immediately after launch

- **Check 1:** In **Cloud Storage**, confirm the staging bucket `kafka-poc-dataflow-temp-kafka-poc` exists in region `us-central1`.
- **Check 2:** In the bucket's **Permissions** tab, confirm `dataflow-pipeline-sa@kafka-poc.iam.gserviceaccount.com` has the **Storage Object Admin** role.
- **Check 3:** In **VPC Network > Subnets**, click `poc-subnet` and verify **Private Google Access** is set to `On`. Without this, workers on private IPs cannot reach Google APIs.
- **Check 4:** In **APIs & Services > Enabled APIs**, confirm the **Dataflow API** is enabled. Search for `dataflow.googleapis.com`.

---

### Problem: Dataflow job is Running but the Snowflake table remains empty

- **Check 1:** In **Secret Manager**, verify all Snowflake secrets (`snowflake-account`, `snowflake-user`, `snowflake-password`, etc.) contain correct, current values. A stale placeholder from Step 06 would cause silent authentication failures.
- **Check 2:** In **Snowsight**, run the following to confirm `DATAFLOW_USER` has the correct INSERT permission:
  ```sql
  SHOW GRANTS TO USER DATAFLOW_USER;
  ```
  Confirm `DATAFLOW_ROLE` is listed and that the role has `INSERT` on `ORDER_EVENTS`.
- **Check 3:** Run any query in Snowsight against `POC_DB.KAFKA_INGEST.ORDER_EVENTS` to wake up the `POC_WH` warehouse. If the warehouse is suspended, the Dataflow connector may time out on its first write attempt. Once the warehouse is active, Dataflow's next flush attempt should succeed.

---

### Problem: Consumer shows no messages when running `consumer.py`

- **Check 1:** Verify the consumer config has `auto_offset_reset = "earliest"`. If set to `"latest"`, the consumer only reads messages published after it starts — it will miss the 10 events already in the topic.
- **Check 2:** If the consumer group has previously committed offsets to offset `10` (from a prior run), it will appear to have nothing to read. Use a new, unique `group.id` value to reset to the beginning, or manually reset offsets in the Managed Kafka console.

---

## How to Shut Down Everything (Stop Billing)

When the POC is complete, shut down resources in this priority order — stopping the most expensive services first.

> **Warning:** The Kafka cluster **cannot be paused** — it must be fully deleted to stop incurring charges. Re-creating the cluster from scratch takes 10–15 minutes. Do not delete until you are certain you no longer need the POC running.

1. **Dataflow job** — Navigate to **Dataflow > Jobs**, select `kafka-to-snowflake-poc`, and click:
   - **"Drain"** — graceful shutdown; waits for in-flight elements to finish processing before stopping. Preferred when data integrity matters.
   - **"Cancel"** — immediate shutdown; any in-flight elements are discarded. Use when you just want to stop billing quickly.

2. **Kafka cluster** — Navigate to **Managed Service for Apache Kafka**, click `poc-kafka-cluster`, and click **"Delete"**. Confirm deletion in the prompt. Billing stops immediately.

3. **GCE VM** — Navigate to **Compute Engine > VM Instances**, select `poc-dev-vm`, and click **"Stop"** (not Delete). A stopped VM does not charge for compute — only for the attached boot disk (~$0.40/month for a 50 GB standard disk). You can restart it later to re-run the producer.

4. **Snowflake warehouse** — No action needed. `POC_WH` is configured to auto-suspend after 1 minute of inactivity. Snowflake does not charge for a suspended warehouse.

5. **Confluent Schema Registry** — If you are on the Confluent Cloud free tier, no action is needed. If you provisioned a paid cluster, log in to Confluent Cloud and pause or delete the environment.

6. **GCS staging bucket** — Optional. The bucket holds only small temporary files. Storage cost is negligible (~$0.02/GB/month). You can delete it via **Cloud Storage > Buckets** if you want a completely clean teardown.

---

## Phase 1 Complete — What's Next (Phase 2 Preview)

With all 12 steps done, you have a working end-to-end streaming pipeline:

**Avro Producer → Managed Kafka → Dataflow → Snowflake**

Phase 2 will harden and extend this foundation:

- **PII tokenization via Cloud DLP** — integrate Google's Data Loss Prevention API into the Dataflow pipeline to detect and tokenize sensitive fields before they reach Snowflake
- **Dead Letter Queue (DLQ) topic** — route malformed or unprocessable events to a separate `raw-events-dlq` Kafka topic instead of silently dropping them, enabling replay and debugging
- **Cloud Monitoring dashboard** — build a unified observability dashboard covering Kafka consumer lag, Dataflow throughput, Snowflake ingestion latency, and error rates — all in a single GCP console view
- **Schema evolution strategy** — design a process for adding or modifying fields in the `OrderEvent` Avro schema without breaking the running pipeline, using Confluent Schema Registry compatibility modes (`BACKWARD`, `FORWARD`, `FULL`)
