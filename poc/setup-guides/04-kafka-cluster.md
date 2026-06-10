# Step 04 — Provision GCP Managed Kafka Cluster

## Overview

Google's **Managed Service for Apache Kafka** is a fully managed Kafka offering — GCP handles broker provisioning, patching, scaling, and availability. You never SSH into a Kafka broker or manage JVM tuning. The cluster runs privately inside your VPC, so it is not reachable from the public internet.

In this step you will create a 3-broker Kafka cluster named `poc-kafka-cluster` in `us-central1`, attached to `poc-vpc`.

---

## 1. Navigate to Managed Kafka

1. In the GCP Console, click the **search bar** at the top of the page (or press `/`).
2. Type `Managed Kafka` and select **Managed Service for Apache Kafka** from the results.
3. Alternatively, open the **Navigation Menu** (☰, top-left) > **Analytics** > **Managed Service for Apache Kafka**.
4. If you see a prompt saying the API is not enabled, click **Enable API** and wait 1–2 minutes for it to activate before continuing.

---

## 2. Create a Kafka Cluster

1. On the Managed Service for Apache Kafka page, click **Create Cluster**.
2. Fill in the following fields:

   | Field | Value |
   |---|---|
   | **Cluster name** | `poc-kafka-cluster` |
   | **Region** | `us-central1` |

3. Leave all other fields on this page at their defaults.
4. Click **Next** to proceed to capacity configuration.

---

## 3. Configure Cluster Capacity

Kafka achieves high availability by replicating data across multiple brokers. Three brokers is the minimum required to maintain a replication factor of 3, which means your data survives a single broker failure without any message loss.

Configure the capacity settings as follows:

| Field | Recommended value |
|---|---|
| **Number of brokers** | `3` |
| **vCPUs per broker** | Minimum available (typically **3 vCPU**) |
| **Memory per broker** | Minimum available |
| **Storage per broker** | Minimum available (typically **100 GB**) |

> **Cost warning:** Even at minimum sizing, a 3-broker Managed Kafka cluster costs approximately **$1.50–$2.00 per hour** while running. This adds up to ~$36–$48 per day. **Delete or pause the cluster when you are not actively testing** to avoid unexpected charges during this POC.

---

## 4. Configure Networking

This section attaches your Kafka cluster to your existing VPC so that only resources inside the VPC can reach it.

1. Under **VPC Network**, use the dropdown to select **`poc-vpc`**.
2. Under **Subnet**, select **`poc-subnet`** (CIDR `10.0.0.0/24`).
3. **IP allocation / PSC endpoint:** Leave this as the default. GCP automatically manages the Private Service Connect (PSC) endpoint IP addresses — you do not need to assign IPs manually.
4. Click **Next**.

> **Why no public access?** Kafka is intentionally not exposed to the internet. Only GCE VMs and Dataflow jobs that run inside `poc-vpc` will be able to connect. This is a security best practice — Kafka should never be reachable from outside your private network.

---

## 5. Review and Create

1. On the **Review** page, check the summary:
   - Cluster name: `poc-kafka-cluster`
   - Region: `us-central1`
   - Brokers: 3
   - Network: `poc-vpc` / `poc-subnet`
2. If everything looks correct, click **Create**.
3. The cluster status will immediately show as **Creating**. Provisioning takes approximately **10–15 minutes**.
4. Do not navigate away from this page while the cluster is being created — you can refresh the page to check progress.
5. Once provisioning is complete, the status changes to **Active** (shown with a green checkmark).

---

## 6. Capture the Bootstrap Server Address

The bootstrap server address is the connection string your producers and consumers will use to reach Kafka. You need to copy it now and store it in Secret Manager in the next step.

1. Once the cluster status shows **Active**, click on **`poc-kafka-cluster`** to open the cluster details.
2. Look for a tab or section labeled **Bootstrap**, **Connection**, or **Connectivity** (the exact label may vary slightly by console version).
3. Copy the bootstrap server address. It will look like this:

   ```
   bootstrap.poc-kafka-cluster.us-central1.managedkafka.PROJECT_ID.cloud.goog:9092
   ```

   Replace `PROJECT_ID` with your actual GCP project ID (`kafka-poc`). The full address should end in `:9092`.

4. Paste this address into a temporary notes file — you will store it as a secret in **Step 06**.

> **Tip:** Do not modify the bootstrap address. Copy it exactly as shown in the console, including the full domain and port number.

---

## 7. Verify Cluster is Healthy

Before moving on, confirm:

- [ ] Cluster status shows **Active**
- [ ] The cluster details page shows **3 brokers**, all in a healthy state
- [ ] The bootstrap server address is visible and copied

---

## What's Next

You now have a running, private, 3-broker Kafka cluster inside your VPC.

Next: **[Step 05 — Confluent Cloud Schema Registry Setup](./05-confluent-schema-registry.md)**

Schema Registry ensures that every message written to Kafka follows a defined Avro schema — preventing bad or malformed data from ever entering the pipeline.
