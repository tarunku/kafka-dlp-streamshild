# Step 05 — Confluent Cloud Schema Registry Setup

## Overview

### What is Schema Registry?

Schema Registry is a service that stores and manages **Avro schemas** — the "contracts" that define the exact structure of messages flowing through Kafka (field names, data types, required vs. optional fields, etc.).

When a producer writes a message to Kafka, it registers the schema with Schema Registry and embeds a schema ID in the message. When a consumer reads that message, it looks up the schema by ID and validates the message against it. If a producer tries to send a message that violates the schema, the write is rejected before it ever reaches Kafka.

**Why this matters for a POC:** Without Schema Registry, a buggy producer could silently write malformed data that only causes errors much later — potentially in Snowflake or downstream analytics. Schema Registry catches these errors immediately at write time.

### Why Confluent Cloud?

GCP's Managed Service for Apache Kafka does not include a built-in Schema Registry. Confluent Cloud — the company that created Kafka — offers Schema Registry as a standalone service with a **free tier**, making it the standard choice for this hybrid architecture. You are only using Confluent for Schema Registry, not for Kafka itself.

---

## 1. Create a Confluent Cloud Account

1. Open a new browser tab and go to **[confluent.io](https://www.confluent.io)**.
2. Click **Try Free** or **Sign Up** in the top-right corner.
3. Fill in your name, email address, and a password. Use the same email you use for this project (`tarunkumar@fusionleap.io` or your preferred address).
4. Click **Start Free**.
5. Check your inbox for a verification email from Confluent and click the confirmation link.
6. After verification, go to **[confluent.cloud](https://confluent.cloud)** and log in.

> **Tip:** Confluent Cloud may offer a $400 free trial credit when you sign up. Accept it — you will not be charged during the POC as long as you stay within free-tier Schema Registry usage.

---

## 2. Create an Environment

In Confluent Cloud, an **Environment** is a logical container that groups resources together (similar to a GCP project). You need at least one environment before you can enable Schema Registry.

1. After logging in, you will land on the Confluent Cloud home dashboard.
2. In the left sidebar or at the top, look for **Environments** and click it.
3. Click **Add Environment** (or **Create Environment** — the button label may vary).
4. Fill in the details:

   | Field | Value |
   |---|---|
   | **Environment name** | `poc-environment` |
   | **Cloud provider** | Google Cloud |
   | **Region** | `us-central1 (Iowa)` |

5. Click **Create**.

> **Important:** You are creating an Environment to host Schema Registry only. You are **not** creating a Confluent Kafka cluster — your Kafka cluster is on GCP Managed Kafka from Step 04. Do not click "Create Cluster" anywhere in these steps.

---

## 3. Enable Schema Registry

1. After the environment is created, you will be inside `poc-environment`.
2. Look at the **right panel** or the **Environment Settings** tab for a section called **Schema Registry**.
3. Click **Enable Schema Registry** (or **Set up on my own** if that option appears instead).
4. A dialog will ask you to choose a cloud provider and region:

   | Field | Value |
   |---|---|
   | **Cloud provider** | Google Cloud |
   | **Region** | `us-central1` |

5. Click **Enable**.
6. After a few seconds, Schema Registry will be provisioned. You will see a **Schema Registry Endpoint URL** appear. It looks like this:

   ```
   https://psrc-xxxxx.us-central1.gcp.confluent.cloud
   ```

   The `xxxxx` part will be a unique identifier for your Schema Registry instance.

7. **Copy this URL and save it** — you will store it in Secret Manager in Step 06.

> **Tip:** If you navigate away before copying the URL, you can always find it again by going to **Environments** > **`poc-environment`** > **Schema Registry** tab.

---

## 4. Create Schema Registry API Key

Applications authenticate to Schema Registry using an API Key and Secret pair. You need to create one now.

1. While inside `poc-environment`, look for the **Schema Registry** panel or navigate to **Schema Registry** settings within the environment.
2. Look for a button labeled **API Keys**, **Add Key**, or a key icon — it may also be accessible from the top-right **API Keys** menu.
3. Click **Add Key** or **Create API Key**.
4. When prompted to choose a scope, select **Schema Registry** (not "Kafka cluster" or "Cloud resource management").
5. In the **Description** field, type: `poc-schema-registry-key`
6. Click **Create** (or **Next**, then **Create**).
7. Confluent will display your new API Key and Secret:

   - **API Key** — a short alphanumeric string, for example: `ABCDEF123456`
   - **API Secret** — a long random string

> **CRITICAL: Copy both values NOW.** The API Secret is shown **only once**. Once you close or dismiss this dialog, the secret cannot be retrieved again — you would have to delete the key and create a new one. Paste both values into a secure temporary location (you will move them to Secret Manager shortly).

8. Click **I have saved my API key and secret** (or similar confirmation) to dismiss the dialog.

---

## 5. Verify Schema Registry is Active

Before moving on, confirm everything looks correct:

1. Return to the `poc-environment` overview page.
2. In the **Schema Registry** section, confirm:
   - Status shows **Running** or **Active**
   - The **Endpoint URL** is visible (starting with `https://psrc-`)
3. Under **API Keys** or **Schema Registry** settings, your key `poc-schema-registry-key` should be listed.

---

## 6. What to Save for Secret Manager

You should now have three values ready. Keep them in a secure place — they will all be entered into GCP Secret Manager in the next step:

| Item | Description | Example format |
|---|---|---|
| **Schema Registry Endpoint URL** | The HTTPS endpoint for your SR instance | `https://psrc-xxxxx.us-central1.gcp.confluent.cloud` |
| **API Key** | Short alphanumeric key | `ABCDEF123456` |
| **API Secret** | Long random secret string | `abc123xyz789...` |

> **Do not store these values in code, config files, environment variables on your laptop, or anywhere other than Secret Manager.** Step 06 covers the correct way to store and manage them.

---

## What's Next

You now have:
- A Confluent Cloud Schema Registry instance running in `us-central1`
- A Schema Registry API Key and Secret for authentication

Next: **[Step 06 — Google Secret Manager: Store All Credentials](./06-secret-manager.md)**

Step 06 walks you through creating all the secrets your pipeline needs (Kafka, Schema Registry, and Snowflake) in GCP Secret Manager, and granting your service accounts access to read them at runtime.
