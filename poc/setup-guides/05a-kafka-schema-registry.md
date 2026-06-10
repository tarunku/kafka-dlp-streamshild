# Step 05a — GCP Managed Kafka Schema Registry Setup (Preview)

## Overview

### What is this?

This is an **alternative to Step 05** (Confluent Cloud Schema Registry). Instead of creating an external Confluent Cloud account, you provision a Schema Registry directly inside GCP's Managed Service for Apache Kafka — keeping your entire pipeline within a single cloud.

> **Preview status:** As of May 2026, GCP Managed Kafka Schema Registry is in **Public Preview (Pre-GA)**. It is fully functional and supported under Google's Pre-GA Offering Terms, but is subject to API changes before General Availability. For this POC, the trade-off is acceptable.

### Why choose this over Step 05?

| Factor | Step 05 (Confluent) | Step 05a (GCP Native) |
|---|---|---|
| **Account required** | Confluent Cloud account | None (uses existing GCP project) |
| **Authentication** | API key + secret (2 extra secrets) | VM service account OAuth token (already configured) |
| **Secret Manager secrets** | 10 total | **8 total** (drop `schema-registry-api-key` and `schema-registry-api-secret`) |
| **Wire format** | Confluent (0x00 + schema ID) | Confluent-compatible — identical |
| **Avro support** | Yes | Yes |
| **Maturity** | GA | Preview |
| **Cost** | Free tier | Included in Managed Kafka pricing |

The code changes are minimal: only the Schema Registry authentication method changes — from HTTP Basic auth (key + secret) to Bearer token (GCP OAuth, already used for Kafka itself).

---

## Prerequisites

- Step 04 (Kafka cluster) must be complete — the Schema Registry is provisioned within the same GCP Managed Kafka service.
- The `vm-producer-sa` service account must exist (created in Step 03).
- You must have the **Managed Kafka Admin** IAM role (or `roles/owner`) on the GCP project to create a schema registry.

---

## 1. Grant IAM Roles to `vm-producer-sa`

The VM's service account needs permission to read and write schemas at runtime. Do this before creating the registry.

1. In the GCP Console, open the **Navigation Menu** (☰) > **IAM & Admin** > **IAM**.
2. Find `vm-producer-sa@kafka-poc.iam.gserviceaccount.com` in the list.
3. Click its **pencil icon** (Edit principal).
4. Click **Add Another Role**.
5. In the role search box, type `Managed Kafka` and look for a role containing **Schema Registry**:
   - For the producer (reads + writes schemas): select **Managed Kafka Schema Registry Editor**
   - If no such role appears, select **Managed Kafka Admin** as a fallback for the POC.
6. Click **Save**.

> **Why no separate consumer role?** For this POC, `vm-producer-sa` runs both producer and consumer. A production setup would use a separate `vm-consumer-sa` with **Managed Kafka Schema Registry Viewer** (read-only).

---

## 2. Create the Schema Registry

1. In the GCP Console search bar, type `Managed Kafka` and open **Managed Service for Apache Kafka**.
2. In the left sidebar, click **Schema Registries** (you may need to expand the menu).
3. Click **Create Schema Registry**.
4. Fill in the fields:

   | Field | Value |
   |---|---|
   | **Name** | `poc-schema-registry` |
   | **Region** | `us-central1` |

5. Leave all other settings at their defaults.
6. Click **Create**. Provisioning takes 1–2 minutes.
7. Once the status shows **Active**, click into `poc-schema-registry` to open its detail page.
8. Locate the **Endpoint** field. It will look like:

   ```
   https://managedkafka.googleapis.com/v1main/projects/kafka-poc/locations/us-central1/schemaRegistries/poc-schema-registry
   ```

9. **Copy this URL and save it** — you will store it in Secret Manager in the next step.

> **Tip:** The endpoint always follows this pattern: `https://managedkafka.googleapis.com/v1main/projects/{PROJECT}/locations/{REGION}/schemaRegistries/{REGISTRY_NAME}`. If the console does not display it prominently, construct it from the resource name components shown on the detail page.

---

## 3. Update Secret Manager

If you have already completed Step 06, update the `schema-registry-url` secret and skip the two key/secret secrets. If you have not yet done Step 06, note what has changed.

### If Step 06 is already done — update the URL secret

1. In the GCP Console, open **Security** > **Secret Manager**.
2. Click `schema-registry-url`.
3. Click **New Version**.
4. Paste the GCP endpoint URL from Step 2 above (the `https://managedkafka.googleapis.com/...` URL).
5. Click **Add New Version**.

### Delete the two Confluent secrets (they are no longer needed)

GCP authentication uses the VM's service account OAuth token, so there is no API key or secret to store.

1. In Secret Manager, check the box next to `schema-registry-api-key`.
2. Click **Delete** > confirm.
3. Repeat for `schema-registry-api-secret`.

> **If you have not done Step 06 yet:** When you reach Step 06, create only **8 secrets** — omit `schema-registry-api-key` and `schema-registry-api-secret` entirely. The total secret count drops from 10 to 8.

### Revised secret list for Step 06

| Secret Name | Source |
|---|---|
| `kafka-bootstrap-servers` | Step 04 — Managed Kafka cluster |
| `schema-registry-url` | This step — GCP Schema Registry endpoint |
| `snowflake-account` | Step 10 |
| `snowflake-user` | Step 10 |
| `snowflake-password` | Step 10 |
| `snowflake-database` | Step 10 |
| `snowflake-schema` | Step 10 |
| `snowflake-warehouse` | Step 10 |

---

## 4. Install Additional Dependency on the VM

On the GCE VM (`poc-dev-vm`), with the virtual environment active:

```bash
cd ~/kafka-poc
source venv/bin/activate
pip install requests
```

> `requests` is likely already installed as a transitive dependency of `confluent-kafka`. Run `python3 -c "import requests; print('ok')"` to check — install only if that fails.

No new authentication library is needed. The Bearer token is fetched directly from the GCE metadata server using `requests`, the same HTTP library already used for Schema Registry calls.

---

## 5. Updated `producer.py`

The only change from the Step 09 version is in the **credentials section** and the **schema registration function**. Everything else — Kafka config, serialization, message loop — is identical.

Replace the relevant sections in `~/kafka-poc/producer.py` as follows.

### 5a. Credentials section (replace lines that load schema registry key/secret)

Remove:
```python
schema_registry_key    = get_secret(PROJECT_ID, "schema-registry-api-key")
schema_registry_secret = get_secret(PROJECT_ID, "schema-registry-api-secret")
```

The credentials section becomes:
```python
print("Loading credentials from Secret Manager...")
bootstrap_servers   = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
print("Credentials loaded.")
```

### 5b. Add a token helper function (add before `get_or_register_schema`)

```python
def get_gcp_bearer_token() -> str:
    """Fetch a short-lived OAuth2 access token from the GCE metadata server."""
    resp = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/"
        "service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]
```

### 5c. Updated `get_or_register_schema` function (swap Basic auth → Bearer token)

Replace the function body so it reads:
```python
def get_or_register_schema(
    registry_url: str,
    subject: str,
    schema: dict,
) -> int:
    """
    Registers the schema under the given subject if it does not exist yet.
    Returns the schema ID assigned by Schema Registry.
    """
    token = get_gcp_bearer_token()
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(schema), "schemaType": "AVRO"}
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        },
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    schema_id = response.json()["id"]
    print(f"Schema registered/found — ID: {schema_id}")
    return schema_id
```

### 5d. Update the call site (remove the key/secret arguments)

Change:
```python
schema_id = get_or_register_schema(
    schema_registry_url,
    schema_registry_key,
    schema_registry_secret,
    SUBJECT,
    ORDER_EVENT_SCHEMA,
)
```

To:
```python
schema_id = get_or_register_schema(
    schema_registry_url,
    SUBJECT,
    ORDER_EVENT_SCHEMA,
)
```

---

## 6. Updated `consumer.py`

Same pattern: remove key/secret loading, swap Basic auth for Bearer token in the schema lookup.

### 6a. Credentials section

Remove:
```python
schema_registry_key    = get_secret(PROJECT_ID, "schema-registry-api-key")
schema_registry_secret = get_secret(PROJECT_ID, "schema-registry-api-secret")
```

The credentials section becomes:
```python
print("Loading credentials from Secret Manager...")
bootstrap_servers   = get_secret(PROJECT_ID, "kafka-bootstrap-servers")
schema_registry_url = get_secret(PROJECT_ID, "schema-registry-url")
print("Credentials loaded.\n")
```

### 6b. Add the same token helper (add before `get_schema_by_id`)

```python
def get_gcp_bearer_token() -> str:
    """Fetch a short-lived OAuth2 access token from the GCE metadata server."""
    resp = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/instance/"
        "service-accounts/default/token",
        headers={"Metadata-Flavor": "Google"},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]
```

### 6c. Updated `get_schema_by_id` function

Replace the function body:
```python
def get_schema_by_id(schema_id: int) -> object:
    """
    Fetches the Avro schema for the given schema ID from Schema Registry.
    Results are cached in memory for the lifetime of this process.
    """
    if schema_id in _schema_cache:
        return _schema_cache[schema_id]

    token = get_gcp_bearer_token()
    url = f"{schema_registry_url.rstrip('/')}/schemas/ids/{schema_id}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()
    raw_schema = json.loads(response.json()["schema"])
    parsed = fastavro.parse_schema(raw_schema)
    _schema_cache[schema_id] = parsed
    print(f"  [schema cache] Loaded schema ID {schema_id} from registry.")
    return parsed
```

> **Token lifetime:** GCE metadata server tokens are valid for approximately 1 hour. For this POC (short-lived processes), fetching a fresh token per Schema Registry call is fine. A long-running consumer in production should cache the token and refresh it before expiry.

---

## 7. Verify the Setup

After updating both scripts, run these checks from the VM.

### Confirm the Schema Registry is reachable

```bash
TOKEN=$(curl -s -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

SR_URL=$(python3 -c "
from utils import get_secret
print(get_secret('kafka-poc', 'schema-registry-url'))
")

curl -s -H "Authorization: Bearer $TOKEN" "$SR_URL/subjects"
```

**Expected output:** `[]` (empty array — no subjects registered yet). Any JSON response (including an empty array) confirms the registry is reachable and authentication works.

### Run the producer

```bash
cd ~/kafka-poc && source venv/bin/activate
python3 producer.py
```

**Expected output:** identical to Step 09, except the schema ID will be `1` (first schema registered in this registry):
```
Loading credentials from Secret Manager...
Credentials loaded.
Schema registered/found — ID: 1

Producing 10 messages to topic 'raw-events'...

Delivered: order_id=3f7a1c2e-...  to raw-events [partition=1]  offset=0
...
All messages delivered successfully.
```

### Run the consumer

```bash
python3 consumer.py
```

The consumer output is identical to Step 09. The only internal difference is it calls the GCP Schema Registry endpoint instead of Confluent.

---

## 8. Troubleshooting

**`403 Forbidden` when calling Schema Registry**
- The `vm-producer-sa` service account does not have the Schema Registry IAM role. Go to **IAM & Admin** > **IAM**, find `vm-producer-sa`, and add the **Managed Kafka Schema Registry Editor** role (or **Managed Kafka Admin** as a fallback).
- Verify the schema registry is in the same project (`kafka-poc`) as the service account.

**`404 Not Found` on Schema Registry URL**
- Double-check the URL stored in the `schema-registry-url` secret. It must be the full path including the registry name, for example: `https://managedkafka.googleapis.com/v1main/projects/kafka-poc/locations/us-central1/schemaRegistries/poc-schema-registry`.
- Confirm the registry exists and its status is **Active** in the GCP Console under **Managed Kafka** > **Schema Registries**.

**Metadata server timeout (`Connection refused` on `169.254.169.254`)**
- This call only works from inside a GCE VM. You cannot test `get_gcp_bearer_token()` from a local machine or Cloud Shell — it must run on `poc-dev-vm`.

**`KeyError: 'id'` in the response from Schema Registry**
- The `schemaType` field may be required. Confirm `"schemaType": "AVRO"` is present in the POST body (it is included in the updated `get_or_register_schema` function above).

**Consumer reads 0 messages after switching Schema Registries**
- The Schema Registry changed, but the Kafka messages still have the old Confluent schema ID embedded (e.g., ID `1` from the Confluent registry). The GCP registry has its own ID namespace starting from `1`. Re-run the producer once to publish new messages registered under the GCP registry, then re-run the consumer.

---

## What's Next

You now have a fully GCP-native schema registry. Your pipeline no longer depends on any external service.

When you reach **Step 06 — Secret Manager**, create only **8 secrets** (skip `schema-registry-api-key` and `schema-registry-api-secret`). All other steps remain unchanged.

Next: **[Step 06 — Google Secret Manager: Store All Credentials](./06-secret-manager.md)**
