# Step 10 — Snowflake: Account, Database & Target Table

Snowflake is our analytics destination. We set up a minimal environment — one warehouse, one database, one table — that Dataflow will stream events into. All steps are performed in the Snowflake web UI (Snowsight) and the GCP Console. No CLI required.

---

## Create a Snowflake Trial Account

1. Open a browser and go to [snowflake.com](https://www.snowflake.com).
2. Click **"Start for Free"**.
3. Fill in your name, email, company, and choose a password.
4. On the cloud/region selection screen:
   - **Cloud Provider**: Google Cloud Platform
   - **Region**: `us-central1 (Iowa)`
5. Click **"Continue"** and follow the remaining prompts.
6. Check your email for a verification link and click it to activate your account.
7. Log in to **Snowsight** (the Snowflake web UI) at the URL in the activation email — it will look like `https://ACCOUNT_ID.snowflakecomputing.com`.
8. **Note your account identifier** — it is visible in the browser URL bar and also under **Admin > Accounts** in the left nav. It typically takes the form `abc12345.us-central1.gcp`. You will need this value later when configuring Secret Manager.

---

## Create a Virtual Warehouse

A virtual warehouse provides the compute resources for running queries and loading data.

1. In Snowsight, click **Admin** in the left navigation panel.
2. Click **Warehouses**.
3. Click **"+ Warehouse"** (top-right).
4. Fill in the form:
   - **Name**: `POC_WH`
   - **Size**: `X-Small`
   - **Auto-suspend**: `1` minute

     > **Cost tip:** Setting auto-suspend to 1 minute means the warehouse shuts down after 60 seconds of inactivity. This is critical for a POC to avoid unnecessary charges — a running warehouse costs credits even with no queries.

   - **Auto-resume**: `On` (warehouse wakes automatically when a query arrives)
5. Leave all other settings at their defaults.
6. Click **"Create Warehouse"**.
7. Confirm `POC_WH` appears in the warehouses list with status **Started** or **Suspended**.

---

## Create a Database and Schema

1. In the left nav, click **Data**, then click **Databases**.
2. Click **"+ Database"** (top-right).
3. Fill in:
   - **Name**: `POC_DB`
4. Click **"Create"**.
5. Click on **`POC_DB`** in the list to open it.
6. Click **"+ Schema"** (top-right of the schemas panel).
7. Fill in:
   - **Name**: `KAFKA_INGEST`
8. Click **"Create"**.
9. Confirm `KAFKA_INGEST` appears under `POC_DB` in the databases tree.

---

## Create the Target Table

1. In the left nav, click **Worksheets**.
2. Click **"+"** (top-right) to open a new worksheet.
3. In the worksheet toolbar, set the context selectors:
   - **Warehouse**: `POC_WH`
   - **Database**: `POC_DB`
   - **Schema**: `KAFKA_INGEST`
4. Paste the following SQL into the editor:

```sql
CREATE TABLE IF NOT EXISTS POC_DB.KAFKA_INGEST.ORDER_EVENTS (
    ORDER_ID    VARCHAR,
    CUSTOMER_ID VARCHAR,
    PRODUCT_ID  VARCHAR,
    AMOUNT      FLOAT,
    CURRENCY    VARCHAR,
    TIMESTAMP   NUMBER,
    STATUS      VARCHAR,
    INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

5. Click the blue **Run** button (the play icon, top-right of the editor).
6. Confirm the results panel at the bottom shows `Table ORDER_EVENTS successfully created.`
7. **Verify the table exists:**
   - In the left nav, go to **Data > Databases > POC_DB > KAFKA_INGEST > Tables**.
   - `ORDER_EVENTS` should appear in the list.
   - Click on it to confirm the column definitions match the Avro schema fields.

> **Schema note:** The `INGESTED_AT` column is not part of the Avro `OrderEvent` schema — it is added by the pipeline at insert time to record when each event arrived in Snowflake. This is useful for lag monitoring.

---

## Create a Dedicated Dataflow User

Never use the Snowflake `ACCOUNTADMIN` role for pipeline connections. We create a least-privilege user with only the permissions the pipeline needs.

1. Open a **new worksheet** (click **"+"** in the Worksheets panel).
2. Make sure the warehouse context is set to `POC_WH`.
3. Paste and run the following SQL **in a single execution** (select all, then click **Run**):

```sql
-- Create user for Dataflow pipeline
CREATE USER DATAFLOW_USER
    PASSWORD = 'StrongP@ssword123!'
    DEFAULT_WAREHOUSE = 'POC_WH'
    DEFAULT_NAMESPACE = 'POC_DB.KAFKA_INGEST'
    MUST_CHANGE_PASSWORD = FALSE;

-- Create a dedicated role
CREATE ROLE DATAFLOW_ROLE;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE POC_WH TO ROLE DATAFLOW_ROLE;

-- Grant database and schema access
GRANT USAGE ON DATABASE POC_DB TO ROLE DATAFLOW_ROLE;
GRANT USAGE ON SCHEMA POC_DB.KAFKA_INGEST TO ROLE DATAFLOW_ROLE;

-- Grant table-level DML permissions (insert + select only)
GRANT INSERT, SELECT ON TABLE POC_DB.KAFKA_INGEST.ORDER_EVENTS TO ROLE DATAFLOW_ROLE;

-- Assign role to user and set as default
GRANT ROLE DATAFLOW_ROLE TO USER DATAFLOW_USER;
ALTER USER DATAFLOW_USER SET DEFAULT_ROLE = DATAFLOW_ROLE;
```

4. Confirm each statement shows a success result in the results pane at the bottom.
5. **Note the password** you used (`StrongP@ssword123!` above is an example — use your own strong password). You will store it in Secret Manager in the next section.

> **Security note:** For a production system, replace password authentication with Snowflake key-pair authentication. For this POC, a strong password stored in Secret Manager is acceptable.

---

## Update Secret Manager with Snowflake Credentials

In Step 06 you created Snowflake secrets as empty placeholders. Now update each one with its real value.

1. Open the **GCP Console** in a new tab and navigate to **Secret Manager** (search for it in the top search bar).
2. For **each** of the following secrets, click the **secret name**, then click **"+ New Version"**, paste the value, and click **"Add New Version"**:

   | Secret Name | Value to paste |
   |---|---|
   | `snowflake-account` | Your account identifier, e.g. `abc12345.us-central1.gcp` |
   | `snowflake-user` | `DATAFLOW_USER` |
   | `snowflake-password` | The password you set in the `CREATE USER` statement above |
   | `snowflake-database` | `POC_DB` |
   | `snowflake-schema` | `KAFKA_INGEST` |
   | `snowflake-warehouse` | `POC_WH` |

3. After adding each new version, confirm the **Latest version** column shows `Version 2` (or higher) and status **Enabled**.

> **Account identifier format:** Snowflake account identifiers for GCP use the pattern `ACCOUNT_LOCATOR.REGION.gcp` (e.g., `xy12345.us-central1.gcp`). Do **not** include `https://` or `.snowflakecomputing.com` when storing in Secret Manager — just the raw identifier. The Dataflow template constructs the full URL itself.

---

## Verify

Before moving on, run a quick sanity check query in Snowsight to confirm the table is accessible with the correct structure.

1. Open a **new worksheet** and set context to `POC_WH` / `POC_DB` / `KAFKA_INGEST`.
2. Run:

```sql
SELECT * FROM POC_DB.KAFKA_INGEST.ORDER_EVENTS LIMIT 10;
```

3. The query should return **0 rows** — the table is empty and that is correct. Dataflow will populate it once the pipeline is running.
4. Confirm the column headers in the results match: `ORDER_ID`, `CUSTOMER_ID`, `PRODUCT_ID`, `AMOUNT`, `CURRENCY`, `TIMESTAMP`, `STATUS`, `INGESTED_AT`.

---

## What's Next

Snowflake is fully configured. The next step is **Step 11 — Google Cloud Dataflow: Kafka to Snowflake Pipeline**, where you launch the managed streaming job that reads from the `raw-events` Kafka topic and writes into `ORDER_EVENTS`.
