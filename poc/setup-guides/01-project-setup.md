# Step 01 — GCP Project Setup & API Enablement

This step creates a brand-new GCP project for the Kafka streaming POC and enables all the cloud APIs the architecture depends on. Complete this before any other setup step.

---

## Prerequisites

Before you begin, make sure you have:

- A Google account (personal or work Gmail)
- A credit card or billing account registered with Google Cloud (required to activate APIs — you will not be charged for simply enabling APIs)
- A browser pointed at [https://console.cloud.google.com](https://console.cloud.google.com)

> **Billing note:** Google Cloud requires an active billing account to enable most APIs, even during the free tier. You will only be charged for resources you actually create and run.

---

## Section 1: Create a New GCP Project

1. Go to [https://console.cloud.google.com](https://console.cloud.google.com) and sign in with your Google account.

2. At the very top of the page, you will see a project dropdown (it may show "Select a project" or a previous project name). Click on it.

3. In the popup window that appears, click **New Project** in the top-right corner.

4. Fill in the project details:
   - **Project name:** `kafka-poc`
   - **Project ID:** GCP will auto-generate a unique ID (e.g., `kafka-poc-382910`). You can click the pencil icon to customise it if you prefer a specific ID. Once created, the project ID cannot be changed.
   - **Organisation:** Leave this as-is unless your company has a Google Workspace organisation — in that case, select the appropriate organisation from the dropdown.
   - **Location / Folder:** Leave as **No organisation** or select your org folder if applicable.

5. Click **Create**.

6. Wait 20–30 seconds while GCP provisions the project. A notification bell in the top-right corner will show progress.

7. Once created, click the project dropdown again and select `kafka-poc` to switch into it. Confirm the project name appears in the top bar.

> **Tip:** Every subsequent step in these guides assumes you are inside the `kafka-poc` project. Always double-check the project name shown in the top bar before making changes.

---

## Section 2: Link a Billing Account

Without an active billing account linked, GCP will block API activation. Even if you plan to stay within free-tier limits, billing must be enabled.

1. In the left-side navigation menu, scroll down and click **Billing**. (If you don't see it, click the hamburger menu icon ☰ at the top left to expand the nav.)

2. If no billing account is linked, GCP will prompt you with a banner: "This project has no billing account." Click **Link a billing account**.

3. Select your existing billing account from the list, or click **Manage billing accounts** to create a new one if you haven't set one up yet.

4. Click **Set account**.

5. You should now see a Billing overview page showing the linked account name. The project is now billing-enabled.

> **Warning:** If you skip this step, the "Enable API" button in the next section will either fail silently or show an error. Always link billing first.

---

## Section 3: Enable Required APIs

GCP services are off by default. You must explicitly enable each API before you can use it.

1. In the left navigation menu, click **APIs & Services**, then click **Enabled APIs and services**.

2. At the top of the page, click **+ Enable APIs and Services**.

3. This opens the API Library. Use the search bar at the top to find and enable each API below, one at a time.

---

### APIs to Enable

Enable each of the following by searching for the exact name, clicking on the result, and then clicking the blue **Enable** button:

**API 1 — Kafka**
- Search for: `Cloud Managed Service for Apache Kafka API`
- Click the result, then click **Enable**
- Wait for the spinner to finish (can take 1–2 minutes)

**API 2 — Compute Engine (for the GCE VM)**
- Search for: `Compute Engine API`
- Click the result, then click **Enable**
- This one may take up to 2 minutes — it provisions the Compute Engine backend for your project

**API 3 — Dataflow**
- Search for: `Dataflow API`
- Click the result, then click **Enable**

**API 4 — Secret Manager**
- Search for: `Secret Manager API`
- Click the result, then click **Enable**

**API 5 — Cloud DLP (for future phase)**
- Search for: `Cloud Data Loss Prevention (DLP) API`
- Click the result, then click **Enable**

> **Note:** DLP is not used in the initial POC phase but is included now so you don't have to revisit this step later.

**API 6 — Cloud Resource Manager**
- Search for: `Cloud Resource Manager API`
- Click the result, then click **Enable**

> **Tip:** If you see a message like "API already enabled", that's fine — it's already active and you can move on.

---

## Section 4: Default Region — Important Note

GCP does not have a single global "default region" setting in the web console. Each service asks you to choose a region when you create a resource.

**Throughout this entire POC, always select `us-central1` as the region in every service, every screen.**

This ensures all resources are co-located, which reduces latency and avoids cross-region data transfer costs.

> **Reminder:** When you see a Region dropdown in any GCP service, always choose `us-central1`. Do not leave it on the default suggestion — double-check every time.

---

## Section 5: Verify Everything is Ready

Before moving to the next step, confirm all 6 APIs are active:

1. In the left nav, click **APIs & Services** > **Enabled APIs and services**.

2. You should see a table listing all enabled APIs. Scroll through and verify the following all appear with a green status or simply appear in the list:

   - [ ] Cloud Managed Service for Apache Kafka API
   - [ ] Compute Engine API
   - [ ] Dataflow API
   - [ ] Secret Manager API
   - [ ] Cloud Data Loss Prevention API
   - [ ] Cloud Resource Manager API

3. If any are missing, click **+ Enable APIs and Services** and enable the missing one.

> **Tip:** Use the search/filter box on the Enabled APIs page to quickly find each one by name rather than scrolling.

---

## What's Next

Project setup is complete. Move on to **[Step 02 — Custom VPC & Networking Setup](./02-vpc-and-networking.md)** to create the private network your Kafka cluster, VM, and Dataflow jobs will run inside.
