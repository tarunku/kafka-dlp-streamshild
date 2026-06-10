# Step 02 — Custom VPC & Networking Setup

In this step you create a private network (VPC) that all your POC resources — the Kafka cluster, the GCE VM, and Dataflow jobs — will live inside. Using a custom VPC instead of GCP's default network gives you full control over IP ranges, routing, and firewall rules, which is required for Managed Kafka and keeps the POC environment isolated and secure.

---

## Why Not Use the Default VPC?

GCP creates a "default" VPC automatically in every new project, but it:

- Has auto-generated subnets in every region (wasteful and hard to control)
- Has overly permissive default firewall rules
- Is not recommended for Managed Kafka, which works best with a dedicated, explicitly configured subnet

A **custom VPC** gives you a clean, minimal network with exactly the firewall rules you need — nothing more.

---

## Section 1: Create the Custom VPC Network

1. In the GCP Console left navigation, click **VPC network** (under the "Networking" section). If you don't see it, click the ☰ hamburger menu, scroll to **Networking**, and expand it.

2. Click **VPC networks** in the sub-menu.

3. Click **Create VPC network** at the top of the page.

4. Fill in the top-level VPC fields:
   - **Name:** `poc-vpc`
   - **Description:** `POC custom VPC for Kafka streaming pipeline`
   - **Subnet creation mode:** Select **Custom**

   > **Custom vs Automatic:** "Automatic" mode creates one subnet per region worldwide — you'd end up with 20+ subnets you don't need. "Custom" mode means you define only the subnets you actually want. Always use Custom for real projects.

5. **Dynamic routing mode:** Select **Regional**

   > Regional routing means routes are only shared within the same region (us-central1). This is sufficient for a single-region POC and is simpler to reason about.

6. Do NOT click **Create** yet — stay on this page to add the subnet in the next section.

---

## Section 2: Add the Subnet

Still on the same VPC creation form, scroll down to the **Subnets** section:

1. Click **Add subnet**.

2. A panel slides open on the right. Fill in the following:
   - **Name:** `poc-subnet`
   - **Region:** `us-central1`
   - **IP address range:** `10.0.0.0/24`

   > This CIDR block gives you 256 IP addresses (10.0.0.0 to 10.0.0.255), which is more than enough for a POC with a handful of VMs and Kafka brokers.

3. **Private Google Access:** Turn this **On**

   > **Why Private Google Access?** This allows VMs and Dataflow workers inside this subnet to reach Google APIs (like Secret Manager, Cloud Storage, Kafka) using internal Google routing — without needing a public IP or going out to the internet. Required for Dataflow to function correctly on private VMs.

4. **Flow logs:** Leave **Off**

   > Flow logs record all network traffic for analysis, but they generate a significant amount of Cloud Logging data and can add cost. Keep them off for POC to save money. You can enable them later for debugging if needed.

5. Click **Done** to close the subnet panel.

6. Now click **Create** at the bottom of the main form to create the VPC.

7. Wait 20–30 seconds. GCP will provision the VPC and subnet. You will be redirected to the VPC Networks list when complete.

---

## Section 3: Create Firewall Rules

Firewall rules control what traffic is allowed in and out of your VPC. By default, a custom VPC blocks all ingress traffic. You need to create three rules.

Navigate to: **VPC network** > **Firewall** in the left nav.

---

### Rule 1: Allow SSH from Your Laptop

This rule allows you to SSH into the GCE VM from your laptop only.

1. Click **Create Firewall Rule** at the top of the page.

2. Fill in the fields:
   - **Name:** `allow-ssh-from-laptop`
   - **Network:** `poc-vpc`
   - **Priority:** Leave as `1000` (default)
   - **Direction of traffic:** **Ingress** (traffic coming in to your VM)
   - **Action on match:** **Allow**
   - **Targets:** Select **Specified target tags**, then in the **Target tags** field type: `dev-vm`

   > The tag `dev-vm` is a label you will attach to your GCE VM in Step 04. Only VMs with this tag will be affected by this rule — not every VM in the VPC.

3. **Source filter:** Select **IPv4 ranges**

4. **Source IPv4 ranges:** Enter your laptop's current public IP address in CIDR notation, e.g., `203.0.113.45/32`

   > **How to find your public IP:** Open a browser tab and search "what is my ip" — Google will show it at the top of the results. Add `/32` at the end (the `/32` means "this exact single IP address"). Example: if your IP is `203.0.113.45`, enter `203.0.113.45/32`.

   > **Warning:** Your home/office IP may change if you have a dynamic IP from your ISP. If SSH stops working later, come back to this firewall rule and update the IP.

5. **Protocols and ports:** Select **Specified protocols and ports**, tick **TCP**, and enter port `22`.

6. Click **Create**.

---

### Rule 2: Allow Internal VPC Traffic

This rule allows all resources inside the VPC subnet to communicate freely with each other (e.g., VM talking to Kafka brokers).

1. Click **Create Firewall Rule** again.

2. Fill in the fields:
   - **Name:** `allow-internal`
   - **Network:** `poc-vpc`
   - **Direction of traffic:** **Ingress**
   - **Action on match:** **Allow**
   - **Targets:** **All instances in the network**
   - **Source filter:** **IPv4 ranges**
   - **Source IPv4 ranges:** `10.0.0.0/24`

   > This matches traffic that originates from within the subnet itself — so your VM, Kafka brokers, and Dataflow workers can all talk to each other.

3. **Protocols and ports:** Select **Allow all**

4. Click **Create**.

---

### Rule 3: Allow SSH via Cloud IAP (Browser Terminal)

This rule allows the GCP browser-based SSH (and `gcloud compute ssh`) to reach your VM. Both tools route through **Cloud Identity-Aware Proxy (IAP)**, which originates from GCP's fixed IP range `35.235.240.0/20`. Without this rule, clicking the "SSH" button in the Console will fail with error code 4003.

1. Click **Create Firewall Rule** again.

2. Fill in the fields:
   - **Name:** `allow-iap-ssh`
   - **Network:** `poc-vpc`
   - **Direction of traffic:** **Ingress**
   - **Action on match:** **Allow**
   - **Targets:** Select **Specified target tags**, then enter: `dev-vm`
   - **Source filter:** **IPv4 ranges**
   - **Source IPv4 ranges:** `35.235.240.0/20`

   > This is GCP's published IP range for Cloud IAP. It never changes, so this rule needs no maintenance.

3. **Protocols and ports:** Select **Specified protocols and ports**, tick **TCP**, and enter port `22`.

4. Click **Create**.

---

### Rule 4: Allow Egress HTTPS to the Internet

This rule allows all VMs in the VPC to make outbound HTTPS calls (port 443) — needed to reach Confluent Cloud Schema Registry, Snowflake, and other external endpoints.

1. Click **Create Firewall Rule** again.

2. Fill in the fields:
   - **Name:** `allow-egress-https`
   - **Network:** `poc-vpc`
   - **Direction of traffic:** **Egress** (traffic going out from your VM)
   - **Action on match:** **Allow**
   - **Targets:** **All instances in the network**
   - **Destination filter:** **IPv4 ranges**
   - **Destination IPv4 ranges:** `0.0.0.0/0`

   > `0.0.0.0/0` means "any destination on the internet". Combined with port 443 only, this is a safe and minimal egress rule.

3. **Protocols and ports:** Select **Specified protocols and ports**, tick **TCP**, and enter port `443`.

4. Click **Create**.

> **Note:** GCP custom VPCs have an implied "allow all egress" rule at low priority by default. Creating this explicit rule at priority 1000 is a best practice — it makes your intent explicit and gives you a hook to tighten later (e.g., restricting egress to specific IP ranges).

---

## Section 4: Verify the VPC Setup

Before moving on, do a quick sanity check:

1. Go to **VPC network** > **VPC networks** in the left nav.
   - [ ] Confirm `poc-vpc` appears in the list
   - [ ] Confirm the **Subnets** column shows `1 subnet`

2. Click on `poc-vpc` to open its details.
   - [ ] Click the **Subnets** tab — verify `poc-subnet` is listed with region `us-central1` and IP range `10.0.0.0/24`
   - [ ] Confirm the **Private Google Access** column shows `On` for `poc-subnet`

3. Go to **VPC network** > **Firewall** in the left nav.
   - [ ] Confirm all four rules appear: `allow-ssh-from-laptop`, `allow-iap-ssh`, `allow-internal`, `allow-egress-https`
   - [ ] Confirm each rule shows `poc-vpc` in the Network column

> **Tip:** If a rule shows a different network (e.g., `default`), delete it and recreate it — make sure you select `poc-vpc` in the Network field when creating the rule.

---

## What's Next

Networking is ready. Move on to **[Step 03 — IAM Service Accounts & Permissions](./03-iam-service-accounts.md)** to create the robot users that your VM and Dataflow pipeline will authenticate as.
