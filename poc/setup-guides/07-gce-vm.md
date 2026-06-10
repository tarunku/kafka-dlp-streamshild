# Step 07 — GCE VM Setup: Development Environment

This VM lives inside the VPC (`poc-vpc`) and is how we run producer/consumer code that talks to Kafka over the **private network**. Because the VM is in the same VPC as the Managed Kafka cluster, all traffic stays internal — no public internet exposure for Kafka. You will develop code on your laptop using **VSCode Remote SSH**, which tunnels your editor directly into the VM.

---

## 1. Create the VM

1. In the GCP Console, open the **Navigation Menu** (three-line hamburger icon, top-left) and go to **Compute Engine** > **VM Instances**.

2. Click **"Create Instance"** (blue button, top of the page).

3. Fill in the **Name** field:
   - Name: `poc-dev-vm-2`

4. Under **Region and Zone**:
   - Region: `us-central1`
   - Zone: `us-central1-a`

5. Under **Machine configuration**:
   - Series: **E2**
   - Machine type: `e2-medium` (2 vCPU, 4 GB RAM — sufficient for this POC)

6. Under **Boot disk**, click **"Change"**:
   - Operating system: **Debian GNU/Linux**
   - Version: **Debian GNU/Linux 12 (bookworm)**
   - Boot disk size: `20` GB
   - Click **"Select"** to confirm

7. Under **Identity and API access**:
   - Service account: click the dropdown and select `vm-producer-sa`
   - Access scopes: select **"Allow full access to all Cloud APIs"**

   > **Why this matters:** The `vm-producer-sa` service account controls what GCP resources this VM can access (Kafka, Secret Manager). Selecting it here means all code running on the VM automatically uses this identity — no manual credential files needed.

8. Scroll down to find the **"Advanced options"** section. Click to expand it, then expand **"Networking"**.

   Inside the Networking section:
   - Under **Network interfaces**, click the default interface to expand it
   - **Network**: select `poc-vpc`
   - **Subnetwork**: select `poc-subnet` (should auto-populate as `poc-subnet (10.0.0.0/24)`)
   - **External IPv4 address**: leave as `None`

   > **Note:** The Vetsource GCP org policy (`constraints/compute.vmExternalIpAccess`) blocks external IPs on VMs. Leave this as `None` — you will connect via **IAP TCP tunneling** instead (see Section 4).

   - Under **Network tags**, type `dev-vm` and press **Enter**

   > **Why the tag?** The firewall rule you'll create (or may have already created) targets this tag. Only VMs with the `dev-vm` tag will be allowed SSH access.

9. Click **"Create"** at the bottom of the page.

10. The VM will provision in approximately **1–2 minutes**. When ready, a **green checkmark** icon appears next to `poc-dev-vm` in the VM Instances list.

---

## 2. Verify the VM is Running

1. In the **VM Instances** list, confirm `poc-dev-vm-2` shows a **green status circle**.
2. The **External IP** column will show `None` — this is expected due to org policy. You will connect via IAP tunneling instead of a public IP.

---

## 3. First SSH — Test via Browser (GCP Console)

Before configuring VSCode, quickly verify the VM is reachable using the built-in browser terminal.

1. In the VM Instances list, find `poc-dev-vm-2` and click the **"SSH"** button in the `Connect` column.
2. A browser-based terminal window opens (may take 10–15 seconds). This confirms the VM is online and your account has access.
3. Run the following command to confirm your login:
   ```bash
   whoami
   ```
   You should see your GCP username printed (e.g., `tarun_kumar1`).
4. Close the browser terminal tab. From this point on, you will use VSCode for all development.

---

## 4. Connect via IAP TCP Tunneling (from your Laptop)

The Vetsource org policy blocks external IPs on VMs. **IAP (Identity-Aware Proxy) TCP tunneling** is the standard replacement — your laptop connects to Google's IAP endpoint over HTTPS, which forwards the connection to the VM's internal IP. No public IP is needed on the VM.

### 4a. One-time: Create a firewall rule for IAP

IAP always uses source IP range `35.235.240.0/20`. Create a rule to allow it to reach the VM on port 22.

In GCP Console → **VPC Network** → **Firewall** → **Create Firewall Rule**:

| Field | Value |
|---|---|
| Name | `allow-iap-ssh` |
| Network | `poc-vpc` |
| Direction | Ingress |
| Action | Allow |
| Targets | Specified target tags |
| Target tags | `dev-vm` |
| Source filter | IPv4 ranges |
| Source IP ranges | `35.235.240.0/20` |
| Protocols / ports | TCP `22` |

Click **Create**.

### 4b. One-time: Grant IAP Tunnel User role

In **IAM & Admin** → **IAM**, find your account and add the role:

```
IAP-secured Tunnel User  (roles/iap.tunnelResourceAccessor)
```

### 4c. One-time: Configure gcloud on your laptop

Ensure `gcloud` is authenticated and pointing at the correct project:

```bash
gcloud auth login
gcloud config set project vetsource-496203
gcloud config set compute/region us-central1
gcloud config set compute/zone us-central1-a
```

### 4d. Test SSH via IAP (gcloud)

```bash
gcloud compute ssh poc-dev-vm-2 \
  --project=vetsource-496203 \
  --zone=us-central1-a \
  --tunnel-through-iap
```

Run `whoami` inside the shell to confirm your username (e.g., `tarun_kumar1`), then `exit`.

### 4e. Connect in VSCode via IAP

1. **Install the Remote - SSH extension** in VSCode if not already installed (search `Remote - SSH` in the Extensions panel).

2. Add the following block to `~/.ssh/config` on your laptop (create the file if it doesn't exist):

   ```
   Host poc-dev-vm-2
       HostName poc-dev-vm-2
       User YOUR_GCP_USERNAME
       ProxyCommand gcloud compute start-iap-tunnel poc-dev-vm-2 22 \
           --project=vetsource-496203 \
           --zone=us-central1-a \
           --local-host-port=localhost:%PORT%
       IdentityFile ~/.ssh/google_compute_engine
   ```

   Replace `YOUR_GCP_USERNAME` with the username from the `whoami` output in step 4d (e.g., `tarun_kumar1`).

3. In VSCode, open the **Command Palette** (`Cmd+Shift+P`) → `Remote-SSH: Connect to Host` → select `poc-dev-vm-2`.

4. The first connection takes 30–60 seconds as VSCode installs its server on the VM.

5. Once connected, you'll see **`SSH: poc-dev-vm-2`** in the bottom-left blue status bar.

6. Open a terminal: **Terminal** > **New Terminal** (or `` Ctrl+` ``). Every command runs on the VM, not your laptop.

---

## 5. Install Python and Dependencies on the VM

All commands below are run in the **VSCode terminal** (which is connected to the VM). You do not need to switch to the browser terminal.

```bash
# Update package lists and install Python
sudo apt-get update && sudo apt-get install -y python3 python3-pip python3-venv

# Create project directory
mkdir ~/kafka-poc && cd ~/kafka-poc

sudo apt install python3.11-venv -y
# Create a virtual environment (isolates Python packages)
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate

# Install required Python libraries
pip install confluent-kafka fastavro requests google-cloud-secret-manager
```

> **What is a virtual environment?** It creates an isolated Python environment just for this project, so installed packages don't conflict with anything else on the system. You need to run `source venv/bin/activate` each time you open a new terminal session.

Verify the installation succeeded:

```bash
python3 -c "import confluent_kafka; print('confluent-kafka OK')"
python3 -c "import fastavro; print('fastavro OK')"
python3 -c "import google.cloud.secretmanager; print('secret-manager OK')"
```

Each line should print the corresponding `OK` message. If you see an `ImportError`, re-run the `pip install` command above.

---

## 6. Test Secret Manager Access from the VM

Run this quick test to confirm the VM can read secrets using its attached service account (`vm-producer-sa`). No credentials file is needed — GCP's metadata server handles authentication automatically.

```bash
python3 -c "
from google.cloud import secretmanager
client = secretmanager.SecretManagerServiceClient()
name = 'projects/vetsource-496203/secrets/kafka-bootstrap-servers/versions/latest'
response = client.access_secret_version(request={'name': name})
print('Bootstrap server:', response.payload.data.decode('UTF-8'))
"
```

**Expected output:** The Kafka bootstrap server address prints to the console, for example:
```
Bootstrap server: bootstrap.poc-kafka-cluster.us-central1.managedkafka.kafka-poc.cloud.google.com:9092
```

> **If you see a permission denied error:** The `vm-producer-sa` service account may be missing the `Secret Manager Secret Accessor` IAM role. Go to **IAM & Admin** > **IAM**, find the service account, and verify the role is assigned.

> **Note:** Actual Kafka connectivity (producing and consuming messages) is tested in **Step 09** when we run the producer and consumer scripts.

---

## 7. What's Next

The VM is running, connected via VSCode, and can reach Secret Manager. Next:

- **Step 08** — Create the `raw-events` Kafka topic in the Managed Kafka cluster
- **Step 09** — Write and run the producer and consumer Python scripts
