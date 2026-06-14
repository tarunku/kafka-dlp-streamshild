"""
StreamShield — Streamlit Producer Demo

Interactive web UI for producing tokenized prescription orders to Kafka.

Run:
    cd kafka-poc/streamshield
    source venv/bin/activate
    pip install -e ".[streamlit]"
    streamlit run examples/streamlit_producer.py
"""

import random
import time
import uuid

import pandas as pd
import streamlit as st

from streamshield import GCPConfig, KafkaProducer, SDKConfig
from streamshield.errors.exceptions import StreamShieldError

st.set_page_config(page_title="StreamShield Producer", layout="wide")
st.title("StreamShield — Prescription Order Producer")
st.caption("Produces tokenized prescription orders to Kafka via Cloud DLP")

# ── Sample data ───────────────────────────────────────────────────────────────
OWNER_NAMES  = ["Sarah Mitchell", "James Okafor", "Priya Sharma", "Carlos Reyes", "Emily Chen"]
OWNER_EMAILS = ["sarah@example.com", "james@example.com", "priya@example.com", "carlos@example.com", "emily@example.com"]
OWNER_PHONES = ["+1-555-0142", "+1-555-0287", "+1-555-0395", "+1-555-0411", "+1-555-0523"]
CARD_NUMBERS = ["4111111111111111", "5500005555555559", "340000000000009", "6011000000000004", "3530111333300000"]
PET_NAMES    = ["Biscuit", "Luna", "Max", "Cleo", "Buddy"]
MEDICATIONS  = ["Carprofen 25mg", "Metronidazole 250mg", "Amoxicillin 500mg", "Prednisone 5mg"]
QUANTITIES   = [15, 30, 60, 90]

# ── Session state ─────────────────────────────────────────────────────────────
if "sent_messages" not in st.session_state:
    st.session_state.sent_messages = []

def _random_prefill() -> dict:
    idx = random.randrange(len(OWNER_NAMES))
    return {
        "order_id":           f"RX-{uuid.uuid4().hex[:8].upper()}",
        "owner_name":         OWNER_NAMES[idx],
        "owner_email":        OWNER_EMAILS[idx],
        "owner_phone":        OWNER_PHONES[idx],
        "owner_payment_card": CARD_NUMBERS[idx],
        "pet_name":           random.choice(PET_NAMES),
        "medication":         random.choice(MEDICATIONS),
        "quantity":           random.choice(QUANTITIES),
        "order_date":         time.strftime("%Y-%m-%d"),
        "is_refill":          random.choice([True, False]),
    }

if "prefill" not in st.session_state:
    st.session_state.prefill = _random_prefill()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")
    project_id     = st.text_input("GCP Project ID", value="terraform-testing-498903")
    topic          = st.text_input("Kafka Topic", value="prescription-events")
    schema_version = st.number_input("Schema Version", value=1, min_value=1, step=1)
    st.divider()
    st.info(
        "Bootstrap servers and Schema Registry URL are loaded "
        "from GCP Secret Manager via Application Default Credentials."
    )

def _build_config() -> SDKConfig:
    return SDKConfig(
        gcp=GCPConfig(
            project_id=project_id,
            use_secret_manager=True,
            bootstrap_servers_secret="kafka-bootstrap-servers",
            schema_registry_url_secret="schema-registry-url",
        )
    )

def _send_orders(orders: list[dict]) -> list[dict]:
    """Produce messages via KafkaProducer; return history rows."""
    results = []
    with KafkaProducer(_build_config()) as producer:
        for order in orders:
            meta = producer.send(
                topic          = topic,
                key            = order["order_id"],
                value          = order,
                schema_version = int(schema_version),
            )
            results.append({
                "sent_at":            time.strftime("%H:%M:%S"),
                "order_id":           order["order_id"],
                "owner_name":         order["owner_name"],
                "owner_email":        order["owner_email"],
                "owner_phone":        order["owner_phone"],
                "owner_payment_card": order["owner_payment_card"],
                "pet_name":           order["pet_name"],
                "medication":         order["medication"],
                "quantity":           order["quantity"],
                "order_date":         order["order_date"],
                "is_refill":          order["is_refill"],
                "kafka_topic":        meta.topic,
            })
    return results

# ── Compose form ──────────────────────────────────────────────────────────────
st.subheader("Compose Order")

col_main, _ = st.columns([3, 1])
with col_main:
    if st.button("Randomize Fields"):
        st.session_state.prefill = _random_prefill()
        st.rerun()

    pre = st.session_state.prefill

    with st.form("order_form"):
        c1, c2 = st.columns(2)
        with c1:
            order_id           = st.text_input("Order ID", value=pre["order_id"])
            owner_name         = st.text_input("Owner Name  [PII — tokenized]", value=pre["owner_name"])
            owner_email        = st.text_input("Owner Email  [PII — tokenized]", value=pre["owner_email"])
            owner_phone        = st.text_input("Owner Phone  [PII — irreversible hash]", value=pre["owner_phone"])
            owner_payment_card = st.text_input("Payment Card  [PCI-DSS — tokenized]", value=pre["owner_payment_card"])
        with c2:
            pet_name   = st.text_input("Pet Name  [PII — tokenized]", value=pre["pet_name"])
            medication = st.selectbox(
                "Medication",
                MEDICATIONS,
                index=MEDICATIONS.index(pre["medication"]) if pre["medication"] in MEDICATIONS else 0,
            )
            quantity   = st.selectbox(
                "Quantity",
                QUANTITIES,
                index=QUANTITIES.index(pre["quantity"]) if pre["quantity"] in QUANTITIES else 1,
            )
            order_date = st.text_input("Order Date", value=pre["order_date"])
            is_refill  = st.checkbox("Is Refill", value=pre["is_refill"])

        c_one, c_batch = st.columns(2)
        with c_one:
            send_one   = st.form_submit_button("Send 1 Message", type="primary", use_container_width=True)
        with c_batch:
            send_batch = st.form_submit_button("Send Random Batch (5)", use_container_width=True)

# ── Handle submissions ────────────────────────────────────────────────────────
if send_one:
    order = {
        "order_id":           order_id,
        "owner_name":         owner_name,
        "owner_email":        owner_email,
        "owner_phone":        owner_phone,
        "owner_payment_card": owner_payment_card,
        "pet_name":           pet_name,
        "medication":         medication,
        "quantity":           quantity,
        "order_date":         order_date,
        "is_refill":          is_refill,
    }
    with st.spinner(f"Tokenizing and producing to '{topic}'..."):
        try:
            rows = _send_orders([order])
            st.session_state.sent_messages.extend(rows)
            st.success(f"Delivered: {order_id}")
        except StreamShieldError as e:
            st.error(f"{type(e).__name__}: {e}")

if send_batch:
    orders = [_random_prefill() for _ in range(5)]
    with st.spinner(f"Tokenizing and producing 5 messages to '{topic}'..."):
        try:
            rows = _send_orders(orders)
            st.session_state.sent_messages.extend(rows)
            st.success(f"Delivered {len(rows)} messages.")
        except StreamShieldError as e:
            st.error(f"{type(e).__name__}: {e}")

# ── History ───────────────────────────────────────────────────────────────────
st.divider()
st.subheader(f"Sent Messages ({len(st.session_state.sent_messages)})")

if st.session_state.sent_messages:
    if st.button("Clear History"):
        st.session_state.sent_messages = []
        st.rerun()

    st.dataframe(pd.DataFrame(st.session_state.sent_messages), use_container_width=True, hide_index=True)
    st.caption(
        "Values shown here are the plaintext values as entered. "
        "The actual Kafka payload stores DLP tokens — "
        "all fields labelled [PII] and [PCI-DSS] are opaque at rest in Kafka."
    )
else:
    st.info("No messages sent yet. Fill in the form above and click Send.")
