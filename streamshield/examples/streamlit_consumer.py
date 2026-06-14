"""
StreamShield — Streamlit Consumer Demo

Interactive web UI for polling prescription orders from Kafka.
Two modes:
  - Tokenized: no DLP access required; sensitive fields shown as opaque tokens.
  - Detokenized: calls Cloud DLP reidentifyContent; requires KMS cryptoKeyDecrypter.

Run:
    cd kafka-poc/streamshield
    source venv/bin/activate
    pip install -e ".[streamlit]"
    streamlit run examples/streamlit_consumer.py
"""

import time
import uuid

import pandas as pd
import streamlit as st

from streamshield import ConsumerConfig, GCPConfig, KafkaConsumer, SDKConfig
from streamshield.dlp.policy import get_tokenized_fields
from streamshield.errors.exceptions import StreamShieldError

st.set_page_config(page_title="StreamShield Consumer", layout="wide")
st.title("StreamShield — Prescription Order Consumer")
st.caption("Poll and display prescription orders from Kafka")

# ── Session state ─────────────────────────────────────────────────────────────
if "tok_messages" not in st.session_state:
    st.session_state.tok_messages = []
if "detok_messages" not in st.session_state:
    st.session_state.detok_messages = []
# Stable per-session suffix so each browser session tracks its own offsets
if "session_id" not in st.session_state:
    st.session_state.session_id = uuid.uuid4().hex[:8]

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Configuration")
    project_id        = st.text_input("GCP Project ID", value="terraform-testing-498903")
    topic             = st.text_input("Kafka Topic", value="prescription-events")
    auto_offset_reset = st.selectbox("Start From", ["earliest", "latest"], index=0)
    st.divider()
    max_messages = st.number_input("Max Messages per Fetch", value=10, min_value=1, max_value=100, step=5)
    idle_timeout = st.number_input("Idle Timeout (s)", value=10.0, min_value=1.0, max_value=60.0, step=1.0)
    st.divider()
    st.caption(f"Session: `{st.session_state.session_id}`")
    st.info(
        "Secrets loaded from Secret Manager via "
        "Application Default Credentials. "
        "Consumer group IDs are scoped to this browser session."
    )

def _build_config() -> SDKConfig:
    return SDKConfig(
        gcp=GCPConfig(
            project_id=project_id,
            use_secret_manager=True,
        ),
        consumer=ConsumerConfig(
            auto_offset_reset=auto_offset_reset,
        ),
    )

def _fetch_messages(detokenize: bool, group_id: str) -> list[dict]:
    """
    Poll up to max_messages from Kafka, annotate tokenized fields, return display rows.
    Resets the idle deadline on each received message so it genuinely idles for
    idle_timeout seconds after the last message arrives.
    """
    config = _build_config()
    rows: list[dict] = []

    with KafkaConsumer(config, group_id=group_id) as consumer:
        consumer.subscribe([topic])
        deadline = time.monotonic() + float(idle_timeout)

        while len(rows) < int(max_messages) and time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0, detokenize=detokenize)
            if msg is None:
                continue  # no message yet — keep waiting until deadline

            consumer.commit(msg)
            deadline = time.monotonic() + float(idle_timeout)  # reset on each message

            tokenized_fields   = {f["name"]: f for f in get_tokenized_fields(msg.raw_schema)} if msg.raw_schema else {}
            default_reversible = (msg.raw_schema or {}).get("token.default-reversible", "true")

            row: dict = {
                "fetched_at": time.strftime("%H:%M:%S"),
                "partition":  msg.partition,
                "offset":     msg.offset,
            }

            for field, value in msg.value.items():
                meta = tokenized_fields.get(field)
                if meta is None:
                    row[field] = value
                else:
                    reversible = meta.get("token.reversible", default_reversible) != "false"
                    if detokenize:
                        tag = " [detokenized]" if reversible else " [hash — irreversible]"
                    else:
                        tag = " [token]" if reversible else " [hash]"
                    row[field] = f"{str(value)[:50]}{tag}"

            rows.append(row)

    return rows

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_tok, tab_detok = st.tabs(["Tokenized Consumer", "Detokenized Consumer"])

# ── Tokenized tab ─────────────────────────────────────────────────────────────
with tab_tok:
    st.markdown(
        "Reads messages **without** calling Cloud DLP. "
        "Sensitive fields are shown as opaque tokens or one-way hashes. "
        "No KMS access is required."
    )

    tok_group = f"streamlit-tok-{st.session_state.session_id}"
    st.caption(f"Consumer group: `{tok_group}`")

    c1, c2 = st.columns([1, 5])
    with c1:
        fetch_tok = st.button("Fetch Messages", key="btn_tok", type="primary", use_container_width=True)
    with c2:
        clear_tok = st.button("Clear", key="clr_tok", use_container_width=True)

    if clear_tok:
        st.session_state.tok_messages = []
        st.rerun()

    if fetch_tok:
        with st.spinner(f"Polling up to {max_messages} messages (idle timeout: {idle_timeout}s)..."):
            try:
                rows = _fetch_messages(detokenize=False, group_id=tok_group)
                st.session_state.tok_messages.extend(rows)
                if rows:
                    st.success(f"Fetched {len(rows)} message(s).")
                else:
                    st.warning("No messages received within the idle timeout.")
            except StreamShieldError as e:
                st.error(f"{type(e).__name__}: {e}")

    if st.session_state.tok_messages:
        st.caption(f"{len(st.session_state.tok_messages)} messages in session")
        st.dataframe(pd.DataFrame(st.session_state.tok_messages), use_container_width=True, hide_index=True)
    else:
        st.info("No messages fetched yet. Click Fetch Messages to poll Kafka.")

# ── Detokenized tab ───────────────────────────────────────────────────────────
with tab_detok:
    st.warning(
        "Calls Cloud DLP `reidentifyContent` to restore original plaintext. "
        "Requires `roles/dlp.user` on the calling service account — "
        "DLP's own service agent handles the KMS key unwrapping internally. "
        "Fields tagged [hash — irreversible] cannot be recovered; they were hashed one-way by design."
    )

    detok_group = f"streamlit-detok-{st.session_state.session_id}"
    st.caption(f"Consumer group: `{detok_group}`")

    c1, c2 = st.columns([1, 5])
    with c1:
        fetch_detok = st.button("Fetch Messages", key="btn_detok", type="primary", use_container_width=True)
    with c2:
        clear_detok = st.button("Clear", key="clr_detok", use_container_width=True)

    if clear_detok:
        st.session_state.detok_messages = []
        st.rerun()

    if fetch_detok:
        with st.spinner(f"Polling and de-tokenizing up to {max_messages} messages (idle timeout: {idle_timeout}s)..."):
            try:
                rows = _fetch_messages(detokenize=True, group_id=detok_group)
                st.session_state.detok_messages.extend(rows)
                if rows:
                    st.success(f"Fetched and de-tokenized {len(rows)} message(s).")
                else:
                    st.warning("No messages received within the idle timeout.")
            except StreamShieldError as e:
                st.error(f"{type(e).__name__}: {e}")

    if st.session_state.detok_messages:
        st.caption(f"{len(st.session_state.detok_messages)} messages in session")
        st.dataframe(pd.DataFrame(st.session_state.detok_messages), use_container_width=True, hide_index=True)
    else:
        st.info("No messages fetched yet. Click Fetch Messages to poll Kafka.")
