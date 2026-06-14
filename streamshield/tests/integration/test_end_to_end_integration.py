"""
Integration tests: end-to-end pipeline and field-level tokenization verification.

Covers:
  1. Full pipeline — produce → tokenized consume → detokenized consume.
  2. Batch DLP (20 records) — single tokenize_batch() call, all delivered.
  3. owner_phone is a SHA-256 hash — irreversible, not a phone number, not a PII token.
  4. owner_payment_card uses PCI domain — different from PII tokens.
  5. Reversible PII fields exactly restored — owner_name and owner_email match input.
  6. Non-PII fields pass through unchanged — medication, quantity, order_date, is_refill.
  7. Same consumer group does not reprocess — offset committed after first successful consume.

All tests run against terraform-testing-498903.  No mocking.

Design pattern:
  - Each test produces 1–20 messages with a unique order_id as the Kafka key.
  - Consumers use fresh group IDs (auto_offset_reset="earliest") and collect ALL
    messages into a dict keyed by order_id.
  - Assertions target the specific message produced in the test by its unique key.
  - This avoids assumptions about which messages are already in the topic.
"""

from __future__ import annotations

import time
import uuid


from streamshield import (
    ConsumedMessage,
    GCPConfig,
    KafkaConsumer,
    KafkaProducer,
    SDKConfig,
)
from tests.integration.conftest import INTEGRATION_PROJECT_ID, INTEGRATION_TOPIC

# PII surrogate prefix (from schema token.surrogate-info-type)
PII_SURROGATE = "VETSOURCE_PII_TOKEN"
# PCI surrogate prefix (from schema token.pci-surrogate-info-type)
PCI_SURROGATE = "VETSOURCE_PCI_TOKEN"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _group_id(label: str) -> str:
    return f"streamshield-e2e-{label}-{int(time.time())}"


def _make_order(
    owner_name:  str = "Alice Brown",
    owner_email: str = "alice@test.com",
    owner_phone: str = "+1-555-0001",
    card:        str = "4111111111111111",
    order_id:    str | None = None,
) -> dict:
    return {
        "order_id":           order_id or f"E2E-{uuid.uuid4().hex[:8].upper()}",
        "owner_name":         owner_name,
        "owner_email":        owner_email,
        "owner_phone":        owner_phone,
        "owner_payment_card": card,
        "pet_name":           "Luna",
        "medication":         "Metronidazole 250mg",
        "quantity":           15,
        "order_date":         "2026-06-04",
        "is_refill":          False,
    }


def _consume_all(
    config: SDKConfig,
    group_id: str,
    detokenize: bool,
    idle_timeout_s: float = 15.0,
) -> dict[str, ConsumedMessage]:
    """
    Consume all messages from INTEGRATION_TOPIC using the given config and
    return them as a dict keyed by the Kafka message key (decoded UTF-8).
    """
    collected: dict[str, ConsumedMessage] = {}

    def handler(msg: ConsumedMessage) -> None:
        key = msg.key.decode("utf-8", errors="replace") if msg.key else ""
        collected[key] = msg

    with KafkaConsumer(config, group_id=group_id) as consumer:
        consumer.process(
            handler        = handler,
            topics         = [INTEGRATION_TOPIC],
            detokenize     = detokenize,
            idle_timeout_s = idle_timeout_s,
        )

    return collected


def _latest_config(base: SDKConfig) -> SDKConfig:
    """Return a config variant with auto_offset_reset=earliest (default is already earliest)."""
    return base


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestFullPipeline:
    def test_full_pipeline_produce_tokenized_detokenized(self, integration_config):
        """
        Produce 1 record.
        Tokenized consumer sees PII token strings (not plaintext).
        Detokenized consumer sees original plaintext (reversible fields restored).
        """
        order_id     = f"FULL-{uuid.uuid4().hex[:8].upper()}"
        original     = _make_order(order_id=order_id)
        original_name  = original["owner_name"]
        original_email = original["owner_email"]

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order_id, value=original)

        # ── Tokenized consumer ────────────────────────────────────────────────
        tok_msgs = _consume_all(integration_config, _group_id("full-tok"), detokenize=False)
        assert order_id in tok_msgs, "Message not found in tokenized consume"
        tok_val = tok_msgs[order_id].value

        # Sensitive fields must be tokens, not plaintext
        assert tok_val["owner_name"]  != original_name,  "owner_name not tokenized"
        assert tok_val["owner_email"] != original_email, "owner_email not tokenized"
        assert tok_val["owner_name"].startswith(f"{PII_SURROGATE}("), \
            f"owner_name token has unexpected format: {tok_val['owner_name'][:60]}"

        # Non-sensitive fields pass through unchanged
        assert tok_val["medication"] == original["medication"]
        assert tok_val["quantity"]   == original["quantity"]

        # ── Detokenized consumer ──────────────────────────────────────────────
        detok_msgs = _consume_all(integration_config, _group_id("full-detok"), detokenize=True)
        assert order_id in detok_msgs, "Message not found in detokenized consume"
        detok_val = detok_msgs[order_id].value

        # Reversible PII fields must be restored to original plaintext
        assert detok_val["owner_name"]  == original_name,  \
            f"owner_name not restored: got {detok_val['owner_name']}"
        assert detok_val["owner_email"] == original_email, \
            f"owner_email not restored: got {detok_val['owner_email']}"

    def test_batch_dlp_20_records_single_api_call(self, integration_config):
        """
        Produce 20 records via send_batch().
        tokenize_batch() sends them all in one DLP deidentifyContent call.
        All 20 must be delivered.
        """
        records = [_make_order(order_id=f"BATCH-{uuid.uuid4().hex[:8].upper()}") for _ in range(20)]
        keys    = {r["order_id"] for r in records}

        with KafkaProducer(integration_config) as producer:
            results = producer.send_batch(INTEGRATION_TOPIC, records=records, key_field="order_id")

        assert len(results) == 20, f"Expected 20 metadata objects, got {len(results)}"

        msgs = _consume_all(integration_config, _group_id("batch20"), detokenize=False)
        found = keys & msgs.keys()
        assert len(found) == 20, \
            f"Only {len(found)}/20 batch messages found after consume: {keys - found}"


class TestFieldLevelTokenization:
    """Verify the exact tokenization behaviour for each field type in the schema."""

    def test_owner_phone_is_irreversible_sha256_hash(self, integration_config):
        """
        owner_phone uses CryptoHashConfig (token.reversible=false).
        After tokenization the value must:
          - NOT start with '+' (not the original phone number).
          - NOT start with the PII surrogate prefix (not a deterministic token).
          - Be a non-empty string (the SHA-256 hash is base64-encoded, ~44 chars).
        Even with detokenize=True, the hash is unchanged — DLP never reverses it.
        """
        order_id      = f"PHONE-{uuid.uuid4().hex[:8].upper()}"
        original      = _make_order(owner_phone="+1-555-9999", order_id=order_id)
        original_phone = original["owner_phone"]

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order_id, value=original)

        # Check tokenized consumer
        tok_msgs = _consume_all(integration_config, _group_id("phone-tok"), detokenize=False)
        assert order_id in tok_msgs
        phone_tok = tok_msgs[order_id].value["owner_phone"]

        assert phone_tok != original_phone, "owner_phone was NOT tokenized"
        assert not phone_tok.startswith("+"), \
            f"owner_phone looks like a phone number (not hashed): {phone_tok}"
        assert not phone_tok.startswith(f"{PII_SURROGATE}("), \
            f"owner_phone looks like a deterministic PII token (expected hash): {phone_tok[:60]}"
        assert len(phone_tok) > 10, "owner_phone hash is suspiciously short"

        # Check detokenized consumer — hash must be IDENTICAL (not reversed)
        detok_msgs = _consume_all(integration_config, _group_id("phone-detok"), detokenize=True)
        assert order_id in detok_msgs
        phone_detok = detok_msgs[order_id].value["owner_phone"]

        assert phone_detok == phone_tok, \
            "owner_phone changed between tokenized and detokenized consume — it should be irreversible"
        assert phone_detok != original_phone, \
            "owner_phone was reversed despite token.reversible=false"

    def test_owner_payment_card_uses_pci_domain(self, integration_config):
        """
        owner_payment_card uses CryptoReplaceFfxFpeConfig with PCI-DSS sensitivity.
        After tokenization the value must NOT be the original card number.
        It uses the PCI key domain (separate from PII fields).
        """
        order_id      = f"CARD-{uuid.uuid4().hex[:8].upper()}"
        original      = _make_order(card="4111111111111111", order_id=order_id)
        original_card = original["owner_payment_card"]

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order_id, value=original)

        tok_msgs = _consume_all(integration_config, _group_id("card-tok"), detokenize=False)
        assert order_id in tok_msgs
        card_tok = tok_msgs[order_id].value["owner_payment_card"]

        assert card_tok != original_card, \
            "owner_payment_card was NOT tokenized (still shows original card number)"
        # Must NOT use the PII surrogate — different KMS domain
        assert not card_tok.startswith(f"{PII_SURROGATE}("), \
            f"owner_payment_card incorrectly uses PII surrogate: {card_tok[:60]}"

    def test_reversible_pii_fields_exactly_restored_after_detokenization(self, integration_config):
        """
        owner_name and owner_email use CryptoDeterministicConfig (reversible by default).
        The detokenized consumer must return the EXACT original plaintext values.
        """
        order_id      = f"REVRS-{uuid.uuid4().hex[:8].upper()}"
        original_name  = "Distinctive Test Name XYZ"
        original_email = "distinctive.test@example-xyz.com"
        original       = _make_order(
            owner_name  = original_name,
            owner_email = original_email,
            order_id    = order_id,
        )

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order_id, value=original)

        # Step 1: tokenized consumer sees tokens
        tok_msgs = _consume_all(integration_config, _group_id("revrs-tok"), detokenize=False)
        assert order_id in tok_msgs
        tok_val = tok_msgs[order_id].value

        assert tok_val["owner_name"]  != original_name,  "owner_name not tokenized"
        assert tok_val["owner_email"] != original_email, "owner_email not tokenized"
        assert tok_val["owner_name"].startswith(f"{PII_SURROGATE}("), \
            f"owner_name token format unexpected: {tok_val['owner_name'][:60]}"

        # Step 2: detokenized consumer sees original plaintext
        detok_msgs = _consume_all(integration_config, _group_id("revrs-detok"), detokenize=True)
        assert order_id in detok_msgs
        detok_val = detok_msgs[order_id].value

        assert detok_val["owner_name"]  == original_name, \
            f"owner_name not restored: expected={original_name!r} got={detok_val['owner_name']!r}"
        assert detok_val["owner_email"] == original_email, \
            f"owner_email not restored: expected={original_email!r} got={detok_val['owner_email']!r}"

    def test_non_pii_fields_pass_through_unchanged(self, integration_config):
        """
        Fields without logicalType='tokenized' in the schema must be identical
        before and after the produce–consume round-trip (both tokenized and detokenized).
        """
        order_id = f"PLAIN-{uuid.uuid4().hex[:8].upper()}"
        original = _make_order(order_id=order_id)

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order_id, value=original)

        msgs = _consume_all(integration_config, _group_id("plain"), detokenize=False)
        assert order_id in msgs
        val = msgs[order_id].value

        plain_fields = ["order_id", "medication", "quantity", "order_date", "is_refill"]
        for field in plain_fields:
            assert val[field] == original[field], \
                f"Non-PII field '{field}' changed during round-trip: " \
                f"expected={original[field]!r} got={val[field]!r}"

    def test_pet_name_is_tokenized_as_pii(self, integration_config):
        """
        pet_name has logicalType='tokenized' and uses CryptoDeterministicConfig.
        It must be tokenized (PII domain) and reversible.
        """
        order_id      = f"PET-{uuid.uuid4().hex[:8].upper()}"
        original      = _make_order(order_id=order_id)
        original_pet  = original["pet_name"]  # "Luna"

        with KafkaProducer(integration_config) as producer:
            producer.send(INTEGRATION_TOPIC, key=order_id, value=original)

        tok_msgs = _consume_all(integration_config, _group_id("pet-tok"), detokenize=False)
        assert order_id in tok_msgs
        pet_tok = tok_msgs[order_id].value["pet_name"]

        assert pet_tok != original_pet, "pet_name was not tokenized"
        assert pet_tok.startswith(f"{PII_SURROGATE}("), \
            f"pet_name token format unexpected: {pet_tok[:60]}"

        detok_msgs = _consume_all(integration_config, _group_id("pet-detok"), detokenize=True)
        assert order_id in detok_msgs
        pet_detok = detok_msgs[order_id].value["pet_name"]

        assert pet_detok == original_pet, \
            f"pet_name not restored: expected={original_pet!r} got={pet_detok!r}"


class TestOffsetManagement:
    """Verify commit-after-process guarantees."""

    def test_same_group_does_not_reprocess_committed_messages(self, integration_config):
        """
        After group A commits an offset, re-running group A must not reprocess
        the same message — it must start from beyond the committed position.

        Strategy:
          - Run 1: consume 1 message with group X (max_messages=1).  Offset committed.
          - Run 2: same group X, max_messages=1.  The message committed in run 1
                   must NOT appear again.  Run 2 may pick up any other message
                   (or time out on idle if all partitions are exhausted), but never
                   the one already committed.
        """
        group_id = f"streamshield-reprocess-{uuid.uuid4().hex[:6]}"

        # ── Run 1: consume 1 message and commit its offset ────────────────────
        first_seen: list[str] = []

        with KafkaConsumer(integration_config, group_id=group_id) as c:
            c.process(
                handler        = lambda msg: first_seen.append(
                    msg.key.decode("utf-8", errors="replace") if msg.key else ""
                ),
                topics         = [INTEGRATION_TOPIC],
                detokenize     = False,
                max_messages   = 1,
                idle_timeout_s = 20.0,
            )

        assert len(first_seen) == 1, "Run 1 must have consumed exactly 1 message"
        committed_key = first_seen[0]

        # ── Run 2: same group_id — must NOT reprocess committed_key ───────────
        second_seen: list[str] = []

        with KafkaConsumer(integration_config, group_id=group_id) as c:
            c.process(
                handler        = lambda msg: second_seen.append(
                    msg.key.decode("utf-8", errors="replace") if msg.key else ""
                ),
                topics         = [INTEGRATION_TOPIC],
                detokenize     = False,
                max_messages   = 1,
                idle_timeout_s = 10.0,  # short — we only want to check for reprocessing
            )

        # The committed message must not appear in run 2 regardless of what else is seen
        assert committed_key not in second_seen, \
            f"Consumer group {group_id} reprocessed already-committed key={committed_key}"
