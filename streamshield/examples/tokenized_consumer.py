"""
StreamShield Example: Tokenized consumer (no DLP access required).

Reads prescription orders from Kafka and prints the tokens as-is.
This simulates any downstream subscriber that does NOT have Cloud KMS
cryptoKeyDecrypter — they see opaque tokens where sensitive values should be.

The offset is committed ONLY after the handler returns successfully.

Run:
    python3 examples/tokenized_consumer.py
"""

import logging

from streamshield import ConsumedMessage, GCPConfig, KafkaConsumer, SDKConfig
from streamshield.dlp.policy import get_tokenized_fields
from streamshield.observability.logging import configure_json_logging

configure_json_logging(level=logging.INFO)

config = SDKConfig(
    gcp=GCPConfig(
        project_id="vetsource-496203",
        use_secret_manager=True,
    )
)

TOPIC    = "prescription-events"
GROUP_ID = "streamshield-tokenized-consumer"


def print_tokenized_record(msg: ConsumedMessage) -> None:
    """
    Application handler — print each field, labelling tokenized ones.
    This handler has no DLP access and sees tokens as opaque strings.
    """
    tokenized_fields = {f["name"]: f for f in get_tokenized_fields(msg.raw_schema)}
    default_reversible = msg.raw_schema.get("token.default-reversible", "true")

    print(f"\nMessage  partition={msg.partition} offset={msg.offset}")
    print(f"{'─' * 55}")

    for field_name, value in msg.value.items():
        meta = tokenized_fields.get(field_name)

        if meta is None:
            # Plain (non-sensitive) field
            print(f"  {'':2}  {field_name:<24}: {value}")
        else:
            reversible = meta.get("token.reversible", default_reversible) != "false"
            label = "[reversible token]" if reversible else "[irreversible hash]"
            icon  = "🔒" if reversible else "🔐"
            print(f"  {icon}  {field_name:<24}: {str(value)[:45]}  {label}")


print(f"\nSubscribed to '{TOPIC}' as group '{GROUP_ID}'.")
print("Printing TOKENIZED data (no DLP access — tokens visible as-is).")
print("Press Ctrl+C to stop.\n")

# process() handles the poll loop, DLQ routing, and offset commits automatically.
# detokenize=False means DLP is NOT called — the consumer sees tokens as-is.
with KafkaConsumer(config, group_id=GROUP_ID) as consumer:
    consumer.process(
        handler      = print_tokenized_record,
        topics       = [TOPIC],
        detokenize   = False,          # no DLP — tokens stay as tokens
        idle_timeout_s = 30.0,
    )

print("\nConsumer stopped.")
