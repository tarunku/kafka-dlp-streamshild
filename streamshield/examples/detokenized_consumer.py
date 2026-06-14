"""
StreamShield Example: Detokenized consumer (requires Cloud KMS access).

Reads prescription orders and calls DLP reidentifyContent to restore original
plaintext values. Requires roles/cloudkms.cryptoKeyDecrypter on both KMS domain
keys (pii-dek-kek and pci-dek-kek).

Irreversible fields (owner_phone, tokenized with CryptoHashConfig) are left
as hashes — even with KMS access, the original value cannot be recovered.

Run (from a VM with the authorized service account):
    python3 examples/detokenized_consumer.py
"""

import json
import logging

from streamshield import ConsumedMessage, GCPConfig, KafkaConsumer, SDKConfig
from streamshield.dlp.policy import get_tokenized_fields
from streamshield.observability.logging import configure_json_logging

configure_json_logging(level=logging.INFO)
# Add a file handler with the same JSON format
class _JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {"level": record.levelname, "logger": record.name, "message": record.getMessage()}
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)

file_handler = logging.FileHandler("streamshield.log")
file_handler.setFormatter(_JsonFormatter())
logging.getLogger("streamshield").addHandler(file_handler)



config = SDKConfig(
    gcp=GCPConfig(
        project_id="terraform-testing-498903",
        use_secret_manager=True,
    )
)

TOPIC    = "prescription-events"
GROUP_ID = "streamshield-detokenized-consumer"


def print_detokenized_record(msg: ConsumedMessage) -> None:
    """
    Application handler — print each field with its compliance tier.
    All reversible tokens have already been replaced with plaintext by the time
    this handler is called (detokenize=True in process()).
    """
    tokenized_fields = {f["name"]: f for f in get_tokenized_fields(msg.raw_schema)}
    default_reversible  = msg.raw_schema.get("token.default-reversible", "true")
    default_sensitivity = msg.raw_schema.get("token.default-sensitivity", "PII")

    print(f"\nMessage  partition={msg.partition} offset={msg.offset}")
    print(f"  {'Field':<24}  {'Value':<40}  Note")
    print(f"  {'─' * 24}  {'─' * 40}  {'─' * 22}")

    for field_name, value in msg.value.items():
        meta = tokenized_fields.get(field_name)

        if meta is None:
            print(f"  {field_name:<24}  {str(value):<40}")
        else:
            reversible  = meta.get("token.reversible", default_reversible) != "false"
            sensitivity = meta.get("token.sensitivity", default_sensitivity)

            if reversible:
                note = f"[de-tokenized — {sensitivity}]"
            else:
                note = "[hash — irreversible, original unrecoverable]"

            print(f"  {field_name:<24}  {str(value):<40}  {note}")


print(f"\nSubscribed to '{TOPIC}' as group '{GROUP_ID}'.")
print("De-tokenizing sensitive fields via Cloud DLP (requires KMS cryptoKeyDecrypter).")
print("Press Ctrl+C to stop.\n")

# detokenize=True: the SDK calls DLP reidentifyContent before delivering to handler.
# The handler sees original plaintext (for reversible fields).
with KafkaConsumer(config, group_id=GROUP_ID) as consumer:
    consumer.process(
        handler      = print_detokenized_record,
        topics       = [TOPIC],
        detokenize   = True,           # ← calls DLP reidentifyContent
        idle_timeout_s = 30.0,
    )

print("\nConsumer stopped.")
