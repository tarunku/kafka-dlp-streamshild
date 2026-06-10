# scripts-dlp — POC reference scripts

These are the original proof-of-concept scripts written during the manual GCP setup phase. They are kept here as reference only.

**For production use, use the [`streamshield/`](../../../streamshield/) SDK instead.** It packages all of the patterns here (DLP tokenization, schema registration, producer, consumer) into a tested, installable Python library.

| Script | SDK equivalent |
|---|---|
| `producer.py` | `streamshield.KafkaProducer` |
| `consumer_tokenized.py` | `streamshield.KafkaConsumer` with `detokenize=False` |
| `consumer_detokenized.py` | `streamshield.KafkaConsumer` with `detokenize=True` |
| `register_schema.py` | `streamshield.SchemaAdmin.register()` |
| `generate_wrapped_dek.py` | One-time ops runbook — no SDK equivalent (key lifecycle is out of scope) |
| `dlp_utils.py` | `streamshield.dlp.tokenizer` / `streamshield.dlp.detokenizer` |
