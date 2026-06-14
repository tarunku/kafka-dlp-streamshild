# utils.py — shared helpers: Secret Manager, GCP auth, Kafka config

import google.auth
import google.auth.transport.requests
from google.cloud import secretmanager


def get_secret(project_id: str, secret_name: str) -> str:
    """Fetches the latest version of a secret from GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def get_gcp_bearer_token() -> str:
    """Fetch a short-lived OAuth2 access token via Application Default Credentials."""
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def make_kafka_config(bootstrap_servers: str, extra: dict | None = None) -> dict:
    """
    Returns a confluent-kafka config dict pre-populated with SASL PLAIN credentials
    for Google Managed Kafka. The VM's attached service account token is used —
    no static credentials needed. Caller merges additional keys via `extra`.
    """
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())

    config = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     creds.service_account_email,
        "sasl.password":     creds.token,
        "error_cb":          lambda e: print(f"[KAFKA ERROR] {e}"),
    }
    if extra:
        config.update(extra)
    return config
