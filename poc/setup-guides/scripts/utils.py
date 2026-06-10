# utils.py — Secret Manager helper
from google.cloud import secretmanager


def get_secret(project_id: str, secret_name: str) -> str:
    """
    Fetches the latest version of a secret from GCP Secret Manager.

    Args:
        project_id:  GCP project ID (e.g., "kafka-poc")
        secret_name: Name of the secret (e.g., "kafka-bootstrap-servers")

    Returns:
        The secret value as a plain string.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")