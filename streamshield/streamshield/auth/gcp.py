"""
GCP authentication and credential management.

GCPAuth is the single point of truth for credentials in the SDK. It wraps:
  - Application Default Credentials (ADC) — short-lived OAuth2 tokens for
    SASL PLAIN auth to Google Managed Kafka and Bearer auth to Schema Registry.
  - GCP Secret Manager — fetches bootstrap servers, schema registry URL,
    and any other secrets needed at startup.

Key improvements over the POC's utils.py:
  - Token expiry is tracked. Tokens are refreshed proactively when they are
    within 'token_refresh_buffer_s' of expiry (default: 5 minutes).
  - Secret Manager values are cached in process memory. They are re-fetched
    only when secrets_refresh_interval_s has elapsed (if configured).
  - All methods are synchronous and thread-safe.
"""

from __future__ import annotations

import threading
import time

import google.auth
import google.auth.transport.requests
from google.cloud import secretmanager

from streamshield.errors.exceptions import AuthenticationError, TokenRefreshError
from streamshield.observability.logging import auth_logger
from streamshield.observability.metrics import token_refreshes


class GCPAuth:
    """
    Manages GCP Application Default Credentials and Secret Manager access.

    Thread-safe: a single GCPAuth instance can be shared by a producer and
    DLQ publisher running on different threads.
    """

    def __init__(
        self,
        project_id: str,
        token_refresh_buffer_s: int = 300,
        secrets_refresh_interval_s: int | None = None,
    ):
        self._project_id = project_id
        self._token_refresh_buffer_s = token_refresh_buffer_s
        self._secrets_refresh_interval_s = secrets_refresh_interval_s

        # Lock protects concurrent token refreshes
        self._lock = threading.RLock()

        # Cached OAuth2 credentials object from google.auth
        self._credentials: google.auth.credentials.Credentials | None = None
        self._service_account_email: str = ""

        # Cached Secret Manager values: {secret_name: (value, fetched_at_epoch)}
        self._secret_cache: dict[str, tuple[str, float]] = {}
        self._sm_client: secretmanager.SecretManagerServiceClient | None = None

        # Initialise credentials at construction time so startup failures are caught early
        self._refresh_credentials()

    # ── Token management ──────────────────────────────────────────────────────

    def _refresh_credentials(self) -> None:
        """
        Fetch or refresh ADC credentials. Called at init and whenever the token
        is within token_refresh_buffer_s of expiry.
        """
        try:
            creds, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            creds.refresh(google.auth.transport.requests.Request())
            self._credentials = creds

            # Service account email is available on SA credentials; use 'unknown' otherwise
            self._service_account_email = getattr(creds, "service_account_email", "unknown")

            auth_logger.info(
                "GCP credentials refreshed. SA: %s token_expiry=%s",
                self._service_account_email,
                getattr(creds, "expiry", "N/A"),
            )
            token_refreshes.add(1, {"reason": "refresh"})
        except Exception as exc:
            raise TokenRefreshError(
                f"Failed to refresh GCP Application Default Credentials: {exc}"
            ) from exc

    def is_token_expiring_soon(self) -> bool:
        """
        Returns True if the current token will expire within token_refresh_buffer_s.
        A True result means build_kafka_config() should be called again before the
        next Kafka operation.
        """
        if self._credentials is None:
            return True
        expiry = getattr(self._credentials, "expiry", None)
        if expiry is None:
            return False  # non-expiring credentials (e.g. user credentials in dev)
        # expiry is a naive UTC datetime
        from datetime import datetime, timezone
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        remaining = (expiry - now_utc).total_seconds()
        return remaining < self._token_refresh_buffer_s

    def ensure_fresh_token(self) -> None:
        """
        Refresh the token if it is expiring soon. Call this before any operation
        that uses the token (Schema Registry fetch, Kafka produce, DLP call).
        """
        with self._lock:
            if self.is_token_expiring_soon():
                auth_logger.warning("Token expiring soon — proactively refreshing.")
                self._refresh_credentials()

    def get_bearer_token(self) -> str:
        """
        Returns a valid OAuth2 Bearer token string for HTTP Authorization headers.
        Refreshes the token if it is close to expiry.
        """
        self.ensure_fresh_token()
        if self._credentials is None or not self._credentials.token:
            raise AuthenticationError("No valid GCP credentials available.")
        return self._credentials.token

    def build_kafka_config(self, bootstrap_servers: str, extra: dict | None = None) -> dict:
        """
        Build a confluent_kafka config dict with SASL PLAIN credentials for
        Google Managed Kafka. Always uses SASL_SSL — plaintext is never allowed.

        Args:
            bootstrap_servers: Kafka broker address(es).
            extra: Additional config keys merged into the result (e.g. group.id).

        Returns:
            Dict ready to pass to confluent_kafka.Producer() or Consumer().
        """
        self.ensure_fresh_token()
        if self._credentials is None:
            raise AuthenticationError("GCP credentials not initialised.")

        config: dict = {
            "bootstrap.servers":  bootstrap_servers,
            "security.protocol":  "SASL_SSL",          # always TLS — no plaintext Kafka
            "sasl.mechanisms":    "PLAIN",
            "sasl.username":      self._service_account_email,
            "sasl.password":      self._credentials.token,
        }
        if extra:
            config.update(extra)

        auth_logger.debug("Kafka config built for SA: %s", self._service_account_email)
        return config

    # ── Secret Manager ────────────────────────────────────────────────────────

    def _get_sm_client(self) -> secretmanager.SecretManagerServiceClient:
        """Lazy-initialise the Secret Manager client (avoids GCP calls during unit tests)."""
        if self._sm_client is None:
            self._sm_client = secretmanager.SecretManagerServiceClient()
        return self._sm_client

    def get_secret(self, secret_name: str) -> str:
        """
        Fetch the latest version of a secret from GCP Secret Manager.

        Results are cached in process memory. If secrets_refresh_interval_s is
        configured, the cache entry is invalidated after that interval.

        Args:
            secret_name: The short secret name (not the full resource path).

        Returns:
            The secret value as a UTF-8 string.
        """
        now = time.monotonic()

        # Check cache first
        if secret_name in self._secret_cache:
            value, fetched_at = self._secret_cache[secret_name]
            if (
                self._secrets_refresh_interval_s is None
                or (now - fetched_at) < self._secrets_refresh_interval_s
            ):
                auth_logger.debug("Secret cache hit: %s", secret_name)
                return value

        # Fetch from Secret Manager
        auth_logger.info("Fetching secret from Secret Manager: %s", secret_name)
        try:
            client = self._get_sm_client()
            name = f"projects/{self._project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            value = response.payload.data.decode("UTF-8").strip()
            self._secret_cache[secret_name] = (value, now)
            return value
        except Exception as exc:
            raise AuthenticationError(
                f"Failed to fetch secret '{secret_name}' from Secret Manager: {exc}"
            ) from exc
