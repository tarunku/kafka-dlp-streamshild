"""
Schema Registry client.

SchemaRegistryClient is the internal HTTP client used by the SDK.
SchemaAdmin is the public-facing API for infrastructure/ops use.

Both classes wrap GCP Managed Kafka's Schema Registry, which exposes the
Confluent Schema Registry REST API.

Thread safety:
  - The schema cache is protected by a threading.RLock so a single
    SchemaRegistryClient instance can safely be shared across threads
    (e.g. a producer thread and a DLQ publisher thread).

Token refresh:
  - get_bearer_token() is called before EVERY HTTP request. GCPAuth
    returns a cached token unless it is within the refresh buffer window,
    so this is cheap under normal circumstances.
"""

from __future__ import annotations

import json
import threading

import fastavro
import requests

from streamshield.auth.gcp import GCPAuth
from streamshield.config import CompatibilityMode, SDKConfig
from streamshield.errors.exceptions import (
    SchemaCompatibilityError,
    SchemaNotFoundError,
    SchemaRegistrationError,
)
from streamshield.observability.logging import schema_logger
from streamshield.observability.metrics import schema_cache_hits, schema_cache_misses
from streamshield.schema.models import (
    CompatibilityResult,
    SchemaDefinition,
    SchemaVersion,
)


class SchemaRegistryClient:
    """
    Internal HTTP client for the Confluent-compatible Schema Registry.

    Caches schemas by schema_id (immutable once registered) and by subject
    (latest version only — invalidated when a new version is registered or
    fetched for the first time after a miss).
    """

    def __init__(self, registry_url: str, auth: GCPAuth):
        self._url = registry_url.rstrip("/")
        self._auth = auth
        self._lock = threading.RLock()

        # Three-level cache:
        #   _by_id      : schema_id → SchemaDefinition  (immutable — safe to cache forever)
        #   _parsed     : schema_id → parsed fastavro schema object
        #   _by_version : (subject, version) → schema_id  (immutable — safe to cache forever)
        # "latest" is never cached — always fetched so producers see new schema versions.
        self._by_id: dict[int, SchemaDefinition] = {}
        self._parsed: dict[int, object] = {}
        self._by_version: dict[tuple[str, int], int] = {}

    # ── Internal HTTP helpers ─────────────────────────────────────────────────

    def _auth_headers(self) -> dict[str, str]:
        """Build HTTP headers with a fresh Bearer token."""
        return {
            "Authorization": f"Bearer {self._auth.get_bearer_token()}",
            "Content-Type": "application/vnd.schemaregistry.v1+json",
        }

    def _get(self, path: str, timeout: int = 10) -> dict:
        """HTTP GET against the Schema Registry. Raises on non-2xx."""
        url = f"{self._url}{path}"
        resp = requests.get(url, headers=self._auth_headers(), timeout=timeout)
        if resp.status_code == 404:
            raise SchemaNotFoundError(
                f"Schema Registry returned 404 for path: {path}",
                safe_context={"path": path},
            )
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict, timeout: int = 15) -> dict:
        """HTTP POST against the Schema Registry. Raises on non-2xx."""
        url = f"{self._url}{path}"
        resp = requests.post(url, headers=self._auth_headers(), json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _put(self, path: str, payload: dict, timeout: int = 15) -> dict:
        """HTTP PUT against the Schema Registry."""
        url = f"{self._url}{path}"
        resp = requests.put(url, headers=self._auth_headers(), json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, timeout: int = 10) -> dict:
        """HTTP DELETE against the Schema Registry."""
        url = f"{self._url}{path}"
        resp = requests.delete(url, headers=self._auth_headers(), timeout=timeout)
        if resp.status_code == 404:
            raise SchemaNotFoundError(f"Schema not found: {path}", safe_context={"path": path})
        resp.raise_for_status()
        return resp.json()

    # ── Cache helpers ─────────────────────────────────────────────────────────

    def _cache_schema(self, schema_id: int, raw_schema: dict) -> None:
        """Store a raw schema and its parsed fastavro form in both caches."""
        with self._lock:
            if schema_id not in self._by_id:
                # fastavro.parse_schema must be called on the raw dict before any
                # read/write operation. Custom logicalType and token.* fields are
                # schema metadata — without parsing, fastavro raises UnknownType.
                parsed = fastavro.parse_schema(raw_schema)
                self._by_id[schema_id] = SchemaDefinition(
                    schema_id=schema_id,
                    schema=raw_schema,
                    schema_type="AVRO",
                )
                self._parsed[schema_id] = parsed

    # ── Public fetch methods ──────────────────────────────────────────────────

    def get_by_id(self, schema_id: int) -> tuple[SchemaDefinition, object]:
        """
        Fetch a schema by its integer ID.

        Returns:
            (SchemaDefinition, parsed_fastavro_schema)
            The parsed schema is the result of fastavro.parse_schema().
        Raises:
            SchemaNotFoundError if no schema with that ID is registered.
        """
        with self._lock:
            if schema_id in self._by_id:
                schema_cache_hits.add(1, {"schema_id": str(schema_id)})
                return self._by_id[schema_id], self._parsed[schema_id]

        # Cache miss — fetch from registry
        schema_cache_misses.add(1, {"schema_id": str(schema_id)})
        schema_logger.info("Schema cache miss — fetching schema_id=%d from registry", schema_id)

        body = self._get(f"/schemas/ids/{schema_id}")
        raw_schema = json.loads(body["schema"])
        self._cache_schema(schema_id, raw_schema)
        schema_logger.debug("Cached schema_id=%d", schema_id)

        with self._lock:
            return self._by_id[schema_id], self._parsed[schema_id]

    def get_latest(self, subject: str) -> SchemaVersion:
        """
        Fetch the latest registered version for a subject.

        Raises:
            SchemaNotFoundError if the subject has no registered schema.
        """
        schema_logger.debug("Fetching latest schema for subject: %s", subject)
        body = self._get(f"/subjects/{subject}/versions/latest")
        schema_id = body["id"]
        version = body["version"]
        raw_schema = json.loads(body["schema"])

        self._cache_schema(schema_id, raw_schema)
        schema_logger.info("Fetched schema subject=%s version=%d schema_id=%d", subject, version, schema_id)

        return SchemaVersion(
            schema_id=schema_id,
            subject=subject,
            version=version,
            schema=raw_schema,
        )

    def get_version(self, subject: str, version: int) -> SchemaVersion:
        """
        Fetch a specific numbered version of a subject's schema.

        (subject, version) pairs are immutable once registered, so the result
        is cached indefinitely — repeated calls for the same version never hit
        the registry after the first fetch.

        Raises:
            SchemaNotFoundError if the subject or version doesn't exist.
        """
        cache_key = (subject, version)
        with self._lock:
            if cache_key in self._by_version:
                schema_id = self._by_version[cache_key]
                schema_cache_hits.add(1, {"schema_id": str(schema_id)})
                schema_logger.debug(
                    "Schema cache hit subject=%s version=%d schema_id=%d",
                    subject, version, schema_id,
                )
                return SchemaVersion(
                    schema_id=schema_id,
                    subject=subject,
                    version=version,
                    schema=self._by_id[schema_id].schema,
                )

        schema_cache_misses.add(1, {"schema_id": f"{subject}:{version}"})
        schema_logger.info(
            "Schema cache miss — fetching subject=%s version=%d from registry",
            subject, version,
        )
        body = self._get(f"/subjects/{subject}/versions/{version}")
        schema_id = body["id"]
        raw_schema = json.loads(body["schema"])
        self._cache_schema(schema_id, raw_schema)

        with self._lock:
            self._by_version[cache_key] = schema_id

        schema_logger.info(
            "Fetched schema subject=%s version=%d schema_id=%d",
            subject, version, schema_id,
        )
        return SchemaVersion(
            schema_id=schema_id,
            subject=subject,
            version=version,
            schema=raw_schema,
        )

    # ── Registration and compatibility ────────────────────────────────────────

    def register(
        self,
        subject: str,
        schema_dict: dict,
        schema_type: str = "AVRO",
    ) -> SchemaVersion:
        """
        Register a schema under the given subject.

        Returns:
            SchemaVersion with the assigned schema_id and version number.
        Raises:
            SchemaRegistrationError on HTTP error.
        """
        schema_logger.info("Registering schema for subject: %s", subject)
        try:
            body = self._post(
                f"/subjects/{subject}/versions",
                payload={"schema": json.dumps(schema_dict), "schemaType": schema_type},
            )
        except requests.HTTPError as exc:
            raise SchemaRegistrationError(
                f"Failed to register schema for subject '{subject}': {exc}",
                safe_context={"subject": subject},
            ) from exc

        schema_id = body["id"]
        # Fetch the full version metadata (id, version number, schema)
        return self.get_latest(subject)

    def check_compatibility(
        self,
        subject: str,
        schema_dict: dict,
    ) -> CompatibilityResult:
        """
        Test whether a schema is compatible with the latest registered version.

        Returns:
            CompatibilityResult(is_compatible, messages)
        Does NOT raise on incompatibility — that is the caller's responsibility.
        """
        try:
            body = self._post(
                f"/compatibility/subjects/{subject}/versions/latest",
                payload={"schema": json.dumps(schema_dict), "schemaType": "AVRO"},
            )
            compatible = body.get("is_compatible", False)
            messages = body.get("messages", [])
            return CompatibilityResult(is_compatible=compatible, messages=messages)
        except SchemaNotFoundError:
            # No existing version — any schema is compatible with empty history
            return CompatibilityResult(is_compatible=True, messages=[])
        except requests.HTTPError as exc:
            schema_logger.warning("Compatibility check failed for subject=%s: %s", subject, exc)
            return CompatibilityResult(is_compatible=False, messages=[str(exc)])

    def set_compatibility(self, subject: str, mode: CompatibilityMode) -> None:
        """Set the compatibility mode for a subject."""
        try:
            self._put(
                f"/config/{subject}",
                payload={"compatibility": mode.value},
            )
            schema_logger.info("Set compatibility mode=%s for subject=%s", mode.value, subject)
        except requests.HTTPError as exc:
            raise SchemaRegistrationError(
                f"Failed to set compatibility mode for '{subject}': {exc}",
                safe_context={"subject": subject, "mode": mode.value},
            ) from exc

    # ── Subject / version management ──────────────────────────────────────────

    def list_subjects(self) -> list[str]:
        """Return all registered subject names."""
        return self._get("/subjects")

    def list_versions(self, subject: str) -> list[int]:
        """Return all version numbers for a subject."""
        return self._get(f"/subjects/{subject}/versions")

    def delete_version(self, subject: str, version: int | str = "latest") -> None:
        """
        Soft-delete a specific schema version.
        Requires the Schema Registry to be configured with READWRITE mode.
        """
        self._delete(f"/subjects/{subject}/versions/{version}")
        schema_logger.info("Deleted schema subject=%s version=%s", subject, version)

    # ── Subject name resolution ───────────────────────────────────────────────

    @staticmethod
    def resolve_subject(topic: str, strategy: str, schema_dict: dict | None = None) -> str:
        """
        Resolve the Schema Registry subject name from a topic and strategy.

        TopicNameStrategy       → "{topic}-value"           (default, matches POC)
        RecordNameStrategy      → "{namespace}.{name}"
        TopicRecordNameStrategy → "{topic}-{namespace}.{name}"
        """
        if strategy == "TopicNameStrategy":
            return f"{topic}-value"

        if schema_dict is None:
            raise ValueError(f"schema_dict required for strategy '{strategy}'")

        namespace = schema_dict.get("namespace", "")
        name = schema_dict.get("name", "")
        fqn = f"{namespace}.{name}" if namespace else name

        if strategy == "RecordNameStrategy":
            return fqn
        if strategy == "TopicRecordNameStrategy":
            return f"{topic}-{fqn}"

        raise ValueError(f"Unknown subject name strategy: {strategy!r}")


# ── Public admin API ──────────────────────────────────────────────────────────

class SchemaAdmin:
    """
    Public API for Schema Registry management operations.

    Used by infrastructure teams to register schemas, set compatibility modes,
    and inspect registered schema versions. Application code typically does not
    call SchemaAdmin directly — the producer and consumer use SchemaRegistryClient
    internally.

    Usage:
        admin = SchemaAdmin(config)
        result = admin.register(
            subject="prescription-events-value",
            schema_definition=build_prescription_schema(...),
            compatibility_mode=CompatibilityMode.BACKWARD,
        )
        print(f"Registered as version {result.version} with ID {result.schema_id}")
    """

    def __init__(self, config: SDKConfig):
        config.validate()
        self._config = config

        # Bootstrap registry URL — from Secret Manager or direct config
        registry_url = self._resolve_registry_url(config)

        self._auth = GCPAuth(
            project_id=config.gcp.project_id,
            token_refresh_buffer_s=config.gcp.token_refresh_buffer_s,
            secrets_refresh_interval_s=config.gcp.secrets_refresh_interval_s,
        )
        self._client = SchemaRegistryClient(registry_url, self._auth)

    def _resolve_registry_url(self, config: SDKConfig) -> str:
        """Resolve the schema registry URL from config or Secret Manager."""
        if config.gcp.schema_registry_url:
            return config.gcp.schema_registry_url
        if config.gcp.use_secret_manager:
            auth = GCPAuth(project_id=config.gcp.project_id)
            return auth.get_secret(config.gcp.schema_registry_url_secret)
        raise ValueError("No schema_registry_url configured and use_secret_manager=False.")

    def register(
        self,
        subject: str,
        schema_definition: dict | str,
        schema_type: str = "AVRO",
        compatibility_mode: CompatibilityMode | None = None,
    ) -> SchemaVersion:
        """
        Register a schema. If compatibility_mode is given, it is applied to the
        subject before registration. Checks compatibility first.

        Args:
            subject:              Schema Registry subject name (e.g. "events-value").
            schema_definition:    Avro schema as a dict or JSON string.
            schema_type:          Always "AVRO" for v1.0.
            compatibility_mode:   If set, applied to the subject before registering.

        Returns:
            SchemaVersion with schema_id and version number.
        Raises:
            SchemaCompatibilityError if the schema violates the compatibility mode.
            SchemaRegistrationError on API failure.
        """
        if isinstance(schema_definition, str):
            schema_definition = json.loads(schema_definition)

        # Apply compatibility mode first if requested.
        # A 403 here means the service account lacks schemaRegistryEditor — the
        # registry's global default still applies, so warn and continue.
        if compatibility_mode is not None:
            try:
                self._client.set_compatibility(subject, compatibility_mode)
            except SchemaRegistrationError as exc:
                cause = exc.__cause__
                if (
                    isinstance(cause, requests.HTTPError)
                    and cause.response is not None
                    and cause.response.status_code == 403
                ):
                    schema_logger.warning(
                        "Cannot set subject-level compatibility for '%s' (403 Forbidden — "
                        "service account may lack schemaRegistryEditor role). "
                        "Proceeding with registration; registry global default applies.",
                        subject,
                    )
                else:
                    raise

        # Check compatibility before writing — fail fast with a clear error
        compat = self._client.check_compatibility(subject, schema_definition)
        if not compat.is_compatible:
            raise SchemaCompatibilityError(
                f"Schema is incompatible with subject '{subject}': {'; '.join(compat.messages)}",
                messages=compat.messages,
                safe_context={"subject": subject},
            )

        return self._client.register(subject, schema_definition, schema_type)

    def get_by_id(self, schema_id: int) -> SchemaDefinition:
        """Fetch a schema by its integer ID."""
        defn, _ = self._client.get_by_id(schema_id)
        return defn

    def get_latest(self, subject: str) -> SchemaVersion:
        """Fetch the latest version for a subject."""
        return self._client.get_latest(subject)

    def get_version(self, subject: str, version: int) -> SchemaVersion:
        """Fetch a specific version for a subject."""
        return self._client.get_version(subject, version)

    def check_compatibility(
        self,
        subject: str,
        schema_definition: dict | str,
    ) -> CompatibilityResult:
        """Test schema compatibility without registering."""
        if isinstance(schema_definition, str):
            schema_definition = json.loads(schema_definition)
        return self._client.check_compatibility(subject, schema_definition)

    def set_compatibility(self, subject: str, mode: CompatibilityMode) -> None:
        """Set the compatibility mode for a subject."""
        self._client.set_compatibility(subject, mode)

    def list_subjects(self) -> list[str]:
        """List all registered subjects."""
        return self._client.list_subjects()

    def list_versions(self, subject: str) -> list[int]:
        """List all version numbers for a subject."""
        return self._client.list_versions(subject)

    def delete_version(self, subject: str, version: int | str = "latest") -> None:
        """Soft-delete a schema version."""
        self._client.delete_version(subject, version)
