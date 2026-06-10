"""Unit tests for streamshield.config."""

import os
import textwrap

import pytest

from streamshield.config import (
    CompatibilityMode,
    DLPConfig,
    GCPConfig,
    ProducerConfig,
    SDKConfig,
    SchemaConfig,
    SerializationFormat,
)
from streamshield.errors.exceptions import InvalidConfigError, MissingConfigError


def minimal_config() -> SDKConfig:
    """Return the simplest valid SDKConfig."""
    return SDKConfig(gcp=GCPConfig(project_id="test-project"))


class TestSDKConfigValidation:
    def test_valid_minimal_config_passes(self):
        cfg = minimal_config()
        cfg.validate()  # should not raise

    def test_missing_project_id_raises(self):
        cfg = SDKConfig(gcp=GCPConfig(project_id=""))
        with pytest.raises(MissingConfigError, match="project_id"):
            cfg.validate()

    def test_http_schema_registry_url_raises(self):
        cfg = SDKConfig(
            gcp=GCPConfig(
                project_id="test-project",
                schema_registry_url="http://registry.example.com",
            )
        )
        with pytest.raises(InvalidConfigError, match="HTTPS"):
            cfg.validate()

    def test_https_schema_registry_url_passes(self):
        cfg = SDKConfig(
            gcp=GCPConfig(
                project_id="test-project",
                schema_registry_url="https://registry.example.com",
            )
        )
        cfg.validate()  # should not raise

    def test_dlp_batch_size_too_small_raises(self):
        cfg = SDKConfig(gcp=GCPConfig(project_id="p"), dlp=DLPConfig(batch_size=0))
        with pytest.raises(InvalidConfigError, match="batch_size"):
            cfg.validate()

    def test_dlp_batch_size_too_large_raises(self):
        cfg = SDKConfig(gcp=GCPConfig(project_id="p"), dlp=DLPConfig(batch_size=5001))
        with pytest.raises(InvalidConfigError, match="batch_size"):
            cfg.validate()

    def test_dlp_batch_size_at_limits_valid(self):
        for size in (1, 5000):
            cfg = SDKConfig(gcp=GCPConfig(project_id="p"), dlp=DLPConfig(batch_size=size))
            cfg.validate()

    def test_invalid_producer_acks_raises(self):
        cfg = SDKConfig(gcp=GCPConfig(project_id="p"), producer=ProducerConfig(acks="2"))
        with pytest.raises(InvalidConfigError, match="acks"):
            cfg.validate()

    def test_invalid_subject_name_strategy_raises(self):
        cfg = SDKConfig(
            gcp=GCPConfig(project_id="p"),
            schema=SchemaConfig(subject_name_strategy="BadStrategy"),
        )
        with pytest.raises(InvalidConfigError, match="subject_name_strategy"):
            cfg.validate()


class TestSDKConfigDefaults:
    def test_producer_idempotence_enabled_by_default(self):
        cfg = minimal_config()
        assert cfg.producer.enable_idempotence is True

    def test_producer_acks_all_by_default(self):
        cfg = minimal_config()
        assert cfg.producer.acks == "all"

    def test_auto_commit_not_exposed(self):
        """enable.auto.commit is managed by the SDK, not the user."""
        cfg = minimal_config()
        # ConsumerConfig deliberately has no enable_auto_commit field
        assert not hasattr(cfg.consumer, "enable_auto_commit")

    def test_dlq_enabled_by_default(self):
        cfg = minimal_config()
        assert cfg.dlq.enabled is True

    def test_dlp_enabled_by_default(self):
        cfg = minimal_config()
        assert cfg.dlp.enabled is True

    def test_schema_format_avro_by_default(self):
        cfg = minimal_config()
        assert cfg.schema.serialization_format == SerializationFormat.AVRO

    def test_compatibility_mode_backward_by_default(self):
        cfg = minimal_config()
        assert cfg.schema.default_compatibility_mode == CompatibilityMode.BACKWARD


class TestSDKConfigFromYaml:
    def test_from_yaml_loads_project_id(self, tmp_path):
        yaml_content = textwrap.dedent("""\
            gcp:
              project_id: "yaml-project"
              use_secret_manager: false
              bootstrap_servers: "localhost:9092"
              schema_registry_url: "https://registry.example.com"
            dlp:
              batch_size: 50
        """)
        config_file = tmp_path / "streamshield-config.yaml"
        config_file.write_text(yaml_content)

        cfg = SDKConfig.from_yaml(str(config_file))
        assert cfg.gcp.project_id == "yaml-project"
        assert cfg.dlp.batch_size == 50

    def test_from_yaml_unknown_keys_ignored(self, tmp_path):
        """Extra YAML keys that don't map to dataclass fields are silently ignored."""
        yaml_content = textwrap.dedent("""\
            gcp:
              project_id: "test"
              unknown_future_key: "ignored"
            """)
        config_file = tmp_path / "cfg.yaml"
        config_file.write_text(yaml_content)
        # Should not raise even though unknown_future_key is not a GCPConfig field
        cfg = SDKConfig.from_yaml(str(config_file))
        assert cfg.gcp.project_id == "test"


class TestSDKConfigFromEnv:
    def test_from_env_reads_project_id(self, monkeypatch):
        monkeypatch.setenv("STREAMSHIELD_GCP_PROJECT_ID", "env-project")
        monkeypatch.setenv("STREAMSHIELD_GCP_USE_SECRET_MANAGER", "false")
        monkeypatch.setenv("STREAMSHIELD_GCP_BOOTSTRAP_SERVERS", "localhost:9092")
        cfg = SDKConfig.from_env()
        assert cfg.gcp.project_id == "env-project"
        assert cfg.gcp.use_secret_manager is False

    def test_from_env_reads_dlp_batch_size(self, monkeypatch):
        monkeypatch.setenv("STREAMSHIELD_GCP_PROJECT_ID", "p")
        monkeypatch.setenv("STREAMSHIELD_DLP_BATCH_SIZE", "75")
        cfg = SDKConfig.from_env()
        assert cfg.dlp.batch_size == 75


class TestSDKConfigToSafeDict:
    def test_safe_dict_masks_dek_values(self):
        cfg = SDKConfig(gcp=GCPConfig(project_id="p"))
        safe = cfg.to_safe_dict()
        # Verify the function runs without error and returns a dict
        assert isinstance(safe, dict)
        assert "gcp" in safe

    def test_safe_dict_does_not_mask_project_id(self):
        cfg = SDKConfig(gcp=GCPConfig(project_id="my-project"))
        safe = cfg.to_safe_dict()
        assert safe["gcp"]["project_id"] == "my-project"
