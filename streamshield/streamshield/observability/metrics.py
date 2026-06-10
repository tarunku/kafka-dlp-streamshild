"""
StreamShield optional OpenTelemetry metrics.

Metrics are completely optional. If 'opentelemetry-sdk' is not installed, all
metric calls are no-ops and no ImportError is raised. Install the 'metrics'
extras to activate: pip install 'streamshield[metrics]'

Available metrics:
    streamshield_messages_produced_total    — per topic, per status (success/failed)
    streamshield_messages_consumed_total    — per topic, group_id, status
    streamshield_dlp_calls_total            — per operation (tokenize/detokenize), status
    streamshield_dlp_call_duration_seconds  — histogram of DLP API latency
    streamshield_dlp_records_per_call       — histogram of batch sizes
    streamshield_schema_cache_hits_total    — cache efficiency
    streamshield_schema_cache_misses_total
    streamshield_token_refreshes_total      — ADC token refresh events
    streamshield_dlq_messages_total         — DLQ routing events per reason
    streamshield_offset_commits_total       — commit success/failure
"""

from __future__ import annotations

# Try to import OpenTelemetry. If not installed, fall back to no-op implementations.
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider

    _provider = MeterProvider()
    otel_metrics.set_meter_provider(_provider)
    _meter = otel_metrics.get_meter("streamshield", version="0.1.0")
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False


def configure_console_metrics(export_interval_ms: int = 10_000) -> None:
    """
    Attach a ConsoleMetricExporter to the streamshield MeterProvider.

    Prints all SDK metrics to stdout every export_interval_ms milliseconds.
    Useful for local development and debugging — not for production.

    Note: ConsoleMetricExporter writes directly to stdout via print(). Metrics
    will NOT appear in the log file. Use configure_logging_metrics() instead
    if you need metrics in the log file.

    Requires: pip install 'streamshield[metrics]'

    Args:
        export_interval_ms: How often (in milliseconds) to print metrics. Default 10s.

    Raises:
        ImportError if opentelemetry-sdk is not installed.
    """
    if not _OTEL_AVAILABLE:
        raise ImportError(
            "opentelemetry-sdk is not installed. "
            "Run: pip install 'streamshield[metrics]'"
        )

    from opentelemetry.sdk.metrics.export import (
        ConsoleMetricExporter,
        PeriodicExportingMetricReader,
    )

    global _provider
    reader = PeriodicExportingMetricReader(
        ConsoleMetricExporter(),
        export_interval_millis=export_interval_ms,
    )
    _provider = MeterProvider(metric_readers=[reader])
    otel_metrics.set_meter_provider(_provider)


def configure_logging_metrics(export_interval_ms: int = 10_000, level: int = 20) -> None:
    """
    Route OTel metric snapshots through the Python logging system.

    Each export interval, all SDK metrics are emitted as a single JSON log line
    via the 'streamshield.metrics' logger at the given level. Because it goes
    through Python logging, the snapshot lands in every attached handler —
    including FileHandlers — alongside the regular SDK logs.

    Requires: pip install 'streamshield[metrics]'

    Args:
        export_interval_ms: How often (in milliseconds) to emit a snapshot. Default 10s.
        level:              Python logging level (default: logging.INFO = 20).

    Raises:
        ImportError if opentelemetry-sdk is not installed.
    """
    if not _OTEL_AVAILABLE:
        raise ImportError(
            "opentelemetry-sdk is not installed. "
            "Run: pip install 'streamshield[metrics]'"
        )

    import json
    import logging
    from opentelemetry.sdk.metrics.export import MetricExporter, MetricExportResult
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

    _metrics_logger = logging.getLogger("streamshield.metrics")

    class _LoggingMetricExporter(MetricExporter):
        """Formats OTel metric snapshots as JSON and emits via Python logging."""

        def export(self, metrics_data, **kwargs):
            snapshot = {}
            for resource_metrics in metrics_data.resource_metrics:
                for scope_metrics in resource_metrics.scope_metrics:
                    for metric in scope_metrics.metrics:
                        points = []
                        for dp in metric.data.data_points:
                            point = {"attributes": dict(dp.attributes or {})}
                            # Counters and gauges have a single value; histograms have sum/count/min/max
                            if hasattr(dp, "value"):
                                point["value"] = dp.value
                            else:
                                point["sum"]   = round(dp.sum, 6)
                                point["count"] = dp.count
                                if dp.count > 0:
                                    point["min"] = round(dp.min, 6)
                                    point["max"] = round(dp.max, 6)
                            points.append(point)
                        if points:
                            snapshot[metric.name] = points

            if snapshot:
                _metrics_logger.log(level, json.dumps({"metrics": snapshot}))
            return MetricExportResult.SUCCESS

        def shutdown(self, **kwargs):
            pass

        def force_flush(self, timeout_millis: int = 30_000, **kwargs):
            return True

    global _provider
    reader = PeriodicExportingMetricReader(
        _LoggingMetricExporter(),
        export_interval_millis=export_interval_ms,
    )
    _provider = MeterProvider(metric_readers=[reader])
    otel_metrics.set_meter_provider(_provider)


# ── Counter helper ────────────────────────────────────────────────────────────

class _NoOpCounter:
    """Stands in for an OTel counter when the SDK is not installed."""
    def add(self, amount: int = 1, attributes: dict | None = None) -> None:
        pass


class _NoOpHistogram:
    """Stands in for an OTel histogram when the SDK is not installed."""
    def record(self, amount: float, attributes: dict | None = None) -> None:
        pass


def _counter(name: str, description: str) -> object:
    if _OTEL_AVAILABLE:
        return _meter.create_counter(name, description=description, unit="1")
    return _NoOpCounter()


def _histogram(name: str, description: str, unit: str = "s") -> object:
    if _OTEL_AVAILABLE:
        return _meter.create_histogram(name, description=description, unit=unit)
    return _NoOpHistogram()


# ── Metric instruments ────────────────────────────────────────────────────────

messages_produced = _counter(
    "streamshield_messages_produced_total",
    "Total messages produced, labelled by topic and status",
)

messages_consumed = _counter(
    "streamshield_messages_consumed_total",
    "Total messages consumed, labelled by topic, group_id, and status",
)

dlp_calls = _counter(
    "streamshield_dlp_calls_total",
    "Total Cloud DLP API calls, labelled by operation and status",
)

dlp_call_duration = _histogram(
    "streamshield_dlp_call_duration_seconds",
    "Latency of Cloud DLP API calls in seconds",
)

dlp_records_per_call = _histogram(
    "streamshield_dlp_records_per_call",
    "Number of records sent per DLP API call",
    unit="records",
)

schema_cache_hits = _counter(
    "streamshield_schema_cache_hits_total",
    "Schema Registry cache hits by subject",
)

schema_cache_misses = _counter(
    "streamshield_schema_cache_misses_total",
    "Schema Registry cache misses (HTTP fetches) by subject",
)

token_refreshes = _counter(
    "streamshield_token_refreshes_total",
    "ADC OAuth2 token refresh events",
)

dlq_messages = _counter(
    "streamshield_dlq_messages_total",
    "Messages routed to DLQ, labelled by source_topic and reason",
)

offset_commits = _counter(
    "streamshield_offset_commits_total",
    "Consumer offset commit attempts, labelled by topic, group_id, and status",
)
