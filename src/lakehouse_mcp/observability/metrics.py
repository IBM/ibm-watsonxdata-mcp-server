"""
OpenTelemetry metrics configuration.

This module sets up metrics for tool calls, duration, and success/failure tracking.

This file has been modified with the assistance of IBM Bob AI tool
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader

if TYPE_CHECKING:
    from opentelemetry.metrics import Counter, Histogram, Meter


def setup_metrics(service_name: str = "ibm-watsonxdata-mcp-server", enabled: bool = True) -> None:
    """Configure OpenTelemetry metrics.

    Args:
        service_name: Service name for metrics
        enabled: Whether to enable metrics
    """
    if not enabled:
        return

    # Create metric reader with console exporter (for development)
    # CRITICAL: Use stderr to avoid corrupting JSON-RPC protocol on stdout
    # In production, this would be replaced with OTLP exporter to a collector
    reader = PeriodicExportingMetricReader(ConsoleMetricExporter(out=sys.stderr))

    # Create meter provider
    provider = MeterProvider(metric_readers=[reader])

    # Set as global meter provider
    metrics.set_meter_provider(provider)


def get_meter(name: str) -> Meter:
    """Get a meter instance.

    Args:
        name: Meter name (usually __name__)

    Returns:
        OpenTelemetry Meter
    """
    return metrics.get_meter(name)


class ToolMetrics:
    """Tool call metrics tracking."""

    def __init__(self, meter: Meter) -> None:
        """Initialize tool metrics.

        Args:
            meter: OpenTelemetry Meter instance
        """
        self.tool_calls: Counter = meter.create_counter(
            name="tool.calls",
            description="Total number of tool calls",
            unit="1",
        )

        self.tool_duration: Histogram = meter.create_histogram(
            name="tool.duration",
            description="Tool call duration",
            unit="ms",
        )

        self.tool_errors: Counter = meter.create_counter(
            name="tool.errors",
            description="Total number of tool errors",
            unit="1",
        )

    def record_call(self, tool_name: str, status: str = "success") -> None:
        """Record a tool call.

        Args:
            tool_name: Name of the tool
            status: Call status (success, error)
        """
        self.tool_calls.add(1, {"tool": tool_name, "status": status})

    def record_duration(self, tool_name: str, duration_ms: float) -> None:
        """Record tool call duration.

        Args:
            tool_name: Name of the tool
            duration_ms: Duration in milliseconds
        """
        self.tool_duration.record(duration_ms, {"tool": tool_name})

    def record_error(self, tool_name: str, error_type: str) -> None:
        """Record a tool error.

        Args:
            tool_name: Name of the tool
            error_type: Type of error
        """
        self.tool_errors.add(1, {"tool": tool_name, "error_type": error_type})
