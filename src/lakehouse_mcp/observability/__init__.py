"""
Observability module for structured logging, tracing, and metrics.

This module provides integrated observability with:
- Structured logging (structlog)
- Distributed tracing (OpenTelemetry)
- Metrics tracking (OpenTelemetry)

This file has been modified with the assistance of IBM Bob AI tool
"""

from lakehouse_mcp.observability.logging import get_logger, setup_logging
from lakehouse_mcp.observability.metrics import ToolMetrics, get_meter, setup_metrics
from lakehouse_mcp.observability.tracing import get_tracer, setup_tracing

__all__ = [
    "ToolMetrics",
    "get_logger",
    "get_meter",
    "get_tracer",
    # Logging
    "setup_logging",
    # Metrics
    "setup_metrics",
    # Tracing
    "setup_tracing",
]


def setup_observability(
    service_name: str = "ibm-watsonxdata-mcp-server",
    log_level: str = "info",
    otel_enabled: bool = True,
) -> None:
    """Set up all observability components.

    Args:
        service_name: Service name for traces and metrics
        log_level: Logging level (debug, info, warn, error, critical)
        otel_enabled: Whether to enable OpenTelemetry traces and metrics
    """
    setup_logging(log_level=log_level, otel_enabled=otel_enabled)
    setup_tracing(service_name=service_name, enabled=otel_enabled)
    setup_metrics(service_name=service_name, enabled=otel_enabled)
