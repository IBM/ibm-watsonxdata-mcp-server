"""
OpenTelemetry tracing configuration.

This module sets up distributed tracing with automatic instrumentation.

This file has been modified with the assistance of IBM Bob AI tool
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer


def setup_tracing(service_name: str = "ibm-watsonxdata-mcp-server", enabled: bool = True) -> None:
    """Configure OpenTelemetry tracing.

    Args:
        service_name: Service name for traces
        enabled: Whether to enable tracing
    """
    if not enabled:
        return

    # Create tracer provider
    provider = TracerProvider()

    # Add console exporter (for development)
    # CRITICAL: Use stderr to avoid corrupting JSON-RPC protocol on stdout
    # In production, this would be replaced with OTLP exporter to a collector
    processor = BatchSpanProcessor(ConsoleSpanExporter(out=sys.stderr))
    provider.add_span_processor(processor)

    # Set as global tracer provider
    trace.set_tracer_provider(provider)

    # Instrument httpx for automatic HTTP tracing
    HTTPXClientInstrumentor().instrument()


def get_tracer(name: str) -> Tracer:
    """Get a tracer instance.

    Args:
        name: Tracer name (usually __name__)

    Returns:
        OpenTelemetry Tracer
    """
    return trace.get_tracer(name)
