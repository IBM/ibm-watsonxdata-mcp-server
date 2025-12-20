"""
Structured logging configuration using structlog.

This module sets up structured logging integrated with OpenTelemetry traces.

This file has been modified with the assistance of IBM Bob AI tool
"""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from structlog.types import EventDict, Processor


def add_log_level(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
    """Add log level to the event dictionary."""
    event_dict["level"] = method_name.upper()
    return event_dict


def setup_logging(log_level: str = "info", otel_enabled: bool = True) -> None:
    """Configure structured logging with structlog.

    Args:
        log_level: Logging level (debug, info, warn, error, critical)
        otel_enabled: Whether to add OpenTelemetry trace context
    """
    # Map string log level to logging level
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warn": logging.WARNING,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }
    level = level_map.get(log_level.lower(), logging.INFO)

    # Configure standard library logging
    # IMPORTANT: Use stderr for stdio transport - stdout is reserved for JSON-RPC
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=level,
    )

    # Build processor chain
    processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # Add OpenTelemetry trace context if enabled
    if otel_enabled:
        try:
            from opentelemetry import trace

            def add_otel_context(logger: logging.Logger, method_name: str, event_dict: EventDict) -> EventDict:
                """Add OpenTelemetry trace context to logs."""
                span = trace.get_current_span()
                if span and span.is_recording():
                    ctx = span.get_span_context()
                    event_dict["trace_id"] = format(ctx.trace_id, "032x")
                    event_dict["span_id"] = format(ctx.span_id, "016x")
                    event_dict["trace_flags"] = f"{ctx.trace_flags:02x}"
                return event_dict

            processors.append(add_otel_context)
        except ImportError:
            pass  # OpenTelemetry not available

    # Add final rendering processor
    processors.append(structlog.processors.JSONRenderer())

    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a configured logger instance.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)
