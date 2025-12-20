"""
FastMCP server setup for watsonx.data MCP server.

This module sets up the FastMCP server with tools and dependencies.

This file has been modified with the assistance of IBM Bob AI tool
"""

import fastmcp

from lakehouse_mcp.client import WatsonXClient
from lakehouse_mcp.config import Config
from lakehouse_mcp.observability import get_logger, get_meter, setup_observability

logger = get_logger(__name__)


def create_server(config: Config) -> fastmcp.FastMCP:
    """Create and configure the FastMCP server.

    Args:
        config: Application configuration

    Returns:
        Configured FastMCP server instance
    """
    # Set up observability
    setup_observability(
        service_name=config.server.otel_service_name,
        log_level=config.server.log_level,
        otel_enabled=config.server.otel_enabled,
    )

    logger.debug("creating_mcp_server", mode=config.server.mode, log_level=config.server.log_level, otel_enabled=config.server.otel_enabled)

    # Create FastMCP server
    mcp = fastmcp.FastMCP(
        name="IBMWatsonxDataMCPServer",
        version="0.1.0",
    )

    # Create WatsonX client (will be passed as dependency to tools)
    watsonx_client = WatsonXClient(config.watsonx)

    # Create metrics tracker
    meter = get_meter(__name__)

    # Store dependencies for tool access
    # Tools can access these via Context
    mcp.dependencies = {
        "config": config,
        "watsonx_client": watsonx_client,
        "meter": meter,
    }

    logger.debug("mcp_server_created", server_name="IBMWatsonxDataMCPServer")

    return mcp


# Global server instance (tools will be registered here)
mcp = create_server(Config())
