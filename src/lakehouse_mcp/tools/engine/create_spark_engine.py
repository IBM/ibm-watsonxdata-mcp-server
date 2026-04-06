"""
Create Spark engine tool.

This tool creates a new Spark engine in watsonx.data.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def create_spark_engine(
    ctx: Context,
    origin: str,
    display_name: str,
    storage_name: str,
    associated_catalogs: list[str] | None = None,
    description: str | None = None,
    default_version: str = "3.5",
    default_config: dict[str, Any] | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Create a new Spark engine in watsonx.data.

    Args:
        origin: Engine origin - "native", "external", or "discover"
        display_name: Display name for the engine
        storage_name: Storage/bucket name for engine_home (REQUIRED)
        associated_catalogs: List of catalog IDs to associate
        description: Engine description
        default_version: Spark version (default: "3.5")
        default_config: Additional engine configuration
        tags: Tags for the engine

    Returns:
        Dict with created engine details including engine_id
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Build configuration with mandatory engine_home
    configuration: dict[str, Any] = {
        "default_version": default_version,
        "engine_home": {
            "storage_name": storage_name
        }
    }
    
    # Add default_config if provided
    if default_config is not None:
        configuration["default_config"] = default_config

    # Build request body
    body: dict[str, Any] = {
        "origin": origin,
        "type": "spark",
        "display_name": display_name,
        "configuration": configuration,
    }
    if associated_catalogs is not None:
        body["associated_catalogs"] = associated_catalogs
    if description is not None:
        body["description"] = description
    if tags is not None:
        body["tags"] = tags

    logger.info("creating_spark_engine", display_name=display_name, origin=origin)

    path = "/v3/spark_engines"
    response = await watsonx_client.post(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("create_spark_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info("spark_engine_created", engine_id=response.get("engine_id"))

    return response
