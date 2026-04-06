"""
Update Spark engine configuration tool.

This tool updates Spark engine settings.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def update_spark_engine(
    ctx: Context,
    engine_id: str,
    description: str | None = None,
    display_name: str | None = None,
    configuration: dict[str, Any] | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Update Spark engine configuration in watsonx.data.

    Args:
        engine_id: Engine identifier
        description: Updated description
        display_name: Updated display name
        configuration: Engine configuration (default_config, default_version, engine_home)
        tags: Updated tags

    Returns:
        Dict with updated engine configuration

    Note:
        Spark engines do NOT support engine_restart parameter.
        Configuration changes may require manual restart.
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Build request body with only provided parameters
    body: dict[str, Any] = {}
    if description is not None:
        body["description"] = description
    if display_name is not None:
        body["display_name"] = display_name
    if configuration is not None:
        body["configuration"] = configuration
    if tags is not None:
        body["tags"] = tags

    logger.info(
        "updating_spark_engine",
        engine_id=engine_id,
        fields=list(body.keys()),
    )

    path = f"/v3/spark_engines/{engine_id}"
    response = await watsonx_client.patch(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("update_spark_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "spark_engine_updated",
        engine_id=engine_id,
    )

    return response
