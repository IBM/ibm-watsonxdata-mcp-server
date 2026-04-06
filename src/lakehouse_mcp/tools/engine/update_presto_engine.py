"""
Update Presto engine configuration tool.

This tool updates Presto engine settings and can trigger a restart.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def update_presto_engine(
    ctx: Context,
    engine_id: str,
    description: str | None = None,
    display_name: str | None = None,
    engine_properties: dict[str, Any] | None = None,
    engine_restart: str | None = None,
    remove_engine_properties: list[str] | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Update Presto engine configuration in watsonx.data.

    Args:
        engine_id: Engine identifier
        description: Updated description
        display_name: Updated display name
        engine_properties: Engine configuration properties
        engine_restart: Set to "force" to trigger restart after update
        remove_engine_properties: Properties to remove
        tags: Updated tags

    Returns:
        Dict with updated engine configuration
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Build request body with only provided parameters
    body: dict[str, Any] = {}
    if description is not None:
        body["description"] = description
    if display_name is not None:
        body["display_name"] = display_name
    if engine_properties is not None:
        body["engine_properties"] = engine_properties
    if engine_restart is not None:
        body["engine_restart"] = engine_restart
    if remove_engine_properties is not None:
        body["remove_engine_properties"] = remove_engine_properties
    if tags is not None:
        body["tags"] = tags

    logger.info(
        "updating_presto_engine",
        engine_id=engine_id,
        fields=list(body.keys()),
    )

    path = f"/v3/presto_engines/{engine_id}"
    response = await watsonx_client.patch(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("update_presto_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "presto_engine_updated",
        engine_id=engine_id,
        restart_triggered=engine_restart == "force",
    )

    return response
