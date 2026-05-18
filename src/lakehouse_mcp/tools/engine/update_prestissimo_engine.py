"""
Update Prestissimo engine configuration tool.

This tool updates Prestissimo engine settings and can trigger a restart.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def update_prestissimo_engine(
    ctx: Context,
    engine_id: str,
    description: str | None = None,
    display_name: str | None = None,
    properties: dict[str, Any] | None = None,
    restart_type: str | None = None,
    remove_engine_properties: dict[str, Any] | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Update Prestissimo engine configuration in watsonx.data.
    
    IMPORTANT: Due to a known API limitation, the `description` field cannot be
    updated on its own. If you want to update the description, you must include 
    at least one additional field in the same call (e.g., `display_name`, `tags`,
    or `properties`). Calls that modify only `description` will fail.
    
    Args:
        engine_id: Engine identifier.
        description: Updated description (1-50 characters). Must be accompanied
        by at least one other updatable field in the same request.
        display_name: Updated display name.
        properties: Engine configuration properties.
        restart_type: Set to "force" to trigger a restart after the update.
        remove_engine_properties: Properties to remove.
        tags: Updated tags.

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
    if properties is not None:
        body["properties"] = properties
    if restart_type is not None:
        body["restart_type"] = restart_type
    if remove_engine_properties is not None:
        body["remove_engine_properties"] = remove_engine_properties
    if tags is not None:
        body["tags"] = tags

    logger.info(
        "updating_prestissimo_engine",
        engine_id=engine_id,
        fields=list(body.keys()),
    )

    path = f"/v3/prestissimo_engines/{engine_id}"
    response = await watsonx_client.patch(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("update_prestissimo_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "prestissimo_engine_updated",
        engine_id=engine_id,
        restart_triggered=restart_type == "force",
    )

    return response
