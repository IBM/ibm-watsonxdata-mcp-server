"""
Pause Presto engine tool.

This tool pauses a running Presto engine to save resources while preserving configuration.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def pause_presto_engine(ctx: Context, engine_id: str) -> dict[str, Any]:
    """Pause a running Presto engine in watsonx.data.

    Args:
        engine_id: Engine identifier

    Returns:
        Dict with pause operation status and engine state transition
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info("pausing_presto_engine", engine_id=engine_id)

    # Pause the engine (empty POST request)
    path = f"/v3/presto_engines/{engine_id}/pause"
    response = await watsonx_client.post(path, {})

    # Check for API errors
    if response.get("error"):
        logger.error("pause_presto_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    # Build result from API response
    result = {
        "message": response.get("response", {}).get("message", "Success"),
        "message_code": response.get("response", {}).get("message_code", "success"),
        "engine_id": engine_id,
    }

    logger.info("presto_engine_paused", engine_id=engine_id)

    return result

