"""
Pause Spark engine tool.

This tool pauses a running Spark engine (SAAS only).

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def pause_spark_engine(
    ctx: Context,
    engine_id: str,
    force: bool = False,
) -> dict[str, Any]:
    """Pause a running Spark engine in watsonx.data (SAAS only).

    Args:
        engine_id: Engine identifier
        force: Force pause even if applications are running (default: False)

    Returns:
        Dict with pause operation status
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info("pausing_spark_engine", engine_id=engine_id, force=force)

    # Build request body
    body = {"force": force}

    # Pause the engine
    path = f"/v3/spark_engines/{engine_id}/pause"
    response = await watsonx_client.post(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("pause_spark_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    # Build result from API response
    result = {
        "message": response.get("message", "pause spark engine"),
        "message_code": response.get("message_code", "success"),
        "engine_id": engine_id,
        "forced": force,
    }

    logger.info("spark_engine_paused", engine_id=engine_id, force=force)

    return result
