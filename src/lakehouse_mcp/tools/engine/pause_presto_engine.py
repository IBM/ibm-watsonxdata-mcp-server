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
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info("pausing_presto_engine", engine_id=engine_id)

    try:
        # Pause the engine (empty POST request)
        path = f"/v3/presto_engines/{engine_id}/pause"
        response = await watsonx_client.post(path, {})

        # Build result from API response
        result = {
            "message": response.get("response", {}).get("message", "Success"),
            "message_code": response.get("response", {}).get("message_code", "success"),
            "engine_id": engine_id,
        }

        logger.info("presto_engine_paused", engine_id=engine_id)

        return result

    except Exception as e:
        logger.error("pause_presto_engine_error", engine_id=engine_id, error=str(e))
        raise
