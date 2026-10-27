"""
Pause Prestissimo engine tool.

This tool pauses a running Prestissimo engine to save resources while preserving configuration.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any
from urllib.parse import quote

from fastmcp import Context
from fastmcp.exceptions import ToolError

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def pause_prestissimo_engine(ctx: Context, engine_id: str) -> dict[str, Any]:
    """Pause a running Prestissimo engine in watsonx.data.

    Args:
        engine_id: Engine identifier

    Returns:
        Dict with pause operation status and engine state transition
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info("pausing_prestissimo_engine", engine_id=engine_id)

    # Pause the engine (empty POST request)
    path = f"/v3/prestissimo_engines/{quote(engine_id, safe='')}/pause"
    response = await watsonx_client.post(path, {})

    # Build result from API response
    result = {
        "message": response.get("response", {}).get("message", "Success"),
        "message_code": response.get("response", {}).get("message_code", "success"),
        "engine_id": engine_id,
    }

    logger.info("prestissimo_engine_paused", engine_id=engine_id)

    return result
