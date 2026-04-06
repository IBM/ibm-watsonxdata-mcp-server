"""
Restart Presto engine tool.

This tool restarts a Presto engine in watsonx.data.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def restart_presto_engine(ctx: Context, engine_id: str) -> dict[str, Any]:
    """Restart a Presto engine in watsonx.data.

    Args:
        engine_id: Engine identifier

    Returns:
        Dict with engine_id, status, message, response
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info("restarting_presto_engine", engine_id=engine_id)

    path = f"/v3/presto_engines/{engine_id}/restart"
    response = await watsonx_client.post(path, {})

    # Check for API errors
    if response.get("error"):
        logger.error("restart_presto_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info("presto_engine_restart_initiated", engine_id=engine_id)

    result = {
        "engine_id": engine_id,
        "engine_type": "presto",
        "status": "restarting",
        "message": "Presto engine restart initiated successfully",
        "response": response,
    }

    return result
