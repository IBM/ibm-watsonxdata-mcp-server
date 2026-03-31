"""
Resume Presto engine tool.

This tool resumes a paused Presto engine to restore query processing capability.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def resume_presto_engine(ctx: Context, engine_id: str) -> dict[str, Any]:
    """Resume a paused Presto engine in watsonx.data.

    Args:
        engine_id: Engine identifier

    Returns:
        Dict with resume operation status and engine state transition
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info("resuming_presto_engine", engine_id=engine_id)

    try:
        # Resume the engine (empty POST request)
        path = f"/v3/presto_engines/{engine_id}/resume"
        response = await watsonx_client.post(path, {})

        # Build result from API response
        result = {
            "message": response.get("message", "Success"),
            "message_code": response.get("message_code", "success"),
            "engine_id": engine_id,
        }

        logger.info("presto_engine_resumed", engine_id=engine_id)

        return result

    except Exception as e:
        logger.error("resume_presto_engine_error", engine_id=engine_id, error=str(e))
        raise
