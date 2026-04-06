"""
Resume Spark engine tool.

This tool resumes a paused Spark engine (SAAS only).

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def resume_spark_engine(ctx: Context, engine_id: str) -> dict[str, Any]:
    """Resume a paused Spark engine in watsonx.data (SAAS only).

    Args:
        engine_id: Engine identifier

    Returns:
        Dict with resume operation status
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info("resuming_spark_engine", engine_id=engine_id)

    # Resume the engine (empty POST request)
    path = f"/v3/spark_engines/{engine_id}/resume"
    response = await watsonx_client.post(path, {})

    # Check for API errors
    if response.get("error"):
        logger.error("resume_spark_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    # Build result from API response
    result = {
        "message": response.get("message", "resume spark engine"),
        "message_code": response.get("message_code", "success"),
        "engine_id": engine_id,
    }

    logger.info("spark_engine_resumed", engine_id=engine_id)

    return result
