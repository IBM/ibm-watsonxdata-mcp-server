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
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info("resuming_spark_engine", engine_id=engine_id)

    try:
        # Resume the engine (empty POST request)
        path = f"/v3/spark_engines/{engine_id}/resume"
        response = await watsonx_client.post(path, {})

        # Build result from API response
        result = {
            "message": response.get("message", "resume spark engine"),
            "message_code": response.get("message_code", "success"),
            "engine_id": engine_id,
        }

        logger.info("spark_engine_resumed", engine_id=engine_id)

        return result

    except ValueError as e:
        logger.error("resume_spark_engine_validation_error", engine_id=engine_id, error=str(e))
        raise

    except Exception as e:
        logger.error("resume_spark_engine_error", engine_id=engine_id, error=str(e))
        raise

