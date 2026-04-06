"""
Stop Spark application tool.

This tool stops and removes a Spark application.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def stop_spark_application(
    ctx: Context,
    engine_id: str,
    application_id: str,
) -> dict[str, Any]:
    """Stop and remove a Spark application.

    This will terminate a running application and remove it from the engine's history.

    Args:
        engine_id: Spark engine identifier
        application_id: Application identifier to stop

    Returns:
        Dict with operation status
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info(
        "stopping_spark_application",
        engine_id=engine_id,
        application_id=application_id,
    )

    # Build path with application_id in the URL path
    path = f"/v3/spark_engines/{engine_id}/applications/{application_id}"
    response = await watsonx_client.delete(path)

    # Check for API errors
    if response.get("error"):
        logger.error("stop_spark_application_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "spark_application_stopped",
        engine_id=engine_id,
        application_id=application_id,
    )

    return response
