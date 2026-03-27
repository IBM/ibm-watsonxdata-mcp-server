"""
Get Spark application status tool.

This tool retrieves detailed status of a Spark application.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def get_spark_application_status(
    ctx: Context,
    engine_id: str,
    application_id: str,
) -> dict[str, Any]:
    """Get detailed status of a Spark application.

    Args:
        engine_id: Spark engine identifier
        application_id: Application identifier

    Returns:
        Dict with detailed application status including:
        - application_id: Application identifier
        - state: Current state (e.g., "running", "finished", "failed")
        - start_time: Application start timestamp
        - end_time: Application end timestamp (if completed)
        - spark_version: Spark version used
        - application_details: Configuration and runtime details
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "getting_spark_application_status",
        engine_id=engine_id,
        application_id=application_id,
    )

    path = f"/v3/spark_engines/{engine_id}/applications/{application_id}"
    response = await watsonx_client.get(path)

    logger.info(
        "spark_application_status_retrieved",
        engine_id=engine_id,
        application_id=application_id,
        state=response.get("state"),
    )

    return response
