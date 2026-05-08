"""
Scale Spark engine tool.

This tool scales a Spark engine by adjusting node count.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def scale_spark_engine(
    ctx: Context,
    engine_id: str,
    number_of_nodes: int,
) -> dict[str, Any]:
    """Scale a Spark engine by adjusting node count in watsonx.data (SAAS only).

    Args:
        engine_id: Engine identifier
        number_of_nodes: Target node count (1-1000)

    Returns:
        Dict with scaling operation status (202 Accepted - asynchronous operation)
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info("scaling_spark_engine", engine_id=engine_id, number_of_nodes=number_of_nodes)

    # Validate number_of_nodes
    if number_of_nodes < 1 or number_of_nodes > 1000:
        return {
            "error": True,
            "error_message": f"number_of_nodes must be between 1 and 1000, got {number_of_nodes}",
            "status_code": 400,
        }

    # Build request body
    body = {"number_of_nodes": number_of_nodes}

    # Scale the engine
    path = f"/v3/spark_engines/{engine_id}/scale"
    response = await watsonx_client.post(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("scale_spark_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "spark_engine_scaled",
        engine_id=engine_id,
        number_of_nodes=number_of_nodes,
    )

    return response
