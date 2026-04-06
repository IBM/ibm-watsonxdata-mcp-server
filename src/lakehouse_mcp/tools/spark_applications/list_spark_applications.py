"""
List Spark applications tool.

This tool lists Spark applications on a Spark engine.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def list_spark_applications(
    ctx: Context,
    engine_id: str,
    state: list[str] | None = None,
) -> dict[str, Any]:
    """List Spark applications on a Spark engine.

    Args:
        engine_id: Spark engine identifier
        state: Filter by application state (e.g., ["running", "finished", "failed"])

    Returns:
        Dict with applications list containing application details
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Build query parameters into path
    path = f"/v3/spark_engines/{engine_id}/applications"
    if state is not None:
        # Convert list to comma-separated string for query parameter
        state_param = ",".join(state)
        path = f"{path}?state={state_param}"

    logger.info(
        "listing_spark_applications",
        engine_id=engine_id,
        state_filter=state,
    )

    response = await watsonx_client.get(path)

    # Check for API errors
    if response.get("error"):
        logger.error("list_spark_applications_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    application_count = len(response.get("applications", []))
    logger.info(
        "spark_applications_listed",
        engine_id=engine_id,
        application_count=application_count,
    )

    return response
