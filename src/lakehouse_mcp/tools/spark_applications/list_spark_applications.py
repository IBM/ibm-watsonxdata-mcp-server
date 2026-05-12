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
    limit: int | None = None,
) -> dict[str, Any]:
    """List Spark applications on a Spark engine.

    Args:
        engine_id: (required) Spark engine identifier
        state: (optional) Filter by application state (e.g., ["running", "finished", "failed"])
        limit: (optional) Maximum number of applications to return (1-1000).
               Recommended to use smaller values (e.g., 10-50) to avoid exhausting tokens.

    Returns:
        Dict with applications list containing application details
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Build query parameters
    query_params = []
    if state is not None:
        # Convert list to comma-separated string for query parameter
        state_param = ",".join(state)
        query_params.append(f"state={state_param}")
    if limit is not None:
        if limit < 1 or limit > 1000:
            return {
                "error": True,
                "error_message": f"limit must be between 1 and 1000, got {limit}",
                "status_code": 400,
            }
        query_params.append(f"limit={limit}")
    
    # Build path with query parameters
    path = f"/v3/spark_engines/{engine_id}/applications"
    if query_params:
        path = f"{path}?{'&'.join(query_params)}"

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
