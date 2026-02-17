"""
List ingestion jobs tool.

This tool lists data ingestion jobs on watsonx.data.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def list_ingestion_jobs(
    ctx: Context,
    start: int | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """List data ingestion jobs on watsonx.data.

    Args:
        start: Offset for pagination (default: 0, 0-based)
        limit: Number of jobs per page (default: 10, max: 100, -1 for all)

    Returns:
        Dict with ingestion_jobs list containing job details
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Build query parameters into path
    path = "/v3/lhingestion/api/v1/ingestion/jobs"
    query_params = []
    
    if start is not None:
        query_params.append(f"start={start}")
    if limit is not None:
        query_params.append(f"limit={limit}")
    
    if query_params:
        path = f"{path}?{'&'.join(query_params)}"

    logger.info(
        "listing_ingestion_jobs",
        start=start,
        limit=limit,
    )

    response = await watsonx_client.get(path)

    job_count = len(response.get("ingestion_jobs", []))
    logger.info(
        "ingestion_jobs_listed",
        job_count=job_count,
    )

    return response