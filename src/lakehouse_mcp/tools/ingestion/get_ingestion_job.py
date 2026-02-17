"""
Get ingestion job tool.

This tool gets detailed status of a data ingestion job.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def get_ingestion_job(
    ctx: Context,
    job_id: str,
) -> dict[str, Any]:
    """Get detailed status of a data ingestion job.

    Args:
        job_id: Job identifier

    Returns:
        Dict with detailed job status, configuration, and execution details
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "getting_ingestion_job",
        job_id=job_id,
    )

    path = f"/v3/lhingestion/api/v1/ingestion/jobs/{job_id}"
    response = await watsonx_client.get(path)

    logger.info(
        "ingestion_job_retrieved",
        job_id=job_id,
        status=response.get("status"),
    )

    return response