"""
Cancel ingestion job tool.

This tool cancels a data ingestion job.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

import httpx
from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def cancel_ingestion_job(
    ctx: Context,
    job_id: str,
) -> dict[str, Any]:
    """Cancel a data ingestion job.

    Args:
        job_id: Job identifier

    Returns:
        Dict with cancellation status
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info(
        "cancelling_ingestion_job",
        job_id=job_id,
    )

    path = f"/v3/lhingestion/api/v1/ingestion/jobs/{job_id}"
    response = await watsonx_client.delete(path)

    # Check for API errors
    if response.get("error"):
        logger.error("cancel_ingestion_job_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "ingestion_job_cancelled",
        job_id=job_id,
    )

    return response
    