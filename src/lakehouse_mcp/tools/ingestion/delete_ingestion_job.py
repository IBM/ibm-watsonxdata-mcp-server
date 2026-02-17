"""
Delete ingestion job tool.

This tool cancels/deletes a data ingestion job.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

import httpx
from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def delete_ingestion_job(
    ctx: Context,
    job_id: str,
) -> dict[str, Any]:
    """Cancel/delete a data ingestion job.

    Note: Jobs in 'failed' or 'completed' status cannot be deleted.
    Only jobs in 'starting', 'running', or 'queued' status can be deleted.

    Args:
        job_id: Job identifier

    Returns:
        Dict with deletion status
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "deleting_ingestion_job",
        job_id=job_id,
    )

    try:
        path = f"/v3/ingestion_jobs/{job_id}"
        response = await watsonx_client.delete(path)

        logger.info(
            "ingestion_job_deleted",
            job_id=job_id,
        )

        return response

    except Exception as e:
        logger.error(
            "ingestion_job_deletion_failed",
            job_id=job_id,
            error=str(e),
        )
        raise