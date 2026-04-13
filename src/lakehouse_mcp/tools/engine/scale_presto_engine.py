"""
Scale Presto engine tool.

This tool scales a Presto engine by adjusting coordinator and worker node counts.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def scale_presto_engine(
    ctx: Context,
    engine_id: str,
    coordinator_node_type: str,
    coordinator_quantity: int,
    worker_node_type: str,
    worker_quantity: int,
) -> dict[str, Any]:
    """Scale a Presto engine by adjusting coordinator and worker node counts in watsonx.data.

    IMPORTANT LIMITATIONS:
    1. API requires BOTH coordinator AND worker configurations together.
    2. You CANNOT change node types when scaling - must match engine's current node type.
    3. You can only change worker quantity within the same node type tier.

    Args:
        engine_id: Engine identifier
        coordinator_node_type: Must be "starter", "small", "medium", or "large". Must match engine's current type.
        coordinator_quantity: Number of coordinator nodes (must be 1 for Presto)
        worker_node_type: Must be "starter", "small", "medium", or "large". Must match coordinator_node_type.
        worker_quantity: Number of worker nodes (1-25). Typically the only value changed when scaling.

    Returns:
        Dict with scaling operation status and new node configuration
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Validate inputs
    if coordinator_quantity != 1:
        raise ValueError(f"coordinator_quantity must be 1 for Presto engines, got {coordinator_quantity}")
    
    if worker_quantity < 1 or worker_quantity > 25:
        raise ValueError(f"worker_quantity must be between 1 and 25, got {worker_quantity}")

    # Validate node types
    valid_node_types = ["starter", "small", "medium", "large"]
    if coordinator_node_type not in valid_node_types:
        raise ValueError(
            f"coordinator_node_type must be one of {valid_node_types}, got '{coordinator_node_type}'"
        )
    if worker_node_type not in valid_node_types:
        raise ValueError(
            f"worker_node_type must be one of {valid_node_types}, got '{worker_node_type}'"
        )
    
    # Validate that coordinator and worker use the same node type
    if coordinator_node_type != worker_node_type:
        raise ValueError(
            f"coordinator_node_type and worker_node_type must match. "
            f"Got coordinator='{coordinator_node_type}', worker='{worker_node_type}'. "
            f"You cannot change node types when scaling - only worker quantity can be adjusted."
        )

    # Build request body - API requires both coordinator and worker
    body: dict[str, Any] = {
        "coordinator": {
            "node_type": coordinator_node_type,
            "quantity": coordinator_quantity,
        },
        "worker": {
            "node_type": worker_node_type,
            "quantity": worker_quantity,
        },
    }

    logger.info(
        "scaling_presto_engine",
        engine_id=engine_id,
        coordinator=body.get("coordinator"),
        worker=body.get("worker"),
    )

    path = f"/v3/presto_engines/{engine_id}/scale"
    response = await watsonx_client.post(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("scale_presto_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "presto_engine_scaled",
        engine_id=engine_id,
        coordinator=response.get("coordinator"),
        worker=response.get("worker"),
    )

    return response
