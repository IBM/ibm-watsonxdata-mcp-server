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

    RECOMMENDED NODE TYPES: "starter" or "cache_optimized" (other types may be available)
    
    SCALING CAPABILITIES:
    - Coordinator quantity: Always 1 (cannot be changed)
    - Worker quantity: 1-18 (recommended), up to 50 may be supported
    - Node types CAN be changed during scaling (e.g., from "starter" to "cache_optimized")
    - Coordinator and worker do NOT need to match node types
    
    API REQUIREMENT: Must provide BOTH coordinator AND worker configurations together.

    Args:
        engine_id: Engine identifier
        coordinator_node_type: Typically "starter" or "cache_optimized". Can be different from current type.
        coordinator_quantity: Number of coordinator nodes (must be 1 for Presto)
        worker_node_type: Typically "starter" or "cache_optimized". Can be different from coordinator_node_type.
        worker_quantity: Number of worker nodes (1-18 recommended, up to 50 may be supported)

    Returns:
        Dict with scaling operation status and new node configuration
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Validate inputs
    if coordinator_quantity != 1:
        return {
            "error": True,
            "error_message": f"coordinator_quantity must be 1 for Presto engines, got {coordinator_quantity}",
            "status_code": 400,
        }
    
    if worker_quantity < 1 or worker_quantity > 1000:
        return {
            "error": True,
            "error_message": f"worker_quantity must be between 1 and 1000 (1-18 recommended), got {worker_quantity}",
            "status_code": 400,
        }

    # Note: Node type validation removed - API accepts various node types depending on environment
    # Common types: "starter", "cache_optimized", but others may be available

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
