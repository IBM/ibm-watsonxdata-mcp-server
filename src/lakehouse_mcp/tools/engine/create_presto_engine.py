"""
Create Presto engine tool.

This tool creates a new Presto engine in watsonx.data.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def create_presto_engine(
    ctx: Context,
    origin: str,
    display_name: str,
    configuration: dict[str, Any],
    associated_catalogs: list[str] | None = None,
    description: str | None = None,
    engine_id: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Create a new Presto engine in watsonx.data.

    VALID NODE TYPES: Only "starter" and "cache_optimized" are valid.
    
    PREDEFINED SIZE CONFIGS (If any of this size config payload are not matching, it will fail):
    - starter: 1 coordinator + 1 worker (both "starter" node_type)
    - small: 1 coordinator + 3 workers (both "cache_optimized" node_type)
    - medium: 1 coordinator + 6 workers (both "cache_optimized" node_type)
    - large: 1 coordinator + 12 workers (both "cache_optimized" node_type)
    
    CUSTOM SIZE CONFIG:
    - Coordinator: 1 node (always), node_type: "starter" or "cache_optimized"
    - Worker: 1-18 nodes, node_type: "starter" or "cache_optimized"
    - Node types do NOT need to match (e.g., starter coordinator + cache_optimized worker is allowed)
    
    NOTE: Node types CAN be changed later during scaling operations.

    Args:
        origin: Engine origin - must be "native" for v3 API
        display_name: Display name for the engine
        configuration: Engine configuration with required fields:
            - size_config: "starter"/"small"/"medium"/"large" (predefined) OR "custom" (flexible)
            - coordinator: {"node_type": "starter" or "cache_optimized", "quantity": 1}
            - worker: {"node_type": "starter" or "cache_optimized", "quantity": varies by size_config}
        associated_catalogs: List of catalog names to associate
        description: Engine description
        engine_id: Optional custom engine ID (must match pattern: presto-0 through presto-1000)
        tags: Tags for the engine

    Returns:
        Dict with created engine details including engine_id
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Validate configuration structure
    if "size_config" not in configuration:
        return {
            "error": True,
            "error_message": "configuration must include 'size_config'",
            "status_code": 400,
        }
    if "coordinator" not in configuration:
        return {
            "error": True,
            "error_message": "configuration must include 'coordinator' with 'node_type' and 'quantity'",
            "status_code": 400,
        }
    if "worker" not in configuration:
        return {
            "error": True,
            "error_message": "configuration must include 'worker' with 'node_type' and 'quantity'",
            "status_code": 400,
        }

    # Validate coordinator
    coordinator = configuration.get("coordinator", {})
    if "node_type" not in coordinator or "quantity" not in coordinator:
        return {
            "error": True,
            "error_message": "coordinator must have 'node_type' and 'quantity'",
            "status_code": 400,
        }
    if coordinator["quantity"] != 1:
        return {
            "error": True,
            "error_message": f"coordinator quantity must be 1 for Presto, got {coordinator['quantity']}",
            "status_code": 400,
        }

    # Validate worker
    worker = configuration.get("worker", {})
    if "node_type" not in worker or "quantity" not in worker:
        return {
            "error": True,
            "error_message": "worker must have 'node_type' and 'quantity'",
            "status_code": 400,
        }
    worker_qty = worker["quantity"]
    if worker_qty < 1 or worker_qty > 18:
        return {
            "error": True,
            "error_message": f"worker quantity must be between 1 and 18, got {worker_qty}",
            "status_code": 400,
        }

    # Validate node types
    valid_node_types = ["starter", "cache_optimized"]
    if coordinator["node_type"] not in valid_node_types:
        return {
            "error": True,
            "error_message": f"coordinator node_type must be one of {valid_node_types}, got '{coordinator['node_type']}'",
            "status_code": 400,
        }
    if worker["node_type"] not in valid_node_types:
        return {
            "error": True,
            "error_message": f"worker node_type must be one of {valid_node_types}, got '{worker['node_type']}'",
            "status_code": 400,
        }

    # Build request body according to API v3 schema
    body: dict[str, Any] = {
        "origin": origin,
        "display_name": display_name,
        "configuration": configuration,
    }
    if associated_catalogs is not None:
        body["associated_catalogs"] = associated_catalogs
    if description is not None:
        body["description"] = description
    if engine_id is not None:
        body["id"] = engine_id
    if tags is not None:
        body["tags"] = tags

    logger.info("creating_presto_engine", display_name=display_name, origin=origin)

    path = "/v3/presto_engines"
    response = await watsonx_client.post(path, body)

    # Check for API errors
    if response.get("error"):
        logger.error("create_presto_engine_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info("presto_engine_created", engine_id=response.get("id"))

    return response
