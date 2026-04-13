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

    IMPORTANT: The node_type is PERMANENT and cannot be changed after creation. Use size_config "custom"
    for flexible worker counts (1-25). Coordinator and worker MUST use the same node_type.

    Args:
        origin: Engine origin - must be "native" for v3 API
        display_name: Display name for the engine
        configuration: Engine configuration with required fields:
            - size_config: "custom" (flexible workers 1-25) OR "starter"/"small"/"medium"/"large" (fixed: 1/3/6/12)
            - coordinator: {"node_type": "starter"/"small"/"medium"/"large", "quantity": 1}
            - worker: {"node_type": MUST match coordinator, "quantity": 1-25 for custom, fixed for predefined}
        associated_catalogs: List of catalog names to associate
        description: Engine description
        engine_id: Optional custom engine ID (must match pattern: presto-0 to presto-1000)
        tags: Tags for the engine

    Returns:
        Dict with created engine details including engine_id
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Validate configuration structure
    if "size_config" not in configuration:
        raise ValueError("configuration must include 'size_config'")
    if "coordinator" not in configuration:
        raise ValueError("configuration must include 'coordinator' with 'node_type' and 'quantity'")
    if "worker" not in configuration:
        raise ValueError("configuration must include 'worker' with 'node_type' and 'quantity'")

    # Validate coordinator
    coordinator = configuration.get("coordinator", {})
    if "node_type" not in coordinator or "quantity" not in coordinator:
        raise ValueError("coordinator must have 'node_type' and 'quantity'")
    if coordinator["quantity"] != 1:
        raise ValueError(f"coordinator quantity must be 1 for Presto, got {coordinator['quantity']}")

    # Validate worker
    worker = configuration.get("worker", {})
    if "node_type" not in worker or "quantity" not in worker:
        raise ValueError("worker must have 'node_type' and 'quantity'")
    worker_qty = worker["quantity"]
    if worker_qty < 1 or worker_qty > 25:
        raise ValueError(f"worker quantity must be between 1 and 25, got {worker_qty}")

    # Validate node types
    valid_node_types = ["starter", "small", "medium", "large"]
    if coordinator["node_type"] not in valid_node_types:
        raise ValueError(
            f"coordinator node_type must be one of {valid_node_types}, got '{coordinator['node_type']}'"
        )
    if worker["node_type"] not in valid_node_types:
        raise ValueError(
            f"worker node_type must be one of {valid_node_types}, got '{worker['node_type']}'"
        )
    
    # Validate that coordinator and worker use the same node type
    if coordinator["node_type"] != worker["node_type"]:
        raise ValueError(
            f"coordinator and worker must use the same node_type. "
            f"Got coordinator='{coordinator['node_type']}', worker='{worker['node_type']}'"
        )

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
