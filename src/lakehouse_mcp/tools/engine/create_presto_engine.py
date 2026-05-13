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

    EXAMPLE PAYLOAD:
    {
        "origin": "native",
        "display_name": "My-Presto-Engine",
        "description": "Presto engine with autoscaling",
        "tags": [],
        "associated_catalogs": [],
        "configuration": {
            "size_config": "custom",
            "coordinator": {
                "node_type": "starter",
                "quantity": 1
            },
            "worker": {
                "node_type": "starter",
                "quantity": 1
            },
            "autoscaling_enabled": true,
            "autoscaling_config": {
                "type": "cpu",
                "target": 40,
                "min_worker_quantity": 1,
                "max_worker_quantity": 18,
                "query_termination_grace_period_min": 1,
                "scale_in_stabilization_window_min": 5,
                "scaling_step_size": 1
            }
        }
    }

    Args:
        origin: (required) "native"
        display_name: (required) Display name for the engine
        configuration: (required) Engine configuration with required fields:
            - size_config: (required) "custom" (recommended) or predefined options (may be supported)
            - coordinator: (required) {"node_type": typically "starter" or "cache_optimized", "quantity": 1}
            - worker: (required) {"node_type": typically "starter" or "cache_optimized", "quantity": 1-18 recommended}
            - autoscaling_enabled: (optional) boolean to enable autoscaling
            - autoscaling_config: (required if autoscaling_enabled is true) autoscaling configuration object (see AUTOSCALING section)
        associated_catalogs: (optional) List of catalog names to associate
        description: (optional) Engine description 50 characters max
        engine_id: (optional) Custom engine ID (must match pattern: presto-0 through presto-1000)
        tags: (optional) Tags for the engine

    AUTOSCALING (OPTIONAL):
    To enable autoscaling, include these fields in the configuration:
    - autoscaling_enabled: true (boolean)
    - autoscaling_config: {
        "type": "cpu" or "memory",
        "target": 1-100 (target utilization percentage, e.g., 40),
        "min_worker_quantity": 1-18 (minimum workers),
        "max_worker_quantity": 1-18 (maximum workers),
        "query_termination_grace_period_min": 1-120 (grace period before terminating queries),
        "scale_in_stabilization_window_min": 5-60 (stabilization window for scale-in),
        "scaling_step_size": 1-18 (nodes to add/remove per scaling action)
      }
    
    PREDEFINED SIZE CONFIGS:
    If using predefined configs, exact node types and quantities must match:
    - starter: 1 coordinator + 1 worker (both bx2.48x192)
    - small: 1 coordinator + 3 workers (both ox2.16x128)
    - medium: 1 coordinator + 6 workers (both ox2.16x128)
    - large: 1 coordinator + 12 workers (both ox2.16x128)
    
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
    if worker_qty < 1 or worker_qty > 50:
        return {
            "error": True,
            "error_message": f"worker quantity must be between 1 and 50 (1-18 recommended), got {worker_qty}",
            "status_code": 400,
        }

    # Note: Node type validation removed - API accepts various node types depending on environment
    # Common types: "starter", "cache_optimized", but others may be available

    # Validate autoscaling configuration if provided
    autoscaling_enabled = configuration.get("autoscaling_enabled")
    autoscaling_config = configuration.get("autoscaling_config")
    
    if autoscaling_enabled is not None and not isinstance(autoscaling_enabled, bool):
        return {
            "error": True,
            "error_message": "autoscaling_enabled must be a boolean",
            "status_code": 400,
        }
    
    if autoscaling_config is not None:
        # Validate autoscaling_config structure
        if not isinstance(autoscaling_config, dict):
            return {
                "error": True,
                "error_message": "autoscaling_config must be a dictionary",
                "status_code": 400,
            }
        
        # Validate required field: type
        if "type" not in autoscaling_config:
            return {
                "error": True,
                "error_message": "autoscaling_config must include 'type' field",
                "status_code": 400,
            }
        
        config_type = autoscaling_config.get("type")
        if config_type not in ["cpu", "memory"]:
            return {
                "error": True,
                "error_message": f"autoscaling_config type must be 'cpu' or 'memory', got '{config_type}'",
                "status_code": 400,
            }
        
        # Validate optional fields with their constraints
        target = autoscaling_config.get("target")
        if target is not None and (not isinstance(target, int) or target < 1 or target > 100):
            return {
                "error": True,
                "error_message": f"autoscaling_config target must be an integer between 1 and 100, got {target}",
                "status_code": 400,
            }
        
        min_workers = autoscaling_config.get("min_worker_quantity")
        if min_workers is not None and (not isinstance(min_workers, int) or min_workers < 1 or min_workers > 18):
            return {
                "error": True,
                "error_message": f"autoscaling_config min_worker_quantity must be an integer between 1 and 18, got {min_workers}",
                "status_code": 400,
            }
        
        max_workers = autoscaling_config.get("max_worker_quantity")
        if max_workers is not None and (not isinstance(max_workers, int) or max_workers < 1 or max_workers > 18):
            return {
                "error": True,
                "error_message": f"autoscaling_config max_worker_quantity must be an integer between 1 and 18, got {max_workers}",
                "status_code": 400,
            }
        
        # Validate min <= max if both are provided
        if min_workers is not None and max_workers is not None and min_workers > max_workers:
            return {
                "error": True,
                "error_message": f"autoscaling_config min_worker_quantity ({min_workers}) cannot be greater than max_worker_quantity ({max_workers})",
                "status_code": 400,
            }
        
        grace_period = autoscaling_config.get("query_termination_grace_period_min")
        if grace_period is not None and (not isinstance(grace_period, int) or grace_period < 1 or grace_period > 120):
            return {
                "error": True,
                "error_message": f"autoscaling_config query_termination_grace_period_min must be an integer between 1 and 120, got {grace_period}",
                "status_code": 400,
            }
        
        stabilization = autoscaling_config.get("scale_in_stabilization_window_min")
        if stabilization is not None and (not isinstance(stabilization, int) or stabilization < 5 or stabilization > 60):
            return {
                "error": True,
                "error_message": f"autoscaling_config scale_in_stabilization_window_min must be an integer between 5 and 60, got {stabilization}",
                "status_code": 400,
            }
        
        step_size = autoscaling_config.get("scaling_step_size")
        if step_size is not None and (not isinstance(step_size, int) or step_size < 1 or step_size > 18):
            return {
                "error": True,
                "error_message": f"autoscaling_config scaling_step_size must be an integer between 1 and 18, got {step_size}",
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
