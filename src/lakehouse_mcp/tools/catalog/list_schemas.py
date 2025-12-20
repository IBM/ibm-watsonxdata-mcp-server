"""
List schemas in watsonx.data catalogs.

This tool retrieves schema information from watsonx.data catalogs.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def list_schemas(
    ctx: Context,
    catalog_name: str,
    engine_id: str,
) -> dict[str, Any]:
    """List database schemas in a watsonx.data catalog.

    Args:
        catalog_name: Catalog to list schemas from (e.g., "iceberg_data", "hive_data", "tpch")
        engine_id: Engine ID for metadata queries (from list_engines)

    Returns:
        Dict with:
        - schemas: List of schema objects with schema_name, catalog_name, description,
          created_on, table_count
        - total_count: Number of schemas found
        - catalog_name, engine_id: Echo of inputs
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "listing_schemas",
        catalog_name=catalog_name,
        engine_id=engine_id,
    )

    # Build API path: /v2/catalogs/{catalog_name}/schemas?engine_id={engine_id}
    path = f"/v2/catalogs/{catalog_name}/schemas?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.get(path)

    # Handle None response
    response = response or {}

    # Log raw response for debugging
    logger.debug("raw_api_response", response_keys=list(response.keys()))

    # Extract schemas from response
    # The API returns: {"schemas": ["schema1", "schema2", ...]}
    schemas = response.get("schemas", []) or []

    # If schemas is empty or not found, log the actual response structure
    if not schemas and "schemas" not in response:
        logger.warning(
            "unexpected_response_structure",
            response_keys=list(response.keys()),
            response_sample=str(response)[:500] if response else "empty",
        )

    # Build schema list with metadata
    all_schemas = []
    for schema_name in schemas:
        # API returns simple string array, not objects
        # We only have the schema name from this endpoint
        schema_info = {
            "schema_name": schema_name if isinstance(schema_name, str) else schema_name.get("name", str(schema_name)),
            "catalog_name": catalog_name,
            "description": None,  # Not provided by this endpoint
            "created_on": None,  # Not provided by this endpoint
            "table_count": 0,  # Not provided by this endpoint
        }
        all_schemas.append(schema_info)

    result = {
        "schemas": all_schemas,
        "total_count": len(all_schemas),
        "catalog_name": catalog_name,
        "engine_id": engine_id,
    }

    logger.info(
        "schemas_listed",
        total_count=result["total_count"],
        catalog_name=catalog_name,
    )

    return result
