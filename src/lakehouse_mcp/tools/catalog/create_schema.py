"""
Create schema tool for watsonx.data catalog management.

This tool creates new schemas in a catalog.

This file has been created with the assistance of IBM Bob AI tool
"""

from typing import Any

import httpx
from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def create_schema(
    ctx: Context,
    catalog_id: str,
    schema_name: str,
    engine_id: str,
    custom_path: str = "",
    storage_name: str = "",
) -> dict[str, Any]:
    """Create a new schema in a watsonx.data catalog.

    Args:
        catalog_id: Catalog identifier (e.g., "iceberg_data")
        schema_name: Name for the new schema
        engine_id: Engine ID to use for the operation (from list_engines)
        custom_path: Path within bucket where schema will be created (default: "")
        storage_name: Storage/bucket name for the schema (REQUIRED for object storage catalogs)

    Returns:
        Dict with:
        - name: Name of the created schema
        - catalog_name: Parent catalog name
        - custom_path: Custom path if specified
        - storage_name: Storage name if specified
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "creating_schema",
        catalog_id=catalog_id,
        schema_name=schema_name,
        engine_id=engine_id,
    )

    try:
        # Build request body
        # Note: API requires 'name' (not 'schema_name') and 'custom_path' is required
        body = {
            "name": schema_name,
            "custom_path": custom_path if custom_path else "",
        }
        
        if storage_name:
            body["storage_name"] = storage_name

        # Build API path
        path = f"/v3/catalogs/{catalog_id}/schemas?engine_id={engine_id}"

        # Make API call
        response = await watsonx_client.post(path, body)
        
        logger.info(
            "schema_created",
            catalog_id=catalog_id,
            schema_name=schema_name,
        )

        return response or {}

    except Exception as e:
        logger.error(
            "schema_creation_failed",
            catalog_id=catalog_id,
            schema_name=schema_name,
            error=str(e),
        )
        raise