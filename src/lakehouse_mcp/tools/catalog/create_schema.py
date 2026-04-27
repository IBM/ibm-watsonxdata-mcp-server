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
    custom_path: str,
    storage_name: str = "",
) -> dict[str, Any]:
    """Create a new schema in a watsonx.data catalog.

    IMPORTANT RESTRICTIONS:
        - Engine must be a Presto or Prestissimo engine (Spark engines are not supported)
        - Catalog must be an object storage catalog (Iceberg or Hive format)
        - Datasource catalogs (e.g., from external databases) are NOT supported for schema creation

    Args:
        catalog_id: Catalog identifier - must be an object storage catalog (e.g., "iceberg_data", "hive_data")
        schema_name: Name for the new schema
        engine_id: Engine ID to use for the operation - must be a Presto or Prestissimo engine (from list_engines)
        custom_path: Path within bucket where schema will be created (REQUIRED, must be at least 1 character).
                     If unsure, use the schema_name as the custom_path.
        storage_name: Storage/bucket name for the schema (REQUIRED for object storage catalogs)

    Returns:
        Dict with:
        - name: Name of the created schema
        - catalog_name: Parent catalog name
        - custom_path: Custom path if specified
        - storage_name: Storage name if specified
        
    Raises:
        Error if:
        - Engine is not Presto or Prestissimo type
        - Catalog is a datasource catalog (not object storage)
        - custom_path is empty or storage_name is not provided
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # Validate custom_path
    if not custom_path or len(custom_path) < 1:
        raise ValueError("custom_path must be at least 1 character long")

    logger.info(
        "creating_schema",
        catalog_id=catalog_id,
        schema_name=schema_name,
        engine_id=engine_id,
    )

    # Build request body
    # Note: API requires 'name' (not 'schema_name') and 'custom_path' is required
    body = {
        "name": schema_name,
        "custom_path": custom_path,
    }
    
    if storage_name:
        body["storage_name"] = storage_name

    # Build API path
    path = f"/v3/catalogs/{catalog_id}/schemas?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.post(path, body)
    
    # Check for API errors
    if response.get("error"):
        logger.error(
            "schema_creation_failed",
            catalog_id=catalog_id,
            schema_name=schema_name,
            error=response.get("error_message"),
        )
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }
    
    logger.info(
        "schema_created",
        catalog_id=catalog_id,
        schema_name=schema_name,
    )

    return response or {}