"""
Rename table tool for watsonx.data catalog management.

This tool renames tables in a schema.

This file has been created with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def rename_table(
    ctx: Context,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    new_table_name: str,
    engine_id: str,
) -> dict[str, Any]:
    """Rename a table in a watsonx.data schema.

    Args:
        catalog_name: Catalog containing the table (e.g., "iceberg_data")
        schema_name: Schema containing the table
        table_name: Current table name
        new_table_name: New name for the table
        engine_id: Engine ID to use for the operation (from list_engines)

    Returns:
        Dict with table details including the new name
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info(
        "renaming_table",
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        new_table_name=new_table_name,
        engine_id=engine_id,
    )

    # Build request body
    body = {
        "name": new_table_name,
    }

    # Build API path
    path = f"/v3/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.patch(path, body)
    
    # Check for API errors
    if response.get("error"):
        logger.error(
            "table_rename_failed",
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            new_table_name=new_table_name,
            error=response.get("error_message"),
        )
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }
    
    logger.info(
        "table_renamed",
        catalog_name=catalog_name,
        schema_name=schema_name,
        old_name=table_name,
        new_name=new_table_name,
    )

    return response or {}
