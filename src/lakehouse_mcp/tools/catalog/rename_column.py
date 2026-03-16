"""
Rename column tool for watsonx.data catalog management.

This tool renames columns in tables.

This file has been created with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def rename_column(
    ctx: Context,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    column_name: str,
    new_column_name: str,
    engine_id: str,
) -> dict[str, Any]:
    """Rename a column in a table in a watsonx.data schema.

    Args:
        catalog_name: Catalog containing the table (e.g., "iceberg_data")
        schema_name: Schema containing the table
        table_name: Table containing the column
        column_name: Current column name
        new_column_name: New name for the column
        engine_id: Engine ID to use for the operation (from list_engines)

    Returns:
        Dict with column details including the new name
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "renaming_column",
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        column_name=column_name,
        new_column_name=new_column_name,
        engine_id=engine_id,
    )

    try:
        # Build request body
        body = {
            "name": new_column_name,
        }

        # Build API path
        path = f"/v3/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns/{column_name}?engine_id={engine_id}"

        # Make API call
        response = await watsonx_client.patch(path, body)
        
        logger.info(
            "column_renamed",
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            old_name=column_name,
            new_name=new_column_name,
        )

        return response or {}

    except Exception as e:
        logger.error(
            "column_rename_failed",
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            new_column_name=new_column_name,
            error=str(e),
        )
        raise
