"""
Add columns tool for watsonx.data catalog management.

This tool adds columns to tables.

This file has been created with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def add_columns(
    ctx: Context,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    columns: list[dict[str, Any]],
    engine_id: str,
) -> dict[str, Any]:
    """Add one or more columns to a table in a watsonx.data schema.

    Args:
        catalog_name: Catalog containing the table (e.g., "iceberg_data")
        schema_name: Schema containing the table
        table_name: Table to add columns to
        columns: List of column definitions, each with: name (required), type (required),
                 comment (optional), extra (optional), precision (optional), scale (optional)
        engine_id: Engine ID to use for the operation (from list_engines)

    Returns:
        Dict with:
        - columns: List of added column details
        - total_count: Number of columns added
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "adding_columns",
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        column_count=len(columns),
        engine_id=engine_id,
    )

    try:
        # Build request body
        body = {
            "columns": columns,
        }

        # Build API path
        path = f"/v3/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}/columns?engine_id={engine_id}"

        # Make API call
        response = await watsonx_client.post(path, body)
        
        logger.info(
            "columns_added",
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            column_count=len(columns),
        )

        return response or {}

    except Exception as e:
        logger.error(
            "add_columns_failed",
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            error=str(e),
        )
        raise
