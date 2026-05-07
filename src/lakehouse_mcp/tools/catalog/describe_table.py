"""
Describe table structure in watsonx.data.

This tool retrieves detailed schema information for a specific table.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def describe_table(
    ctx: Context,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    engine_id: str,
) -> dict[str, Any]:
    """Get detailed schema and metadata for a watsonx.data table.

    Args:
        catalog_name: Catalog containing the table (e.g., "iceberg_data", "tpch")
        schema_name: Schema containing the table (from list_schemas)
        table_name: Table to describe (from list_tables)
        engine_id: Engine ID for metadata retrieval (from list_engines)

    Returns:
        Dict with:
        - name: Table name
        - catalog_name, schema_name: Echo of inputs
        - columns: List of column objects with:
          - name: Column name (required)
          - type: SQL data type (required)
          - comment: Optional comment/description
          - extra: Optional extra attributes (e.g., AUTO_INCREMENT)
          - length: Optional length for VARCHAR/CHAR types
          - precision: Optional precision for DECIMAL types
          - scale: Optional scale for DECIMAL types
        - column_count: Total number of columns
        - engine_id: Echo of input
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info(
        "describing_table",
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        engine_id=engine_id,
    )

    # Build API path: /v3/catalogs/{catalog}/schemas/{schema}/tables/{table}?engine_id={engine_id}
    path = f"/v3/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.get(path)

    # Handle None response
    response = response or {}

    # Check for API errors
    if response.get("error"):
        logger.error("describe_table_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    # Extract table information
    # API returns: {"name": "table_name", "columns": [...]}
    name = response.get("name", table_name)
    columns_raw = response.get("columns", []) or []

    # Process columns - only include fields that API actually provides
    columns = []
    for col in columns_raw:
        column_info = {
            "name": col.get("name"),
            "type": col.get("type"),
        }
        
        # Only add optional fields if they have values
        if col.get("comment"):
            column_info["comment"] = col["comment"]
        if col.get("extra"):
            column_info["extra"] = col["extra"]
        if col.get("length"):
            column_info["length"] = col["length"]
        if col.get("precision"):
            column_info["precision"] = col["precision"]
        if col.get("scale"):
            column_info["scale"] = col["scale"]
            
        columns.append(column_info)

    # Build result with only API-provided fields
    result = {
        "name": name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "columns": columns,
        "column_count": len(columns),
        "engine_id": engine_id,
    }

    logger.info(
        "table_described",
        table_name=name,
        column_count=len(columns),
    )

    return result
