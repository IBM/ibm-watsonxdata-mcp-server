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
        - table_name, catalog_name, schema_name, table_type
        - columns: List with column_name, data_type, nullable, comment, ordinal_position
        - column_count: Total columns
        - primary_keys: List of PK column names
        - partitions: List of partition columns
        - properties: Table properties (format, compression, etc.)
        - statistics: row_count, size_bytes, created_on, last_modified
        - engine_id: Echo of input
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "describing_table",
        catalog_name=catalog_name,
        schema_name=schema_name,
        table_name=table_name,
        engine_id=engine_id,
    )

    # Build API path: /v2/catalogs/{catalog}/schemas/{schema}/tables/{table}?engine_id={engine_id}
    path = f"/v2/catalogs/{catalog_name}/schemas/{schema_name}/tables/{table_name}?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.get(path)

    # Handle None response
    response = response or {}

    # Extract table information
    table_info = response.get("table", response) or {}

    # Process columns
    columns = []
    for col in (table_info.get("columns", []) or []):
        column_info = {
            "column_name": col.get("column_name", col.get("name")),
            "data_type": col.get("data_type", col.get("type")),
            "nullable": col.get("nullable", col.get("is_nullable", True)),
            "comment": col.get("comment", col.get("description")),
            "ordinal_position": col.get("ordinal_position", col.get("position")),
        }
        columns.append(column_info)

    # Build result
    result = {
        "table_name": table_name,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "table_type": table_info.get("table_type", "TABLE"),
        "columns": columns,
        "column_count": len(columns),
        "primary_keys": table_info.get("primary_keys", []),
        "partitions": table_info.get("partitions", []),
        "properties": table_info.get("properties", {}),
        "statistics": {
            "row_count": table_info.get("row_count"),
            "size_bytes": table_info.get("size_bytes", table_info.get("size")),
            "created_on": table_info.get("created_on", table_info.get("created_at")),
            "last_modified": table_info.get("last_modified", table_info.get("updated_at")),
        },
        "engine_id": engine_id,
    }

    logger.info(
        "table_described",
        table_name=table_name,
        column_count=len(columns),
        table_type=result["table_type"],
    )

    return result
