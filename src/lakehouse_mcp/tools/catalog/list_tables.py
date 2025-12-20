"""
List tables in watsonx.data schemas.

This tool retrieves table information from watsonx.data schemas.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def list_tables(
    ctx: Context,
    catalog_name: str,
    schema_name: str,
    engine_id: str,
) -> dict[str, Any]:
    """List tables in a watsonx.data schema.

    Args:
        catalog_name: Catalog containing the schema (e.g., "iceberg_data", "hive_data")
        schema_name: Schema/database containing tables (from list_schemas)
        engine_id: Engine ID for metadata queries (from list_engines)

    Returns:
        Dict with:
        - tables: List of table objects with name, type, created_on, row_count, size_bytes, column_count
        - total_count: Number of tables in schema
        - catalog_name, schema_name, engine_id: Echo of inputs
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info(
        "listing_tables",
        catalog_name=catalog_name,
        schema_name=schema_name,
        engine_id=engine_id,
    )

    # Build API path: /v2/catalogs/{catalog}/schemas/{schema}/tables?engine_id={engine_id}
    path = f"/v2/catalogs/{catalog_name}/schemas/{schema_name}/tables?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.get(path)

    # Handle None response
    response = response or {}

    # Extract tables from response
    # The API returns: {"tables": ["table1", "table2", ...]}
    tables = response.get("tables", []) or []

    # Process table information
    processed_tables = []
    for table in tables:
        # API returns simple string array, not objects
        # We only have the table name from this endpoint
        if isinstance(table, str):
            table_info = {
                "name": table,
                "type": "TABLE",  # Not provided by this endpoint
                "created_on": None,  # Not provided by this endpoint
                "row_count": None,  # Not provided by this endpoint
                "size_bytes": None,  # Not provided by this endpoint
                "column_count": 0,  # Not provided by this endpoint
            }
        else:
            # Handle dict response (for backward compatibility or future API changes)
            table_info = {
                "name": table.get("table_name", table.get("name")),
                "type": table.get("table_type", table.get("type", "TABLE")),
                "created_on": table.get("created_on", table.get("created_at")),
                "row_count": table.get("row_count"),
                "size_bytes": table.get("size_bytes", table.get("size")),
                "column_count": table.get("column_count", len(table.get("columns", []))),
            }
        processed_tables.append(table_info)

    result = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "tables": processed_tables,
        "total_count": len(processed_tables),
        "engine_id": engine_id,
    }

    logger.info(
        "tables_listed",
        total_count=result["total_count"],
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    return result
