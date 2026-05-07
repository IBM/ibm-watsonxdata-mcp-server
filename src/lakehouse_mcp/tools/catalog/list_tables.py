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
        - tables: List of table names (strings)
        - total_count: Number of tables in schema
        - catalog_name, schema_name, engine_id: Echo of inputs
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    logger.info(
        "listing_tables",
        catalog_name=catalog_name,
        schema_name=schema_name,
        engine_id=engine_id,
    )

    # Build API path: /v3/catalogs/{catalog}/schemas/{schema}/tables?engine_id={engine_id}
    path = f"/v3/catalogs/{catalog_name}/schemas/{schema_name}/tables?engine_id={engine_id}"

    # Make API call
    response = await watsonx_client.get(path)

    # Handle None response
    response = response or {}

    # Check for API errors
    if response.get("error"):
        logger.error("list_tables_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    # Extract tables from response
    # The API returns: {"tables": ["table1", "table2", ...]}
    tables = response.get("tables", []) or []

    result = {
        "catalog_name": catalog_name,
        "schema_name": schema_name,
        "tables": tables,
        "total_count": len(tables),
        "engine_id": engine_id,
    }

    logger.info(
        "tables_listed",
        total_count=result["total_count"],
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    return result
