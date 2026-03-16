"""Catalog management tools.

This module contains tools for managing watsonx.data catalog metadata
(schemas, tables, columns).
"""

from lakehouse_mcp.tools.catalog.add_columns import add_columns
from lakehouse_mcp.tools.catalog.create_schema import create_schema
from lakehouse_mcp.tools.catalog.describe_table import describe_table
from lakehouse_mcp.tools.catalog.list_schemas import list_schemas
from lakehouse_mcp.tools.catalog.list_tables import list_tables
from lakehouse_mcp.tools.catalog.rename_column import rename_column
from lakehouse_mcp.tools.catalog.rename_table import rename_table

__all__ = [
    "add_columns",
    "create_schema",
    "describe_table",
    "list_schemas",
    "list_tables",
    "rename_column",
    "rename_table",
]
