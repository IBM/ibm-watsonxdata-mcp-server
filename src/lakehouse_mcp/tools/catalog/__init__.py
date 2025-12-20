"""
Catalog management tools.

This module contains tools for managing watsonx.data catalog metadata
(schemas, tables, columns).

This file has been modified with the assistance of IBM Bob AI tool
"""

from lakehouse_mcp.tools.catalog.describe_table import describe_table
from lakehouse_mcp.tools.catalog.list_schemas import list_schemas
from lakehouse_mcp.tools.catalog.list_tables import list_tables

__all__ = [
    "describe_table",
    "list_schemas",
    "list_tables",
]
