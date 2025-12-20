"""
watsonx.data MCP tools.

This module imports all tool implementations to ensure they are registered
with the FastMCP server via decorators.

This file has been modified with the assistance of IBM Bob AI tool
"""

# Import tool modules to trigger registration
from lakehouse_mcp.tools.catalog import (
    describe_table,
    list_schemas,
    list_tables,
)
from lakehouse_mcp.tools.engine import list_engines
from lakehouse_mcp.tools.platform import get_instance_details
from lakehouse_mcp.tools.query import execute_select

__all__ = [
    "describe_table",
    "execute_select",
    "get_instance_details",
    "list_engines",
    "list_schemas",
    "list_tables",
]
