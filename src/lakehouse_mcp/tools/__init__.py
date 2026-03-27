"""
watsonx.data MCP tools.

This module imports all tool implementations to ensure they are registered
with the FastMCP server via decorators.

This file has been modified with the assistance of IBM Bob AI tool
"""

# Import tool modules to trigger registration
from lakehouse_mcp.tools.catalog import (
    create_schema,
    describe_table,
    list_schemas,
    list_tables,
)
from lakehouse_mcp.tools.engine import list_engines
from lakehouse_mcp.tools.ingestion import (
    create_ingestion_job,
    delete_ingestion_job,
    get_ingestion_job,
    list_ingestion_jobs,
)
from lakehouse_mcp.tools.platform import get_instance_details
from lakehouse_mcp.tools.query import execute_insert, execute_select, execute_update
from lakehouse_mcp.tools.spark_applications import (
    get_spark_application_status,
    list_spark_applications,
    stop_spark_application,
    submit_spark_application,
)

__all__ = [
    "create_ingestion_job",
    "create_schema",
    "delete_ingestion_job",
    "describe_table",
    "execute_insert",
    "execute_select",
    "execute_update",
    "get_ingestion_job",
    "get_instance_details",
    "get_spark_application_status",
    "list_engines",
    "list_ingestion_jobs",
    "list_schemas",
    "list_spark_applications",
    "list_tables",
    "stop_spark_application",
    "submit_spark_application",
]
