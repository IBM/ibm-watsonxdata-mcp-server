"""
watsonx.data MCP tools.

This module imports all tool implementations to ensure they are registered
with the FastMCP server via decorators.

This file has been modified with the assistance of IBM Bob AI tool
"""

# Import tool modules to trigger registration
from lakehouse_mcp.tools.catalog import (
    add_columns,
    create_schema,
    describe_table,
    list_schemas,
    list_tables,
    rename_column,
    rename_table,
)
from lakehouse_mcp.tools.engine import (
    create_presto_engine,
    create_spark_engine,
    list_engines,
    pause_presto_engine,
    pause_spark_engine,
    restart_presto_engine,
    resume_presto_engine,
    resume_spark_engine,
    scale_presto_engine,
    update_presto_engine,
    update_spark_engine,
)
from lakehouse_mcp.tools.ingestion import (
    create_ingestion_job,
    delete_ingestion_job,
    get_ingestion_job,
    list_ingestion_jobs,
)
from lakehouse_mcp.tools.platform import get_instance_details
from lakehouse_mcp.tools.query import (
    execute_insert,
    execute_select,
    execute_update,
    explain_analyze_query,
    explain_query,
)
from lakehouse_mcp.tools.spark_applications import (
    get_spark_application_status,
    list_spark_applications,
    stop_spark_application,
    submit_spark_application,
)

__all__ = [
    "add_columns",
    "create_ingestion_job",
    "create_presto_engine",
    "create_schema",
    "create_spark_engine",
    "delete_ingestion_job",
    "describe_table",
    "execute_insert",
    "execute_select",
    "execute_update",
    "explain_analyze_query",
    "explain_query",
    "get_ingestion_job",
    "get_instance_details",
    "get_spark_application_status",
    "list_engines",
    "list_ingestion_jobs",
    "list_schemas",
    "list_spark_applications",
    "list_tables",
    "pause_presto_engine",
    "pause_spark_engine",
    "rename_column",
    "rename_table",
    "restart_presto_engine",
    "resume_presto_engine",
    "resume_spark_engine",
    "scale_presto_engine",
    "stop_spark_application",
    "submit_spark_application",
    "update_presto_engine",
    "update_spark_engine",
]
