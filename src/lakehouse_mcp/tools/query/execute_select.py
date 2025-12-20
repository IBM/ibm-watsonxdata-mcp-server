"""
Execute SELECT queries in watsonx.data.

This tool executes read-only SELECT queries against watsonx.data engines.

This file has been modified with the assistance of IBM Bob AI tool
"""

import asyncio
import time
from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def execute_select(
    ctx: Context,
    sql: str,
    catalog_name: str,
    schema_name: str,
    engine_id: str,
    limit: int | None = None,
) -> dict[str, Any]:
    """Execute read-only SELECT queries against watsonx.data.

    Args:
        sql: SQL SELECT query to execute (must start with SELECT)
        catalog_name: Target catalog (e.g., "iceberg_data", "tpch")
        schema_name: Default schema for unqualified table names
        engine_id: Engine to run query on (from list_engines, must be running)
        limit: Max rows to return (default: 500 if no LIMIT in query)

    Returns:
        Dict with:
        - query_id: Unique query identifier
        - columns: List of {name, type} objects
        - rows: List of row data (list of lists)
        - row_count: Number of rows returned
        - execution_time_ms: Query duration in milliseconds
        - catalog_name, schema_name: Echo of inputs
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Validate query is a SELECT statement
    sql_trimmed = sql.strip().upper()
    if not sql_trimmed.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed. Query must start with SELECT keyword.")

    # Check for unsafe operations (semicolon-separated statements, etc.)
    if ";" in sql.rstrip(";"):
        raise ValueError("Multiple statements not allowed. Query must be a single SELECT statement.")

    # Apply limit if not present in query
    # Default to 500 if no limit specified to prevent large result sets
    final_sql = sql
    if "LIMIT" not in sql_trimmed:
        applied_limit = limit if limit is not None else 500
        final_sql = f"{sql.rstrip(';')} LIMIT {applied_limit}"

    logger.info(
        "executing_select_query",
        catalog_name=catalog_name,
        schema_name=schema_name,
        engine_id=engine_id,
        query_length=len(final_sql),
        has_limit=limit is not None,
    )

    # Build request body for query submission
    # API format: {"source":"wxd-sql","schema":"sf1","catalog":"tpch","sqlQuery":"SELECT ...","prepared_statement":"","session":""}
    request_body = {
        "source": "wxd-sql",
        "catalog": catalog_name,
        "sqlQuery": final_sql,
        "prepared_statement": "",
        "session": "",
        "schema": schema_name if schema_name else "",
    }

    # Track execution time
    start_time = time.time()

    # Submit query - POST /v3/v1/statement?engine_id={engine_id}
    path = f"/v3/v1/statement?engine_id={engine_id}"
    response = await watsonx_client.post(path, request_body)

    # Handle None response
    response = response or {}

    # Response is nested under "data" key
    data = response.get("data", {}) or {}
    query_id = data.get("id")
    next_uri = data.get("nextUri", "")
    stats = data.get("stats", {}) or {}
    state = stats.get("state", "")

    logger.info(
        "query_submitted",
        query_id=query_id,
        initial_state=state,
        next_uri=next_uri,
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    # Poll until query completes
    # States: WAITING_FOR_PREREQUISITES → RUNNING → FINISHED
    max_wait_time = 60  # Maximum 60 seconds total
    poll_interval = 1  # Start with 1 second
    elapsed_time = 0
    empty_response_count = 0

    # Capture columns and rows as soon as we see them
    # The API returns data during RUNNING state, but FINISHED response may not have data
    result_columns = []
    result_rows = []

    while True:
        # Capture data if available (may be in RUNNING state)
        current_columns = data.get("columns", []) or []
        current_rows = data.get("data", []) or []

        if current_columns and not result_columns:
            result_columns = current_columns
        if current_rows and not result_rows:
            result_rows = current_rows

        # Check if query is finished
        # Some responses may not have state but have columns/data, indicating completion
        has_data = len(current_columns) > 0 or len(current_rows) > 0
        if state == "FINISHED" or (state == "" and has_data and next_uri == ""):
            # Query completed successfully
            break

        # Check for error states
        if state == "FAILED":
            error = response.get("error", {}) or {}
            error_message = error.get("message", "Unknown error")
            raise RuntimeError(f"Query failed: {error_message}")

        if state == "CANCELED":
            raise RuntimeError("Query was canceled")

        if elapsed_time >= max_wait_time:
            raise TimeoutError(f"Query {query_id} did not complete within {max_wait_time} seconds")

        # Wait before polling
        await asyncio.sleep(poll_interval)
        elapsed_time += poll_interval

        # Poll by POSTing to the same endpoint with nextUri in the body
        # API requires: POST /v3/v1/statement?engine_id=... with {"source":"wxd-sql","catalog":"...","schema":"...","nextUri":"..."}
        poll_request_body = {
            "source": "wxd-sql",
            "catalog": catalog_name,
            "schema": schema_name if schema_name else "",
            "nextUri": next_uri,
        }

        response = await watsonx_client.post(path, poll_request_body)
        response = response or {}
        data = response.get("data", {}) or {}

        # Update state
        stats = data.get("stats", {}) or {}
        state = stats.get("state", "")

        # Only update nextUri if it's not empty
        if data.get("nextUri"):
            next_uri = data.get("nextUri")

        # Check for empty responses (no state, no data, no nextUri)
        if state == "" and data.get("nextUri", "") == "" and len(data.get("columns", [])) == 0:
            empty_response_count += 1
            if empty_response_count >= 3:
                raise RuntimeError(f"Query execution failed: received multiple empty responses. Query ID: {query_id}")
        else:
            empty_response_count = 0

        logger.debug(
            "query_polling",
            query_id=query_id,
            state=state,
            next_uri=next_uri,
            elapsed_time=elapsed_time,
            columns=len(data.get("columns", [])),
            rows=len(data.get("data", [])),
        )

    # Calculate execution time
    execution_time_ms = int((time.time() - start_time) * 1000)

    # Use captured results (from RUNNING state) if available, otherwise use final response
    columns = result_columns if result_columns else (data.get("columns", []) or [])
    rows = result_rows if result_rows else (data.get("data", []) or [])

    # Build result with column metadata
    result = {
        "query_id": query_id,
        "columns": columns,  # Already includes name and type
        "rows": rows,
        "row_count": len(rows),
        "execution_time_ms": execution_time_ms,
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }

    logger.info(
        "select_query_executed",
        query_id=query_id,
        row_count=result["row_count"],
        column_count=len(columns),
        execution_time_ms=execution_time_ms,
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    return result
