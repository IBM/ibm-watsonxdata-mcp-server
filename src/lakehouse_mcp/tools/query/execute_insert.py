"""
Execute INSERT queries in watsonx.data.

This tool executes INSERT statements against watsonx.data engines.

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
async def execute_insert(
    ctx: Context,
    sql: str,
    catalog_name: str,
    schema_name: str,
    engine_id: str,
) -> dict[str, Any]:
    """Execute INSERT queries against watsonx.data.

    Args:
        sql: SQL INSERT query to execute (must start with INSERT)
        catalog_name: Target catalog (e.g., "iceberg_data", "hive_data")
        schema_name: Default schema for unqualified table names
        engine_id: Engine to run query on (from list_engines, must be running)

    Returns:
        Dict with:
        - query_id: Unique query identifier
        - rows_inserted: Number of rows inserted (if available)
        - execution_time_ms: Query duration in milliseconds
        - status: Query execution status
        - catalog_name, schema_name: Echo of inputs
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Validate SQL is not empty
    if not sql or not sql.strip():
        raise ValueError("SQL query cannot be empty")

    # Validate query is an INSERT statement
    sql_trimmed = sql.strip().upper()
    if not sql_trimmed.startswith("INSERT"):
        raise ValueError("Only INSERT queries are allowed. Query must start with INSERT keyword.")

    # Check for unsafe operations (semicolon-separated statements, etc.)
    if ";" in sql.rstrip(";"):
        raise ValueError("Multiple statements not allowed. Query must be a single INSERT statement.")

    logger.info(
        "executing_insert_query",
        catalog_name=catalog_name,
        schema_name=schema_name,
        engine_id=engine_id,
        query_length=len(sql),
    )

    # Build request body for query submission
    request_body = {
        "source": "wxd-sql",
        "catalog": catalog_name,
        "sqlQuery": sql,
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
        "insert_query_submitted",
        query_id=query_id,
        initial_state=state,
        next_uri=next_uri,
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    # Poll until query completes
    max_wait_time = 120  # Maximum 120 seconds for INSERT operations
    poll_interval = 1  # Start with 1 second
    elapsed_time = 0
    empty_response_count = 0

    # Track rows inserted
    rows_inserted = 0

    while True:
        # Check if query is finished
        if state == "FINISHED":
            # Query completed successfully
            # Try to extract rows inserted from update count
            update_count = data.get("updateCount")
            if update_count is not None:
                rows_inserted = update_count
            break

        # Check for error states
        if state == "FAILED":
            error = response.get("error", {}) or {}
            error_message = error.get("message", "Unknown error")
            raise RuntimeError(f"INSERT query failed: {error_message}")

        if state == "CANCELED":
            raise RuntimeError("INSERT query was canceled")

        if elapsed_time >= max_wait_time:
            raise TimeoutError(f"INSERT query {query_id} did not complete within {max_wait_time} seconds")

        # Wait before polling
        await asyncio.sleep(poll_interval)
        elapsed_time += poll_interval

        # Poll by POSTing to the same endpoint with nextUri in the body
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

        # Check for empty responses
        if state == "" and data.get("nextUri", "") == "":
            empty_response_count += 1
            if empty_response_count >= 3:
                raise RuntimeError(f"INSERT query execution failed: received multiple empty responses. Query ID: {query_id}")
        else:
            empty_response_count = 0

        logger.debug(
            "insert_query_polling",
            query_id=query_id,
            state=state,
            next_uri=next_uri,
            elapsed_time=elapsed_time,
        )

    # Calculate execution time
    execution_time_ms = int((time.time() - start_time) * 1000)

    # Build result
    result = {
        "query_id": query_id,
        "rows_inserted": rows_inserted,
        "execution_time_ms": execution_time_ms,
        "status": "success",
        "catalog_name": catalog_name,
        "schema_name": schema_name,
    }

    logger.info(
        "insert_query_executed",
        query_id=query_id,
        rows_inserted=rows_inserted,
        execution_time_ms=execution_time_ms,
        catalog_name=catalog_name,
        schema_name=schema_name,
    )

    return result
