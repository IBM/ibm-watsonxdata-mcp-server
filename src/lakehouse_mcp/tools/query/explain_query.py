"""
Explain query execution plan tool.

This tool gets the query execution plan without running the query.
Supports both Presto and Prestissimo engines.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any, Literal

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def explain_query(
    ctx: Context,
    engine_id: str,
    statement: str,
    engine_type: Literal["presto", "prestissimo"] = "presto",
    format: Literal["json", "text"] | None = None,
    type: Literal["logical", "distributed", "validate", "io"] | None = None,
) -> dict[str, Any]:
    """Get query execution plan without running the query in watsonx.data.

    Args:
        engine_id: Presto or Prestissimo engine identifier
        statement: SQL query to explain
        engine_type: Engine type - "presto" or "prestissimo" (default: "presto")
        format: Output format - "json" or "text"
        type: Explain type - "logical", "distributed", "validate", or "io"

    Returns:
        Dict with engine_id, engine_type, statement, plan, and full response
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Validate engine type
    if engine_type not in ["presto", "prestissimo"]:
        raise ValueError(f"Invalid engine_type: {engine_type}. Must be 'presto' or 'prestissimo'")

    logger.info(
        "explaining_query",
        engine_id=engine_id,
        engine_type=engine_type,
        statement_length=len(statement),
        format=format,
        type=type,
    )

    # Build request body
    request_body: dict[str, Any] = {
        "statement": statement,
    }

    # Add optional parameters if provided
    if format is not None:
        request_body["format"] = format
    if type is not None:
        request_body["type"] = type

    # Call appropriate endpoint based on engine type
    path = f"/v3/{engine_type}_engines/{engine_id}/query_explain"
    response = await watsonx_client.post(path, request_body)

    # Handle None response
    response = response or {}

    logger.info(
        "query_explained",
        engine_id=engine_id,
        engine_type=engine_type,
        has_plan=bool(response.get("result")),
    )

    result = {
        "engine_id": engine_id,
        "engine_type": engine_type,
        "statement": statement,
        "plan": response.get("result", ""),
        "response": response,
    }

    return result
