"""
Explain analyze query tool.

This tool gets detailed query analysis with execution statistics.
Supports both Presto and Prestissimo engines.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any, Literal

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def explain_analyze_query(
    ctx: Context,
    engine_id: str,
    statement: str,
    engine_type: Literal["presto", "prestissimo"] = "presto",
    verbose: bool | None = None,
) -> dict[str, Any]:
    """Get detailed query analysis with execution statistics in watsonx.data.

    Args:
        engine_id: Presto or Prestissimo engine identifier
        statement: SQL query to analyze
        engine_type: Engine type - "presto" or "prestissimo" (default: "presto")
        verbose: Include detailed statistics

    Returns:
        Dict with engine_id, engine_type, statement, analysis, and full response
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Validate engine type
    if engine_type not in ["presto", "prestissimo"]:
        raise ValueError(f"Invalid engine_type: {engine_type}. Must be 'presto' or 'prestissimo'")

    logger.info(
        "explaining_analyzing_query",
        engine_id=engine_id,
        engine_type=engine_type,
        statement_length=len(statement),
        verbose=verbose,
    )

    # Build request body
    request_body: dict[str, Any] = {
        "statement": statement,
    }

    # Add optional parameters if provided
    if verbose is not None:
        request_body["verbose"] = verbose

    # Call appropriate endpoint based on engine type
    path = f"/v3/{engine_type}_engines/{engine_id}/query_explain_analyze"
    response = await watsonx_client.post(path, request_body)

    # Handle None response
    response = response or {}

    logger.info(
        "query_explained_analyzed",
        engine_id=engine_id,
        engine_type=engine_type,
        has_analysis=bool(response.get("result")),
    )

    result = {
        "engine_id": engine_id,
        "engine_type": engine_type,
        "statement": statement,
        "analysis": response.get("result", ""),
        "response": response,
    }

    return result

# Made with Bob
