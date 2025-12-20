"""
List watsonx.data engines tool.

This tool retrieves information about Presto and Spark engines.

This file has been modified with the assistance of IBM Bob AI tool
"""

import asyncio
from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def list_engines(ctx: Context, engine_type: str | None = None) -> dict[str, Any]:
    """List available Presto and Spark compute engines in watsonx.data.

    Args:
        engine_type: Optional filter - "presto", "spark", or None for all engines

    Returns:
        Dict with:
        - engines: List of engine objects with engine_id, display_name, type, status, size,
          created_on, created_by, associated_catalogs
        - summary: Counts by type and status (total_count, presto_count, spark_count, by_status)
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Validate engine_type
    if engine_type and engine_type not in ("presto", "spark"):
        raise ValueError(f"Invalid engine_type: {engine_type}. Must be 'presto', 'spark', or None")

    logger.info("listing_engines", engine_type=engine_type)

    # Determine which API calls to make
    should_fetch_presto = engine_type is None or engine_type == "presto"
    should_fetch_spark = engine_type is None or engine_type == "spark"

    # Make parallel API calls
    tasks = []
    if should_fetch_presto:
        tasks.append(watsonx_client.get("/v2/presto_engines"))
    else:
        tasks.append(asyncio.sleep(0, result={"presto_engines": []}))

    if should_fetch_spark:
        tasks.append(watsonx_client.get("/v2/spark_engines"))
    else:
        tasks.append(asyncio.sleep(0, result={"spark_engines": []}))

    # Execute in parallel
    presto_response, spark_response = await asyncio.gather(*tasks)

    # Extract engine lists (handle different response structures)
    # Handle None responses from API
    presto_engines = (presto_response or {}).get("presto_engines", []) or []
    spark_engines = (spark_response or {}).get("spark_engines", []) or []

    # Build enhanced unified engine list
    all_engines = []
    status_counts = {}

    # Add Presto engines with enhanced information
    for engine in presto_engines:
        # Normalize status to lowercase
        status = engine.get("status", "unknown").lower()
        status_counts[status] = status_counts.get(status, 0) + 1

        engine_info = {
            "engine_id": engine.get("engine_id", engine.get("id")),
            "display_name": engine.get("engine_display_name", engine.get("display_name", engine.get("name"))),
            "type": "presto",
            "status": status,
            "size": engine.get("size_config", "unknown"),
            "created_on": engine.get("created_on"),
            "created_by": engine.get("created_by", ""),
            "associated_catalogs": engine.get("associated_catalogs", []),
        }
        all_engines.append(engine_info)

    # Add Spark engines with enhanced information
    for engine in spark_engines:
        # Normalize status to lowercase
        status = engine.get("status", "unknown").lower()
        status_counts[status] = status_counts.get(status, 0) + 1

        engine_info = {
            "engine_id": engine.get("engine_id", engine.get("id")),
            "display_name": engine.get("engine_display_name", engine.get("display_name", engine.get("name"))),
            "type": "spark",
            "status": status,
            "size": engine.get("instance_capacity", engine.get("size_config", "unknown")),
            "created_on": engine.get("created_on"),
            "created_by": engine.get("created_by", ""),
            "associated_catalogs": engine.get("associated_catalogs", []),
        }
        all_engines.append(engine_info)

    # Build result with enhanced summary
    result = {
        "engines": all_engines,
        "summary": {
            "total_count": len(all_engines),
            "presto_count": len(presto_engines),
            "spark_count": len(spark_engines),
            "by_status": status_counts,
        },
    }

    logger.info(
        "engines_listed",
        total_count=len(all_engines),
        presto_count=len(presto_engines),
        spark_count=len(spark_engines),
        by_status=status_counts,
    )

    return result
