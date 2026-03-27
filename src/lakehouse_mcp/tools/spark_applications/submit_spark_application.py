"""
Submit Spark application tool.

This tool submits a Spark application for execution on a Spark engine.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def submit_spark_application(
    ctx: Context,
    engine_id: str,
    application: str,
    arguments: list[str] | None = None,
    conf: dict[str, str] | None = None,
    env: dict[str, str] | None = None,
    name: str | None = None,
    job_endpoint: str | None = None,
    service_instance_id: str | None = None,
    type: str | None = None,
    volumes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Submit a Spark application for execution on a Spark engine.

    Args:
        engine_id: Spark engine identifier
        application: Application file path (JAR, Python, R file)
        arguments: Application arguments array
        conf: Spark configuration properties (e.g., {"spark.executor.memory": "2g"})
        env: Environment variables
        name: Application name
        job_endpoint: External job endpoint
        service_instance_id: Service instance ID
        type: Application type - "iae" or "emr"
        volumes: Volume mounts for data access

    Returns:
        Dict with application_id, state, and submission details
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    # Build application_details object
    application_details: dict[str, Any] = {
        "application": application,
    }
    if arguments is not None:
        application_details["arguments"] = arguments
    if conf is not None:
        application_details["conf"] = conf
    if env is not None:
        application_details["env"] = env
    if name is not None:
        application_details["name"] = name

    # Build request body
    body: dict[str, Any] = {
        "application_details": application_details,
    }
    if job_endpoint is not None:
        body["job_endpoint"] = job_endpoint
    if service_instance_id is not None:
        body["service_instance_id"] = service_instance_id
    if type is not None:
        body["type"] = type
    if volumes is not None:
        body["volumes"] = volumes

    logger.info(
        "submitting_spark_application",
        engine_id=engine_id,
        application=application,
        name=name,
    )

    path = f"/v3/spark_engines/{engine_id}/applications"
    response = await watsonx_client.post(path, body)

    logger.info(
        "spark_application_submitted",
        engine_id=engine_id,
        application_id=response.get("application_id"),
        state=response.get("state"),
    )

    return response
