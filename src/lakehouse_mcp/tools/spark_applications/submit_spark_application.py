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

    Examples:
        Minimal configuration for IBM Cloud Object Storage using cos:// protocol:
        
        {
          "engine_id": "spark398",
          "application": "cos://bucket.instance/app.py",
          "arguments": ["cos://bucket.instance/data.csv"],
          "conf": {
            "spark.hadoop.fs.cos.instance.endpoint": "s3.direct.us-east.cloud-object-storage.appdomain.cloud",
            "spark.hadoop.fs.cos.instance.access.key": "your-access-key",
            "spark.hadoop.fs.cos.instance.secret.key": "your-secret-key"
          }
        }

        Minimal configuration for IBM Cloud Object Storage using s3a:// protocol:
        
        {
          "engine_id": "spark398",
          "application": "s3a://bucket/app.py",
          "arguments": ["s3a://bucket/data.csv"],
          "conf": {
            "spark.hadoop.fs.s3a.bucket.bucket.access.key": "your-access-key",
            "spark.hadoop.fs.s3a.bucket.bucket.secret.key": "your-secret-key",
            "spark.hadoop.fs.s3a.bucket.bucket.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
          }
        }

        Optional conf parameters (uses engine defaults if not specified):
        - spark.app.name: Custom application name
        - ae.spark.driver.log.level / ae.spark.executor.log.level: Log levels
        - spark.driver.cores / spark.driver.memory: Driver resources
        - spark.executor.cores / spark.executor.memory: Executor resources
    """
    watsonx_client = ctx.fastmcp.watsonx_client

    # If name is provided, add it to conf as spark.app.name instead of top-level
    if name is not None:
        if conf is None:
            conf = {}
        conf["spark.app.name"] = name

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

    # Check for API errors
    if response.get("error"):
        logger.error("submit_spark_application_failed", error=response.get("error_message"))
        return {
            "error": True,
            "error_message": response.get("error_message", "Unknown error"),
            "status_code": response.get("status_code", 0),
        }

    logger.info(
        "spark_application_submitted",
        engine_id=engine_id,
        application_id=response.get("application_id"),
        state=response.get("state"),
    )

    return response
