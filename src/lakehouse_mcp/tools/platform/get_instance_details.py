"""
Get watsonx.data instance details tool.

This tool retrieves detailed information about the watsonx.data instance.

This file has been modified with the assistance of IBM Bob AI tool
"""

from typing import Any

from fastmcp import Context

from lakehouse_mcp.observability import get_logger
from lakehouse_mcp.server import mcp

logger = get_logger(__name__)


@mcp.tool()
async def get_instance_details(ctx: Context) -> dict[str, Any]:
    """Get watsonx.data instance information including status, version, region, and enabled features.

    Returns:
        Dict with instance details:
        - instance_id: Instance CRN identifier
        - region: IBM Cloud region (e.g., "us-south")
        - status: Instance status ("active", "provisioning", "inactive", "failed")
        - version: watsonx.data version
        - account_type: Account type ("TRIAL", "ENTERPRISE", "LITE", "STANDARD")
        - serverless_spark_enabled: Whether Spark is available
        - public_endpoints_enabled: Public internet access enabled
        - console_url: Web console URL
    """
    watsonx_client = ctx.fastmcp.dependencies["watsonx_client"]

    logger.info("getting_instance_details")

    # Make API call to get instance details
    response = await watsonx_client.get("/v2/instance")

    # Handle None response
    response = response or {}

    # Log raw response for debugging
    logger.debug("raw_api_response", response_keys=list(response.keys()))

    # Extract deployment information from response
    deployment_response = response.get("deploymentresponse", {}) or {}
    deployment = deployment_response.get("deployment", {}) or {}

    # If deployment is empty, log the actual response structure
    if not deployment:
        logger.warning(
            "unexpected_response_structure",
            has_deployment_response=bool(deployment_response),
            response_keys=list(response.keys()),
        )

    # Build result with instance details
    result = {
        "instance_id": deployment.get("id", "unknown"),
        "region": deployment.get("region", "unknown"),
        "cloud_type": deployment.get("cloud_type", "unknown"),
        "account_type": deployment.get("account_type", "unknown"),
        "plan_id": deployment.get("plan_id", "unknown"),
        "status": deployment.get("status", "unknown"),
        "instance_status": deployment.get("instance_status", "unknown"),
        "version": deployment.get("version", "unknown"),
        "public_endpoints_enabled": deployment.get("enable_public_endpoints", False),
        "private_endpoints_enabled": deployment.get("enable_private_endpoints", False),
        "serverless_spark_enabled": deployment.get("serverless_spark", False),
        "resource_group_crn": deployment.get("resource_group_crn", ""),
        "console_url": deployment.get("console_url", ""),
        "first_time_use": deployment.get("first_time_use", False),
    }

    logger.info(
        "instance_details_retrieved",
        region=result["region"],
        status=result["status"],
        version=result["version"],
    )

    return result
