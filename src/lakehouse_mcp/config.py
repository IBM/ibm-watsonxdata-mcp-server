"""
Configuration management using Pydantic Settings.

This module provides type-safe configuration loading from environment variables.

This file has been modified with the assistance of IBM Bob AI tool
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WatsonXConfig(BaseSettings):
    """watsonx.data API configuration."""

    model_config = SettingsConfigDict(
        env_prefix="WATSONX_DATA_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    base_url: str = Field(
        ...,
        description="watsonx.data API base URL",
        examples=["https://console-ibm-ussouth.lakehouse.test.saas.ibm.com/lakehouse/api"],
    )
    api_key: str = Field(
        ...,
        description="IBM Cloud IAM API key",
    )
    instance_id: str = Field(
        ...,
        description="watsonx.data instance ID (CRN)",
        examples=["crn:v1:bluemix:public:lakehouse:us-south:a/..."],
    )
    timeout_seconds: int = Field(
        default=120,
        description="HTTP request timeout in seconds",
        ge=10,
        le=300,
    )
    tls_insecure_skip_verify: bool = Field(
        default=False,
        description="Skip TLS certificate verification (dev/test only)",
    )


class ServerConfig(BaseSettings):
    """MCP server configuration."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    mode: str = Field(
        default="local",
        description="Deployment mode (local, self-hosted, ibm-managed)",
        pattern="^(local|self-hosted|ibm-managed)$",
    )
    log_level: str = Field(
        default="info",
        description="Logging level",
        pattern="^(debug|info|warn|warning|error|critical)$",
    )
    otel_enabled: bool = Field(
        default=False,
        description="Enable OpenTelemetry traces and metrics",
    )
    otel_service_name: str = Field(
        default="ibm-watsonxdata-mcp-server",
        description="OpenTelemetry service name",
    )


class Config:
    """Application configuration container."""

    def __init__(self) -> None:
        """Initialize configuration from environment variables."""
        self.watsonx = WatsonXConfig()
        self.server = ServerConfig()

    def __repr__(self) -> str:
        """Return string representation (without sensitive data)."""
        return f"Config(watsonx_url={self.watsonx.base_url}, mode={self.server.mode}, log_level={self.server.log_level})"
