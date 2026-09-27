"""
Configuration management using Pydantic Settings.

This module provides type-safe configuration loading from environment variables.

This file has been modified with the assistance of IBM Bob AI tool
"""

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WatsonXConfig(BaseSettings):
    """watsonx.data API configuration for SaaS and on-Prem (CPD/Software)."""

    model_config = SettingsConfigDict(
        env_prefix="WATSONX_DATA_",
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    auth_type: str | None = Field(
        default=None,
        description=(
            "Authentication and environment type: 'saas' (IBM Cloud IAM) or 'cpd' (Cloud Pak for Data / Software). "
            "If omitted, inferred from instance_id."
        ),
    )
    base_url: str = Field(
        ...,
        description="watsonx.data API base URL",
        examples=[
            "https://console-ibm-ussouth.lakehouse.test.saas.ibm.com/lakehouse/api",
            "https://cpd-instance.apps.mycluster.example.com/lakehouse/api",
        ],
    )
    api_key: str = Field(
        ...,
        description="IBM Cloud IAM API key (for SaaS) or platform API key (for CPD)",
    )
    username: str | None = Field(
        default=None,
        description="CPD username (required for CPD auth)",
    )
    instance_id: str = Field(
        ...,
        description="watsonx.data instance ID or CRN (e.g., CRN for SaaS or numerical/string ID for CPD)",
        examples=["crn:v1:bluemix:public:lakehouse:us-south:a/...", "1609968977179454"],
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

    @model_validator(mode="after")
    def validate_auth_credentials(self) -> "WatsonXConfig":
        """Validate and infer auth_type and required credentials."""
        # Infer auth_type if not provided
        if not self.auth_type:
            if self.instance_id.startswith("crn:"):
                self.auth_type = "saas"
            else:
                self.auth_type = "cpd"
        else:
            self.auth_type = self.auth_type.lower()
            if self.auth_type not in ("saas", "cpd"):
                raise ValueError("auth_type must be either 'saas' or 'cpd'")

        if self.auth_type == "cpd" and not self.username:
            raise ValueError("WATSONX_DATA_USERNAME is required for CPD (on-Prem/Software) authentication")

        return self


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
