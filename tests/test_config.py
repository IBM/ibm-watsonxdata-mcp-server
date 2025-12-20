"""
Tests for configuration management.

This file has been modified with the assistance of IBM Bob AI tool
"""

import pytest
from pydantic import ValidationError

from lakehouse_mcp.config import Config, ServerConfig, WatsonXConfig


class TestWatsonXConfig:
    """Tests for WatsonXConfig."""

    def test_valid_config(self, mock_env_vars):
        """Test creating config with valid environment variables."""
        config = WatsonXConfig()

        assert config.base_url == "https://test.watsonx.com/api"
        assert config.api_key == "test_api_key_12345"
        assert config.instance_id == "crn:v1:bluemix:public:lakehouse:us-south:a/test123:instance456::"
        assert config.timeout_seconds == 60
        assert config.tls_insecure_skip_verify is False

    def test_missing_required_fields(self, monkeypatch, tmp_path):
        """Test that missing required fields raise validation error."""
        # Clear required env vars
        monkeypatch.delenv("WATSONX_DATA_BASE_URL", raising=False)
        monkeypatch.delenv("WATSONX_DATA_API_KEY", raising=False)
        monkeypatch.delenv("WATSONX_DATA_INSTANCE_ID", raising=False)

        # Point to non-existent .env file to prevent loading from project .env
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ValidationError) as exc_info:
            WatsonXConfig()

        # Check that all required fields are mentioned in error
        error_str = str(exc_info.value)
        assert "base_url" in error_str
        assert "api_key" in error_str
        assert "instance_id" in error_str

    def test_default_timeout(self, monkeypatch):
        """Test default timeout value."""
        monkeypatch.setenv("WATSONX_DATA_BASE_URL", "https://test.watsonx.com/api")
        monkeypatch.setenv("WATSONX_DATA_API_KEY", "test_key")
        monkeypatch.setenv("WATSONX_DATA_INSTANCE_ID", "crn:v1:test")
        monkeypatch.delenv("WATSONX_DATA_TIMEOUT_SECONDS", raising=False)

        config = WatsonXConfig()
        assert config.timeout_seconds == 120  # Default value

    def test_timeout_validation(self, mock_env_vars, monkeypatch):
        """Test timeout value validation."""
        # Test timeout too low
        monkeypatch.setenv("WATSONX_DATA_TIMEOUT_SECONDS", "5")
        with pytest.raises(ValidationError):
            WatsonXConfig()

        # Test timeout too high
        monkeypatch.setenv("WATSONX_DATA_TIMEOUT_SECONDS", "500")
        with pytest.raises(ValidationError):
            WatsonXConfig()

        # Test valid timeout
        monkeypatch.setenv("WATSONX_DATA_TIMEOUT_SECONDS", "60")
        config = WatsonXConfig()
        assert config.timeout_seconds == 60

    def test_tls_insecure_skip_verify_default(self, mock_env_vars, monkeypatch):
        """Test default value for TLS skip verify."""
        monkeypatch.delenv("WATSONX_DATA_TLS_INSECURE_SKIP_VERIFY", raising=False)

        config = WatsonXConfig()
        assert config.tls_insecure_skip_verify is False

    def test_tls_insecure_skip_verify_true(self, mock_env_vars, monkeypatch):
        """Test setting TLS skip verify to true."""
        monkeypatch.setenv("WATSONX_DATA_TLS_INSECURE_SKIP_VERIFY", "true")

        config = WatsonXConfig()
        assert config.tls_insecure_skip_verify is True

    def test_case_insensitive_env_vars(self, monkeypatch):
        """Test that environment variables are case-insensitive."""
        monkeypatch.setenv("watsonx_data_base_url", "https://test.watsonx.com/api")
        monkeypatch.setenv("WATSONX_DATA_API_KEY", "test_key")
        monkeypatch.setenv("WatsonX_Data_Instance_Id", "crn:v1:test")

        config = WatsonXConfig()
        assert config.base_url == "https://test.watsonx.com/api"
        assert config.api_key == "test_key"
        assert config.instance_id == "crn:v1:test"


class TestServerConfig:
    """Tests for ServerConfig."""

    def test_default_config(self, monkeypatch, tmp_path):
        """Test server config with default values."""
        # Clear all env vars that affect ServerConfig
        monkeypatch.delenv("MODE", raising=False)
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        monkeypatch.delenv("OTEL_ENABLED", raising=False)
        monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)

        # Point to non-existent .env file to prevent loading from project .env
        monkeypatch.chdir(tmp_path)

        config = ServerConfig()

        assert config.mode == "local"
        assert config.log_level == "info"
        assert config.otel_enabled is False
        assert config.otel_service_name == "ibm-watsonxdata-mcp-server"

    def test_custom_config(self, monkeypatch):
        """Test server config with custom values."""
        monkeypatch.setenv("MODE", "self-hosted")
        monkeypatch.setenv("LOG_LEVEL", "debug")
        monkeypatch.setenv("OTEL_ENABLED", "false")
        monkeypatch.setenv("OTEL_SERVICE_NAME", "custom-mcp-server")

        config = ServerConfig()
        assert config.mode == "self-hosted"
        assert config.log_level == "debug"
        assert config.otel_enabled is False
        assert config.otel_service_name == "custom-mcp-server"

    def test_mode_validation(self, monkeypatch):
        """Test mode value validation."""
        monkeypatch.setenv("MODE", "invalid-mode")

        with pytest.raises(ValidationError):
            ServerConfig()

    def test_valid_modes(self, monkeypatch):
        """Test all valid mode values."""
        valid_modes = ["local", "self-hosted", "ibm-managed"]

        for mode in valid_modes:
            monkeypatch.setenv("MODE", mode)
            config = ServerConfig()
            assert config.mode == mode

    def test_log_level_validation(self, monkeypatch):
        """Test log level validation."""
        monkeypatch.setenv("LOG_LEVEL", "invalid")

        with pytest.raises(ValidationError):
            ServerConfig()

    def test_valid_log_levels(self, monkeypatch):
        """Test all valid log levels."""
        valid_levels = ["debug", "info", "warn", "warning", "error", "critical"]

        for level in valid_levels:
            monkeypatch.setenv("LOG_LEVEL", level)
            config = ServerConfig()
            assert config.log_level == level


class TestConfig:
    """Tests for Config container."""

    def test_config_initialization(self, mock_env_vars):
        """Test config container initialization."""
        config = Config()

        assert isinstance(config.watsonx, WatsonXConfig)
        assert isinstance(config.server, ServerConfig)

    def test_config_repr(self, mock_env_vars):
        """Test config string representation."""
        config = Config()
        repr_str = repr(config)

        assert "Config(" in repr_str
        assert "watsonx_url=https://test.watsonx.com/api" in repr_str
        assert "mode=local" in repr_str or "mode=info" in repr_str
        # API key should not be in repr (security)
        assert "test_api_key" not in repr_str

    def test_config_watsonx_access(self, mock_env_vars):
        """Test accessing WatsonX config from container."""
        config = Config()

        assert config.watsonx.base_url == "https://test.watsonx.com/api"
        assert config.watsonx.api_key == "test_api_key_12345"

    def test_config_server_access(self, mock_env_vars):
        """Test accessing server config from container."""
        config = Config()

        assert config.server.mode == "local"
        assert config.server.log_level == "info"
