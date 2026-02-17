# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] - 2026-02-17

### Added
- Data ingestion tools for managing ingestion jobs:
  - `create_ingestion_job` - Create new data ingestion jobs
  - `get_ingestion_job` - Get details of a specific ingestion job
  - `list_ingestion_jobs` - List all ingestion jobs
  - `delete_ingestion_job` - Delete an ingestion job
- Schema creation tool:
  - `create_schema` - Create new schemas in catalogs

### Changed
- Migrated API endpoints from v2 to v3:
  - Catalog endpoints (list_schemas, list_tables, describe_table)
  - Engine endpoints (list_engines)
  - Platform endpoints (get_instance_details)
- Enhanced error handling in HTTP client with detailed error messages

## [0.1.0] - 2025-12-08

### Added
- Initial release of IBM watsonx.data MCP Server
- FastMCP 2.0 server with stdio transport support
- watsonx.data API client with IBM Cloud IAM authentication and automatic token refresh
- Six read-only tools across 4 domains:
  - **Platform**: `get_instance_details` - Get instance status, version, and configuration
  - **Engine**: `list_engines` - List Presto and Spark engines with parallel API calls
  - **Catalog**: `list_schemas`, `list_tables`, `describe_table` - Explore data catalogs and schemas
  - **Query**: `execute_select` - Execute read-only SELECT queries with safety validation
- OpenTelemetry observability integration (stderr)
  - Distributed tracing for API calls
  - Metrics collection (request counts, latencies, errors)
  - Structured logging with context propagation
- Configuration management with Pydantic Settings
  - Environment variable support
  - Validation and type safety
  - Configurable timeouts and TLS settings
- Enhanced tool schemas following Anthropic best practices
  - Detailed parameter documentation with examples
  - Extensive use case descriptions
  - Multiple example queries per tool
  - Common usage patterns and best practices
  - Performance notes and error handling guidance
- Comprehensive documentation
  - README.md: Overview and quick start guide
  - TOOLS.md: Detailed tool reference with examples
  - TROUBLESHOOTING.md: Common issues and solutions
- Example configurations for Claude Desktop and IBM Bob
- Comprehensive test suite with 91 tests and 89.84% code coverage
- Apache 2.0 License

### Security
- Read-only access enforced (SELECT queries only)
- SQL injection prevention with query validation
- Secure credential management via environment variables
