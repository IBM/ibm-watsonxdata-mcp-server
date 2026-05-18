# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4] - 2026-05-18

### Added
- Prestissimo engine management tools:
  - `create_prestissimo_engine` - Create new Prestissimo engines in watsonx.data
  - `update_prestissimo_engine` - Update Prestissimo engine configuration and settings
  - `restart_prestissimo_engine` - Restart Prestissimo engines
  - `pause_prestissimo_engine` - Pause running Prestissimo engines
  - `resume_prestissimo_engine` - Resume paused Prestissimo engines
  - `scale_prestissimo_engine` - Scale Prestissimo engines by adjusting worker node counts
- Streamable HTTP transport support for remote deployments
- Remote MCP server documentation reference in README
- Limit parameter support for `list_spark_applications` tool (1-1000 range)

### Changed
- Updated TOOLS.md with comprehensive Prestissimo engine tool documentation
- Enhanced README with streamable HTTP transport usage examples
- Improved error handling in query tools (`execute_insert`, `execute_update`, `explain_query`, `explain_analyze_query`)
  - Changed from raising exceptions to returning error dictionaries
  - Added proper status code reporting (400 for validation errors)
- Enhanced test coverage for query validation and Spark application listing

### Fixed
- Query validation error handling now returns structured error responses instead of raising exceptions
- Improved error messages for invalid engine types in explain tools
- Fixed multiple statement detection in INSERT and UPDATE queries
- Added validation for limit parameter in `list_spark_applications` (must be between 1 and 1000)

## [0.1.3] - 2026-04-13

### Changed
- Upgraded to FastMCP v3.x (from v2.x)
  - Updated dependency access pattern from `ctx.fastmcp.dependencies["watsonx_client"]` to `ctx.fastmcp.watsonx_client`
  - Updated server initialization to use direct attribute assignment instead of dependencies dict
  - All tools and tests updated for FastMCP v3 compatibility
- Enhanced error handling across all tools
  - HTTP client now returns error dicts instead of raising exceptions
  - Added comprehensive error checking in all tool functions
  - Improved error messages and status code reporting

### Fixed
- Improved error handling in watsonx.data API client
- Enhanced error responses across catalog, engine, query, ingestion, and spark application tools

### Added
- Presto engine management tools:
  - `create_presto_engine` - Create new Presto engines in watsonx.data
  - `scale_presto_engine` - Scale Presto engines by adjusting worker node counts
- Spark engine management tools:
  - `create_spark_engine` - Create new Spark engines in watsonx.data
  - `update_spark_engine` - Update Spark engine configuration and settings
  - `pause_spark_engine` - Pause running Spark engines (SAAS only)
  - `resume_spark_engine` - Resume paused Spark engines (SAAS only)
- Query analysis tools:
  - `explain_query` - Get query execution plan without running the query
  - `explain_analyze_query` - Get detailed query analysis with execution statistics

## [0.1.2] - 2026-03-16

### Added
- Catalog management tools for table and column operations:
  - `rename_table` - Rename tables in watsonx.data schemas
  - `add_columns` - Add one or more columns to existing tables
  - `rename_column` - Rename columns in tables

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
