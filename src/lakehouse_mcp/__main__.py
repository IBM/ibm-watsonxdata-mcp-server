"""
Entry point for running the MCP server.

Usage:
    python -m lakehouse_mcp [OPTIONS]
    OR
    ibm-watsonxdata-mcp-server [OPTIONS] (if installed)

This file has been modified with the assistance of IBM Bob AI tool
"""

import argparse
import os
import sys
from pathlib import Path

# CRITICAL: Redirect all stdout to stderr before any imports
# This prevents any accidental stdout writes from breaking JSON-RPC protocol
# FastMCP will explicitly use the real stdout for JSON-RPC messages
# _original_stdout = sys.stdout
# sys.stdout = sys.stderr
# Suppress asyncio internal logs (selector messages)
# stdlib_logging.getLogger("asyncio").setLevel(stdlib_logging.WARNING)
from lakehouse_mcp.observability import get_logger, setup_logging
from lakehouse_mcp.server import mcp

# Restore stdout after imports (FastMCP will use this for JSON-RPC)
# sys.stdout = _original_stdout


def discover_available_tools() -> dict[str, list[tuple[str, str]]]:
    """Dynamically discover available tools by scanning the tools directory.

    Scans src/lakehouse_mcp/tools/ directory structure:
    - Each subdirectory is a domain (e.g., platform, engine, catalog, query)
    - Each .py file (except __init__.py) in a domain is a tool

    This approach allows discovering tools before importing them, enabling
    selective loading based on user preferences.

    Returns:
        Dictionary mapping domain names to list of (tool_name, module_path) tuples

    Example:
        {
            "platform": [("get_instance_details", "lakehouse_mcp.tools.platform.get_instance_details")],
            "engine": [("list_engines", "lakehouse_mcp.tools.engine.list_engines")],
            ...
        }
    """
    tools_dir = Path(__file__).parent / "tools"
    available_tools = {}

    # Iterate through domain directories
    for domain_dir in sorted(tools_dir.iterdir()):
        if not domain_dir.is_dir() or domain_dir.name.startswith("_"):
            continue

        domain_name = domain_dir.name
        domain_tools = []

        # Iterate through tool files in the domain
        for tool_file in sorted(domain_dir.glob("*.py")):
            if tool_file.name.startswith("_"):
                continue

            # Tool name is the file name without .py extension
            tool_name = tool_file.stem
            # Module path for import
            module_path = f"lakehouse_mcp.tools.{domain_name}.{tool_name}"

            domain_tools.append((tool_name, module_path))

        if domain_tools:
            available_tools[domain_name] = domain_tools

    return available_tools


# Dynamically discover all available tools
AVAILABLE_TOOLS = discover_available_tools()


def list_tools() -> None:
    """Print available tools organized by domain and exit."""
    print("\nAvailable watsonx.data MCP Tools:\n")
    print("=" * 50)

    for domain, tools in AVAILABLE_TOOLS.items():
        print(f"\n{domain.upper()} Tools ({len(tools)} tool{'s' if len(tools) > 1 else ''}):")
        for tool_name, _ in tools:
            print(f"  - {tool_name}")

    print(f"\n{'=' * 50}")
    print(f"Total: {sum(len(tools) for tools in AVAILABLE_TOOLS.values())} tools across {len(AVAILABLE_TOOLS)} domains")
    print("\nUsage:")
    print("  Enable all tools:       ibm-watsonxdata-mcp-server")
    print("  Enable specific domains: ibm-watsonxdata-mcp-server --tools platform,engine")
    print()


def load_tools(domains: list[str] | None = None) -> None:
    """Load and register tools based on specified domains.

    Args:
        domains: List of domain names to load. If None, load all domains.
    """
    from lakehouse_mcp.observability import get_logger

    logger = get_logger(__name__)

    if domains is None:
        # Load all tools
        domains = list(AVAILABLE_TOOLS.keys())

    # Validate domains
    invalid_domains = set(domains) - set(AVAILABLE_TOOLS.keys())
    if invalid_domains:
        logger.error(
            "invalid_domains",
            invalid=list(invalid_domains),
            valid=list(AVAILABLE_TOOLS.keys()),
        )
        print(f"\nError: Invalid domain(s): {', '.join(invalid_domains)}")
        print(f"Valid domains: {', '.join(AVAILABLE_TOOLS.keys())}")
        sys.exit(1)

    # Import and register tools
    loaded_tools = []
    for domain in domains:
        for tool_name, module_path in AVAILABLE_TOOLS[domain]:
            try:
                # Dynamic import to trigger @mcp.tool() registration
                __import__(module_path)
                loaded_tools.append(f"{domain}.{tool_name}")
            except Exception as e:
                logger.exception(
                    "tool_load_failed",
                    tool=tool_name,
                    domain=domain,
                    error=str(e),
                )
                print(f"Warning: Failed to load {domain}.{tool_name}: {e}")

    logger.debug("tools_loaded", domains=domains, count=len(loaded_tools), tools=loaded_tools)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="watsonx.data MCP Server - Provides read-only access to IBM watsonx.data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings (stdio, all tools, INFO logging)
  ibm-watsonxdata-mcp-server

  # Enable debug logging
  ibm-watsonxdata-mcp-server --debug

  # Set specific log level
  ibm-watsonxdata-mcp-server --log-level WARNING

  # Enable only specific domains
  ibm-watsonxdata-mcp-server --tools platform,engine

  # Run with HTTP transport (for remote access)
  ibm-watsonxdata-mcp-server --transport streamable-http --port 8080

  # List all available tools
  ibm-watsonxdata-mcp-server --list-tools

Environment Variables:
  WATSONX_DATA_BASE_URL       watsonx.data API base URL (required)
  WATSONX_DATA_API_KEY        IBM Cloud IAM API key (required)
  WATSONX_DATA_INSTANCE_ID    watsonx.data instance CRN (required)
  LOG_LEVEL                   Logging level (default: info)
  OTEL_ENABLED                Enable OpenTelemetry (default: false)

For more information, see: https://github.com/IBM/ibm-watsonxdata-mcp-server
        """,
    )

    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="List all available tool domains and tools, then exit",
    )

    parser.add_argument(
        "--tools",
        type=str,
        metavar="DOMAINS",
        help="Comma-separated list of tool domains to enable (e.g., platform,engine,catalog,query). "
        "If not specified, all tools are enabled.",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (overrides LOG_LEVEL environment variable)",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode with DEBUG level logging (shorthand for --log-level DEBUG)",
    )

    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="Transport mode for MCP communication (default: stdio). "
        "Use 'stdio' for local Claude Desktop, 'streamable-http' for remote access.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to listen on when using streamable-http transport (default: 8080)",
    )

    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to when using streamable-http transport (default: 0.0.0.0)",
    )

    return parser.parse_args()


def main() -> None:
    """Run the MCP server with stdio transport."""
    # Parse command line arguments
    args = parse_args()

    # Handle --list-tools (print and exit)
    if args.list_tools:
        list_tools()
        sys.exit(0)

    # Configure logging level
    log_level = "INFO"  # Default
    if args.debug:
        log_level = "DEBUG"
    elif args.log_level:
        log_level = args.log_level
    elif os.getenv("LOG_LEVEL"):
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Set log level in environment for observability module
    os.environ["LOG_LEVEL"] = log_level

    # Reinitialize logging with new level
    setup_logging()
    logger = get_logger(__name__)

    # Parse and load tools
    domains = None
    if args.tools:
        domains = [d.strip() for d in args.tools.split(",")]
        logger.info("selective_tools_enabled", domains=domains)

    load_tools(domains)

    # Log startup
    logger.debug("starting_mcp_server", transport=args.transport, log_level=log_level, domains=domains or "all")

    try:
        # Run with selected transport (suppress banner for clean logs)
        if args.transport == "streamable-http":
            logger.info("http_server_starting", host=args.host, port=args.port)
            mcp.run(transport="streamable-http", host=args.host, port=args.port, show_banner=False)
        else:
            # Run with stdio transport (default, for Claude Desktop)
            mcp.run(transport="stdio", show_banner=False)
    except KeyboardInterrupt:
        logger.info("mcp_server_stopped", reason="keyboard_interrupt")
        sys.exit(0)
    except Exception as e:
        logger.error("mcp_server_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
