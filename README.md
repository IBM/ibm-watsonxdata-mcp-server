# IBM watsonx.data MCP Server

## Overview

The IBM watsonx.data MCP Server enables AI assistants to interact seamlessly with IBM watsonx.data lakehouses using natural language. It exposes specialized tools for conversational data operations, including:

- Querying lakehouse data with SQL or natural language prompts
- Exploring data catalogs and metadata
- Inspecting table schemas and structures
- Monitoring of engines

Currently, it supports stdio transport mechanism for local subprocess. For comprehensive details on transport options, including implementation guidelines and security best practices, refer to the [MCP Transports Specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports).

## Supported Features

- Tools: instance info, engine discovery, catalog browsing, table schemas, SQL execution
- Read-only access (SELECT only, safety validation)
- IBM Cloud IAM authentication & token refresh
- Transport: stdio (now), Streamable HTTP (planned)


## Architecture Overview

```mermaid
flowchart LR
    User --> Assistant[AI Assistant]
    Assistant -->|stdio/JSON-RPC| Server[watsonx.data MCP Server]
    Server -->|IAM Auth + API Calls| WX[watsonx.data Service]
    WX --> Engines[Presto & Spark Engines]
    Engines --> Lakehouse[Lakehouse Storage]

    style Server fill:#f3e5f5,stroke:#4a148c
    style WX fill:#e0f2f1,stroke:#00695c
    style Engines fill:#fff3e0,stroke:#ef6c00
```


### Query Execution Flow
```mermaid
sequenceDiagram
    participant A as AI Assistant
    participant S as MCP Server
    participant I as IBM Cloud IAM
    participant W as watsonx.data API
    participant E as Presto/Spark Engines

    A->>S: Natural-language request (MCP)
    S->>I: Request IAM token
    I-->>S: IAM access token
    S->>W: API request (catalog, SQL, schema...)
    W->>E: Query execution / metadata ops
    E-->>W: Results
    W-->>S: Response
    S-->>A: Structured MCP result
```

## Getting Started

### 1. Prerequisites

Before installation, ensure you have:

- **Python 3.11 or higher** ([Download](https://www.python.org/downloads/))
- **uv package manager** ([Install](https://github.com/astral-sh/uv))
- **IBM Cloud account** ([Create Account](https://cloud.ibm.com/docs/account?topic=account-account-getting-started))
- **watsonx.data instance** ([Provision Instance](https://cloud.ibm.com/docs/watsonxdata?topic=watsonxdata-tutorial_prov_lite_1)) and ([Setup](https://cloud.ibm.com/docs/watsonxdata?topic=watsonxdata-tutorial_hp_intro))
- **IBM Cloud API key** ([Create API Key](https://cloud.ibm.com/iam/apikeys))
- Gather Instance details:
  - Base URL (e.g., `https://your-instance.lakehouse.cloud.ibm.com`)
  - Instance CRN (e.g., `crn:v1:bluemix:public:lakehouse:us-south:a/...`)
  - IAM API Key with access to watsonx.data instance, catalog and engines

### 2. Installation

#### Option 1: Using pip / pipx

```bash
pipx install ibm-watsonxdata-mcp-server
```

If pipx is not installed, you can install the MCP server using pip:

```bash
pip install --user ibm-watsonxdata-mcp-server
```

#### Option 2: Development Setup

```bash
# Clone repository
git clone https://github.com/IBM/ibm-watsonxdata-mcp-server.git
cd ibm-watsonxdata-mcp-server

# Install dependencies
uv sync

# Copy example configuration
cp examples/.env.example .env

# Edit with your credentials
export WATSONX_DATA_BASE_URL=https://your-instance.lakehouse.cloud.ibm.com
export WATSONX_DATA_API_KEY=your_ibm_cloud_api_key_here
export WATSONX_DATA_INSTANCE_ID=crn:v1:bluemix:public:lakehouse:us-south:a/...

# Verify installation
uv run ibm-watsonxdata-mcp-server --transport stdio
```

### 3. Configure your AI Assistants

#### Integration with Claude Desktop

Find your Claude Desktop configuration file:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Linux**: `~/.config/Claude/claude_desktop_config.json`

Add this to `claude_desktop_config.json`:

**Option 1: Using pip/pipx install**

First, find the full path to the installed command:
```bash
# macOS/Linux
which ibm-watsonxdata-mcp-server

# Windows (PowerShell)
where.exe ibm-watsonxdata-mcp-server
```

Common installation paths:
- **macOS/Linux with pipx or pip --user**: `~/.local/bin/ibm-watsonxdata-mcp-server`
- **Windows with pipx**: `%USERPROFILE%\.local\bin\ibm-watsonxdata-mcp-server.exe`
- **System-wide install**: `/usr/local/bin/ibm-watsonxdata-mcp-server`

Then use the full path in your config:
```json
{
  "mcpServers": {
    "IBM watsonx.data MCP Server": {
      "command": "/path/from/which/command/ibm-watsonxdata-mcp-server",
      "args": ["--transport", "stdio"],
      "env": {
        "WATSONX_DATA_BASE_URL": "https://your-instance.lakehouse.cloud.ibm.com/lakehouse/api",
        "WATSONX_DATA_API_KEY": "your_api_key_here",
        "WATSONX_DATA_INSTANCE_ID": "crn:v1:bluemix:public:lakehouse:us-south:a/..."
      }
    }
  }
}
```

**Option 2: Using development installation**
```json
{
  "mcpServers": {
    "IBM watsonx.data MCP Server": {
      "command": "/absolute/path/to/uv",
      "args": [
        "--directory",
        "/absolute/path/to/mcp-watsonx-data",
        "run",
        "ibm-watsonxdata-mcp-server"
      ],
      "env": {
        "WATSONX_DATA_BASE_URL": "https://your-instance.lakehouse.cloud.ibm.com/lakehouse/api",
        "WATSONX_DATA_API_KEY": "your_api_key_here",
        "WATSONX_DATA_INSTANCE_ID": "crn:v1:bluemix:public:lakehouse:us-south:a/..."
      }
    }
  }
}
```

#### Integration with IBM Bob

Find your mcp_settings.json configuration file:
`~/Library/Application Support/Bob-IDE/User/globalStorage/ibm.bob-code/settings/mcp_settings.json`

**Option 1: Using pip/pipx install**

First, find the full path:
```bash
which ibm-watsonxdata-mcp-server
```

Then use that path in your config:
```json
{
  "mcpServers": {
    "IBM watsonx.data MCP Server": {
      "command": "/path/from/which/command/ibm-watsonxdata-mcp-server",
      "args": ["--transport", "stdio"],
      "env": {
        "WATSONX_DATA_BASE_URL": "https://your-instance.lakehouse.cloud.ibm.com/lakehouse/api",
        "WATSONX_DATA_API_KEY": "your_api_key_here",
        "WATSONX_DATA_INSTANCE_ID": "crn:v1:bluemix:public:lakehouse:us-south:a/..."
      }
    }
  }
}
```

**Option 2: Using development installation**
```json
{
  "mcpServers": {
    "IBM watsonx.data MCP Server": {
      "command": "/absolute/path/to/uv",
      "args": [
        "--directory",
        "/absolute/path/to/mcp-watsonx-data",
        "run",
        "ibm-watsonxdata-mcp-server"
      ],
      "env": {
        "WATSONX_DATA_BASE_URL": "https://your-instance.lakehouse.cloud.ibm.com/lakehouse/api",
        "WATSONX_DATA_API_KEY": "your_api_key_here",
        "WATSONX_DATA_INSTANCE_ID": "crn:v1:bluemix:public:lakehouse:us-south:a/..."
      }
    }
  }
}
```

## Available Tools
### Quick Reference

| Goal | Tool to Use | Example |
|------|-------------|---------|
| Check connectivity | `get_instance_details` | "What's my instance status?" |
| Find engines | `list_engines` | "List all engines" |
| Discover data | `list_schemas` | "What schemas exist?" |
| Find tables | `list_tables` | "Show tables in sales_db" |
| Understand schema | `describe_table` | "Describe customers table" |
| Query data | `execute_select` | "SELECT * FROM tpch.tiny.customer LIMIT 10" |


See [TOOLS.md](TOOLS.md) for currently supported tools and advanced usage patterns.

### Example Usage

Once configured with Claude Desktop, you can interact with your watsonx.data instance using natural language:

#### 1. Check Instance Status

**You ask:**
```
What watsonx.data instance am I connected to?
```

**Claude responds with:**
```
You're connected to watsonx.data instance:
- Instance ID: crn:v1:bluemix:public:lakehouse:us-south:a/abc123...
- Region: us-south
- Version: 2.0.0
- Status: active
- Serverless Spark: enabled
- Presto: enabled
```

#### 2. Discover Available Data

**You ask:**
```
What schemas are available in tpch catalog?
```

**Claude responds:**
```
Available Schemas:
- `tiny` - TPC-H benchmark data (smallest scale)
- `sf1` through `sf100000` - Various scale factors (1, 10, 100, 300, 1000, 3000, 10000, 30000, 100000)
- `information_schema` - System schema

**Tables in the "tiny" schema:**
1. customer
2. lineitem
3. nation
4. orders
5. part
6. partsupp
7. region
8. supplier

These are the standard TPC-H benchmark tables. Would you like me to:
- Describe the schema of one of these tables?
- Run a sample query on one of the tables?
- Explore tables from a different scale factor (sf1, sf100, etc.)?
```

#### 3. Explore Table Structure

**You ask:**
```
Show me the schema for the customer table in tiny schema
```

**Claude responds:**
```
Here's the schema for the **customer** table in the **tpch.tiny** schema:

Columns (8 total):

1. `c_custkey` - bigint (nullable)
2. `c_name` - varchar(25) (nullable)
3. `c_address` - varchar(40) (nullable)
4. `c_nationkey` - bigint (nullable)
5. `c_phone` - varchar(15) (nullable)
6. `c_acctbal` - double (nullable)
7. `c_mktsegment` - varchar(10) (nullable)
8. `c_comment` - varchar(117) (nullable)

Table Properties:
- Type: TABLE
- Primary Keys: None
- Partitions: None

This is a standard TPC-H customer table with customer ID, name, address, nation reference, phone, account balance, market segment, and comments. All columns are nullable.

Would you like me to run a query on this table or explore other tables?
```

#### 4. Query Data

**You ask:**
```
Show me the top 10 customers by account balance
```

**Claude executes:**
```sql
SELECT c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment FROM customer ORDER BY c_acctbal DESC LIMIT 10
```

**And presents results in a formatted table**
Here are the top 10 customers by account balance:

| Customer ID | Customer Name | Account Balance |
|---|---|---|
| 213 | Customer#000000213 | $9,987.71 |
| 45 | Customer#000000045 | $9,983.38 |
| 1106 | Customer#000001106 | $9,977.62 |
| 200 | Customer#000000200 | $9,967.60 |
| 140 | Customer#000000140 | $9,963.15 |
| 381 | Customer#000000381 | $9,931.71 |
| 43 | Customer#000000043 | $9,904.28 |
| 100 | Customer#000000100 | $9,889.89 |
| 780 | Customer#000000780 | $9,874.12 |
| 518 | Customer#000000518 | $9,871.66 |

The query executed in 42.7 seconds and returned 10 rows. Customer#000000213 has the highest account balance at $9,987.71.

#### 5. Multi-Step Analysis

**You ask:**
```
I need to analyze customer data. What's available and what does it look like?
```

**Claude:**
1. Let me check what's in your watsonx.data instance
2. I'll explore the tpch catalog
3. Here's the customer table schema
4. Runs a query to get customer data
5. Runs few more queries based on the columns in the customer table
6. Looks at the related tables
7. Presents a summary 

See [TOOLS.md](TOOLS.md) for more usage patterns and advanced examples.

## Development

### Setting Up Development Environment

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-org/mcp-watsonx-data.git
   cd mcp-watsonx-data
   ```

2. **Install dependencies (including dev dependencies)**
   ```bash
   uv sync --extra dev
   ```

3. **Set up environment variables**
   ```bash
   cp examples/.env.example .env
   # Edit .env with your credentials
   export WATSONX_DATA_BASE_URL=https://your-instance.lakehouse.cloud.ibm.com
   export WATSONX_DATA_API_KEY=your_ibm_cloud_api_key_here
   export WATSONX_DATA_INSTANCE_ID=crn:v1:bluemix:public:lakehouse:us-south:a/...
   ```

### Running Tests

Run the full test suite with coverage:
```bash
uv run pytest
```

Run tests with verbose output:
```bash
uv run pytest -v
```

Run specific test file:
```bash
uv run pytest tests/test_client.py
```

Run tests with coverage report:
```bash
uv run pytest --cov=lakehouse_mcp --cov-report=html
```

View coverage report:
```bash
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

### Code Quality

Run linting and formatting:
```bash
uv run ruff check .
uv run ruff format .
```

Run type checking:
```bash
uv run mypy src/
```

Run pre-commit hooks:
```bash
uv run pre-commit run --all-files
```

## Troubleshooting
See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | for common issues, diagnostics, and solutions

## Useful Links

- **IBM watsonx.data Docs**: https://cloud.ibm.com/docs/watsonxdata?topic=watsonxdata-getting-started
- **IBM Cloud API Keys**: https://cloud.ibm.com/iam/apikeys
- **MCP Specification**: https://modelcontextprotocol.io/
