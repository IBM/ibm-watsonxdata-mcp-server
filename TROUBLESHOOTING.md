# Troubleshooting Guide

Comprehensive troubleshooting guide for watsonx.data MCP Server.

## Table of Contents

- [Quick Diagnostics](#quick-diagnostics)
- [Installation Issues](#installation-issues)
- [Server Startup Issues](#server-startup-issues)
- [Claude Desktop Connection](#claude-desktop-connection)
- [API Authentication Errors](#api-authentication-errors)
- [Query Issues](#query-issues)
- [Performance Issues](#performance-issues)
- [Getting Additional Help](#getting-additional-help)

---

## Quick Diagnostics

Run these commands first to identify the problem category:

```bash
# 1. Verify Python installation
python3.11 --version
# Expected: Python 3.11.0 or higher

# 2. Verify uv installation
uv --version
# Expected: uv X.Y.Z

# 3. Check if project dependencies are installed
cd /path/to/ibm-watsonxdata-mcp-server
uv run ibm-watsonxdata-mcp-server --help
# Expected: Shows usage information

# 4. Test server starts
uv run ibm-watsonxdata-mcp-server --debug
# Expected: "Server ready (stdio mode)" in logs
# Press Ctrl+C to stop

# 5. Check environment variables
env | grep WATSONX
# Expected: WATSONX_DATA_BASE_URL, WATSONX_DATA_API_KEY, WATSONX_DATA_INSTANCE_ID

6. # Generate a bearer token
curl -X POST   "https://iam.cloud.ibm.com/identity/token"   --header 'Content-Type: application/x-www-form-urlencoded'   --header 'Accept: application/json'   --data-urlencode 'grant_type=urn:ibm:params:oauth:grant-type:apikey'   --data-urlencode 'apikey='$WATSONX_DATA_API_KEY

7. # Test API connectivity
curl -X 'GET' $WATSONX_DATA_BASE_URL/v2/instance -H 'accept: application/json' -H 'AuthInstanceId: '$WATSONX_DATA_INSTANCE_ID -H 'Authorization: Bearer '$BEARER_TOKEN

```

# Expected: 200 OK with JSON response
```

If any of these fail, jump to the relevant section below.

---

## Installation Issues

### Python 3.11 Not Found

**Symptom:**
```bash
$ python3.11 --version
command not found: python3.11
```

**Root Cause:** Python 3.11+ is not installed

**Solution:**

```bash
# macOS (using Homebrew)
brew install python@3.14

# Verify installation
python3.11 --version
which python3.11

# Ubuntu/Debian
sudo apt update
sudo apt install python3.11 python3.11-venv

# Windows
# Download from https://www.python.org/downloads/
# Ensure "Add Python to PATH" is checked during installation
```

### uv Package Manager Not Found

**Symptom:**
```bash
$ uv --version
command not found: uv
```

**Root Cause:** uv package manager not installed

**Solution:**

```bash
# Install uv (macOS/Linux)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Restart shell or source profile
source ~/.bashrc  # or ~/.zshrc on macOS

# Verify installation
uv --version
which uv

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Dependency Installation Fails

**Symptom:**
```bash
$ uv sync
error: Failed to resolve dependencies
```

**Root Cause:** Network issues, cache corruption, or incompatible Python version

**Solutions:**

```bash
# Clear uv cache
uv cache clean

# Force reinstall
uv sync --force

# Verify Python version compatibility
python3.11 --version  # Must be 3.11+

# Check for network issues
ping pypi.org

# Use verbose mode to see detailed errors
uv sync -v
```

---

## Server Startup Issues

### Module Not Found Error

**Symptom:**
```bash
$ uv run ibm-watsonxdata-mcp-server
ModuleNotFoundError: No module named 'fastmcp'
```

**Root Cause:** Dependencies not installed or virtual environment corrupted

**Solution:**

```bash
# Reinstall all dependencies
uv sync --force

# Verify installation
uv run python -c "import fastmcp; print(fastmcp.__version__)"

# If still failing, check Python version
python3.11 --version  # Must be 3.11+

# Clean and reinstall
rm -rf .venv
uv sync
```

### Configuration File Not Found

**Symptom:**
```bash
$ uv run ibm-watsonxdata-mcp-server
Error: Configuration file .env not found
```

**Root Cause:** Missing `.env` file

**Solution:**

```bash
# Copy example configuration
cp examples/.env.example .env

# Edit with your credentials
nano .env

# Verify file exists
ls -la .env
cat .env  # Check contents (ensure no syntax errors)
```

### Invalid Environment Variables

**Symptom:**
```bash
$ uv run ibm-watsonxdata-mcp-server
Error: WATSONX_DATA_BASE_URL is required
```

**Root Cause:** Missing or malformed environment variables

**Solution:**

```bash
# Check current environment
env | grep WATSONX

# Required format:
WATSONX_DATA_BASE_URL=https://region.lakehouse.cloud.ibm.com  # No trailing slash!
WATSONX_DATA_API_KEY=your_ibm_cloud_api_key  # IBM Cloud API key
WATSONX_DATA_INSTANCE_ID=crn:v1:bluemix:public:lakehouse:...  # Full CRN

# Verify .env file syntax
cat .env

# Common mistakes:
# ❌ WATSONX_DATA_BASE_URL=https://.../ (trailing slash)
# ❌ WATSONX_DATA_INSTANCE_ID=shortened-id (not full CRN)
# ❌ Using quotes incorrectly: VAR="value" should be VAR=value
```

### Import Errors on Startup

**Symptom:**
```bash
$ uv run ibm-watsonxdata-mcp-server
ImportError: cannot import name 'x' from 'y'
```

**Root Cause:** Version mismatch or corrupted installation

**Solution:**

```bash
# Check installed versions
uv pip list | grep fastmcp
uv pip list | grep httpx

# Reinstall with locked versions
uv sync --locked

# If persistent, clear cache and reinstall
uv cache clean
rm -rf .venv
uv sync
```

---

## Claude Desktop Connection

### Server Not Appearing in Claude

**Symptom:** The watsonx.data server doesn't show up in Claude Desktop's available tools

**Root Causes:**
1. Invalid JSON syntax in configuration
2. Relative paths instead of absolute paths
3. Server fails to start silently
4. Claude Desktop not fully restarted

**Diagnostics:**

```bash
# 1. Validate JSON syntax
python3 -m json.tool ~/Library/Application\ Support/Claude/claude_desktop_config.json
# Expected: Formatted JSON output
# If error: Fix JSON syntax (look for trailing commas, missing quotes)

# 2. Check absolute path
cat ~/Library/Application\ Support/Claude/claude_desktop_config.json | grep directory
# Verify path starts with "/" not "~" or "./"

# 3. Confirm uv is in PATH
which uv

# 4. Test server starts manually
cd /absolute/path/to/ibm-watsonxdata-mcp-server
uv run ibm-watsonxdata-mcp-server --debug
# Should see: "Server ready (stdio mode)"

# 5. Check Claude Desktop logs
tail -f ~/Library/Logs/Claude/mcp*.log
# Look for errors related to "watsonx-data"
```

**Solutions:**

**Fix 1: Validate and Fix JSON**
```bash
# Check for common JSON errors:
# ❌ Trailing commas
# ❌ Missing closing braces
# ❌ Unescaped special characters
# ❌ Single quotes instead of double quotes

# Example valid config:
{
  "mcpServers": {
    "watsonx-data": {
      "command": "/absolute/path/to/uv",
      "args": [
        "--directory",
        "/absolute/path/to/ibm-watsonxdata-mcp-server",
        "run",
        "ibm-watsonxdata-mcp-server"
      ],
      "env": {
        "WATSONX_DATA_BASE_URL": "https://region.lakehouse.cloud.ibm.com",
        "WATSONX_DATA_API_KEY": "your_key",
        "WATSONX_DATA_INSTANCE_ID": "crn:..."
      }
    }
  }
}
```

**Fix 2: Use Absolute Paths**
```bash
# Get absolute path
cd /path/to/ibm-watsonxdata-mcp-server
pwd
# Copy this exact path into config

# Common mistakes:
# ❌ ~/projects/mcp-watsonx-data
# ❌ ./mcp-watsonx-data
# ❌ ../mcp-watsonx-data
# ✅ /Users/yourname/projects/mcp-watsonx-data
```

**Fix 3: Restart Claude Desktop Properly**
```bash
# macOS: Completely quit (not just close window)
# 1. Cmd+Q or right-click dock icon → Quit
# 2. Verify process is gone: ps aux | grep Claude
# 3. Reopen from Applications

# Windows: Exit from system tray
# 1. Right-click system tray icon → Quit
# 2. Check Task Manager to verify it's closed
# 3. Reopen from Start Menu
```

### Authentication Failed Errors

**Symptom:** Server starts but authentication fails

**Diagnostics:**

```bash
# Test API key directly
```bash
# Generate a bearer token
curl -X POST   "https://iam.cloud.ibm.com/identity/token"   --header 'Content-Type: application/x-www-form-urlencoded'   --header 'Accept: application/json'   --data-urlencode 'grant_type=urn:ibm:params:oauth:grant-type:apikey'   --data-urlencode 'apikey='$WATSONX_DATA_API_KEY


# Test the token
curl -X 'GET' $WATSONX_DATA_BASE_URL/v2/instance -H 'accept: application/json' -H 'AuthInstanceId: '$WATSONX_DATA_INSTANCE_ID -H 'Authorization: Bearer '$BEARER_TOKEN

```
# Expected: 200 OK with JSON response
# Actual errors:
# - 401: Invalid API key
# - 403: API key lacks permissions
# - 404: Wrong base URL or instance ID
```

**Solutions by Error Code:**

**401 Unauthorized:**
```
Root Cause: API key is invalid or expired

Solution:
1. Go to https://cloud.ibm.com/iam/apikeys
2. Delete old API key
3. Create new API key
4. Update WATSONX_DATA_API_KEY in config
5. Restart Claude Desktop
```

**403 Forbidden:**
```
Root Cause: API key doesn't have access to watsonx.data instance

Solution:
1. Verify instance ownership in IBM Cloud Console
2. Check IAM permissions for your API key
3. Ensure API key has "Editor" or "Viewer" role for watsonx.data
4. Verify instance ID matches an instance you have access to
```

**404 Not Found:**
```
Root Cause: Incorrect base URL or instance ID

Solution:
1. Verify base URL format (no /api, no /v2, no trailing slash)
   Correct: https://us-south.lakehouse.cloud.ibm.com
   Wrong:   https://us-south.lakehouse.cloud.ibm.com/api/

2. Verify instance ID is full CRN (not shortened ID)
   Correct: crn:v1:bluemix:public:lakehouse:us-south:a/...
   Wrong:   instance-123

3. Check instance is running in IBM Cloud Console
```

### Server Crashes on Startup

**Symptom:** Server starts then immediately crashes

**Diagnostics:**

```bash
# Run with debug logging
uv run ibm-watsonxdata-mcp-server --debug 2>&1 | tee server.log

# Check for errors in log
grep -i error server.log
grep -i exception server.log

# Common errors:
# - Connection refused: Network issue or wrong URL
# - SSL error: Certificate problem
# - Timeout: Instance not responding
```

**Solutions:**

```bash
# Test network connectivity
ping lakehouse.cloud.ibm.com

# Test SSL/TLS
curl -v https://us-south.lakehouse.cloud.ibm.com

# Check firewall/proxy settings
# If behind corporate firewall, may need proxy configuration
export HTTP_PROXY=http://proxy:port
export HTTPS_PROXY=http://proxy:port
```

---

## API Authentication Errors

### 401 Unauthorized

**Symptom:**
```
Error: 401 Unauthorized - Authentication required
```

**Root Cause:** Invalid, expired, or missing API key

**Diagnostics:**

```bash
# Generate a bearer token
curl -X POST   "https://iam.cloud.ibm.com/identity/token"   --header 'Content-Type: application/x-www-form-urlencoded'   --header 'Accept: application/json'   --data-urlencode 'grant_type=urn:ibm:params:oauth:grant-type:apikey'   --data-urlencode 'apikey='$WATSONX_DATA_API_KEY


# Test the token
curl -X 'GET' $WATSONX_DATA_BASE_URL/v2/instance -H 'accept: application/json' -H 'AuthInstanceId: '$WATSONX_DATA_INSTANCE_ID -H 'Authorization: Bearer '$BEARER_TOKEN

```

**Solutions:**

1. **Regenerate API Key:**
   ```bash
   # Go to IBM Cloud Console
   # https://cloud.ibm.com/iam/apikeys
   #
   # 1. Find your API key
   # 2. Delete old key
   # 3. Create new key
   # 4. Update configuration
   ```

2. **Verify API Key Format:**
   ```bash
   # API key should:
   # - Be ~40-50 characters
   # - Contain alphanumeric characters and hyphens
   # - Start with IBM Cloud pattern

   # Check length
   echo -n "$WATSONX_DATA_API_KEY" | wc -c
   ```

3. **Update Configuration:**
   ```bash
   # Update .env file
   nano .env
   # Update WATSONX_DATA_API_KEY=new_key

   # Update Claude Desktop config
   nano ~/Library/Application\ Support/Claude/claude_desktop_config.json
   # Update "WATSONX_DATA_API_KEY" in env section

   # Restart server/Claude Desktop
   ```

### 403 Forbidden

**Symptom:**
```
Error: 403 Forbidden - Access denied
```

**Root Cause:** API key lacks necessary permissions for the instance

**Diagnostics:**

```bash
# Check instance ownership
# Go to: https://cloud.ibm.com/resources
# Filter by: watsonx.data
# Verify: You have access to the instance

# Check IAM permissions
# Go to: https://cloud.ibm.com/iam/users
# Select your user
# Check: Service access for watsonx.data
```

**Solutions:**

1. **Verify Instance Access:**
   - Ensure you own the instance or have been granted access
   - Check instance ID matches an instance you can see in console

2. **Check IAM Roles:**
   - Minimum required: "Viewer" role
   - Recommended: "Editor" role
   - To grant access: IAM → Users → Service Access → Add Policy

3. **Verify Instance ID:**
   ```bash
   # Get correct instance ID from console
   # IBM Cloud Console → Resources → watsonx.data → Instance Details
   # Copy full CRN (starts with crn:v1:bluemix:...)
   ```

### 404 Not Found

**Symptom:**
```
Error: 404 Not Found - Resource not found
```

**Root Cause:** Incorrect base URL or instance ID

**Diagnostics:**

```bash
# Check base URL format
echo $WATSONX_DATA_BASE_URL
# Should be: https://region.lakehouse.cloud.ibm.com
# NO /api, NO /v2, NO trailing slash

# Check instance ID format
echo $WATSONX_DATA_INSTANCE_ID
# Should start with: crn:v1:bluemix:public:lakehouse:

```

**Solutions:**

1. **Fix Base URL:**
   ```bash
   # Correct formats by region:
   # us-south:  https://us-south.lakehouse.cloud.ibm.com
   # us-east:   https://us-east.lakehouse.cloud.ibm.com
   # eu-de:     https://eu-de.lakehouse.cloud.ibm.com
   # eu-gb:     https://eu-gb.lakehouse.cloud.ibm.com

   # Common mistakes:
   # ❌ https://us-south.lakehouse.cloud.ibm.com/
   # ❌ https://us-south.lakehouse.cloud.ibm.com/api
   # ❌ https://lakehouse.cloud.ibm.com (missing region)
   ```

2. **Fix Instance ID:**
   ```bash
   # Get from IBM Cloud Console:
   # Resources → watsonx.data → Click instance → Copy CRN

   # Format: crn:v1:bluemix:public:lakehouse:REGION:a/ACCOUNT:INSTANCE_ID::
   ```

3. **Verify Instance is Running:**
   ```bash
   # Check instance status in IBM Cloud Console
   # Status should be: "Active" or "Running"
   # If stopped: Start the instance first
   ```

### 500 Internal Server Error

**Symptom:**
```
Error: 500 Internal Server Error
```

**Root Cause:** watsonx.data service issue (not your configuration)

**Diagnostics:**

```bash
# Check IBM Cloud status
# Visit: https://cloud.ibm.com/status

# Check instance health in console
# Visit IBM Cloud Console → watsonx.data → Instance

# Check if issue is persistent
# Wait 5 minutes and retry
```

**Solutions:**

1. **Wait and Retry:** Service may be temporarily unavailable
2. **Check IBM Cloud Status Page:** Verify no ongoing incidents
3. **Contact Support:** If persistent, open IBM Cloud support ticket

---

## Query Issues

### Query Timeout Errors

**Symptom:**
```
Error: Query timeout after 300 seconds
```

**Root Cause:** Query scans too much data or lacks proper filtering

**Diagnostics:**

```bash
# Check table size first
"Describe the table_name table"
# Look at row count and size

# Test with small limit
"SELECT * FROM table LIMIT 10"
# If this works, problem is result set size
```

**Solutions:**

1. **Add LIMIT Clause:**
   ```sql
   -- Instead of:
   SELECT * FROM large_table

   -- Use:
   SELECT * FROM large_table LIMIT 100
   ```

2. **Add WHERE Filters:**
   ```sql
   -- Filter on partition columns
   SELECT * FROM orders
   WHERE order_date >= DATE '2024-12-01'
   LIMIT 1000
   ```

3. **Select Specific Columns:**
   ```sql
   -- Instead of SELECT *
   SELECT customer_id, name, email
   FROM customers
   LIMIT 100
   ```

4. **Use Partition Pruning:**
   ```sql
   -- Query partitioned columns in WHERE clause
   SELECT * FROM events
   WHERE event_date = DATE '2024-12-04'  -- Partition column
   LIMIT 100
   ```

### Table Not Found Errors

**Symptom:**
```
Error: Table 'table_name' not found
```

**Root Cause:** Incorrect table name or missing catalog/schema qualifier

**Diagnostics:**

```bash
# Step 1: List all schemas
"List all schemas"

# Step 2: List tables in schema
"List tables in catalog_name.schema_name"

# Step 3: Verify exact table name
# Tables names are case-sensitive!
```

**Solutions:**

1. **Use Fully Qualified Names:**
   ```sql
   -- Always use: catalog.schema.table
   SELECT * FROM iceberg_data.sales_db.customers LIMIT 10

   -- Not just: customers
   ```

2. **Check Name Casing:**
   ```sql
   -- Table names may be case-sensitive
   -- Use exact name from list_tables

   -- If table is "Customers" (capital C):
   SELECT * FROM iceberg_data.sales_db.Customers LIMIT 10
   ```

3. **Discovery Workflow:**
   ```
   1. "List all schemas" → Find your schema
   2. "List tables in schema_name" → Find your table
   3. Use exact names from output
   ```

### Engine Not Running Errors

**Symptom:**
```
Error: Engine 'engine-id' is not running
```

**Root Cause:** Selected engine is stopped or unavailable

**Diagnostics:**

```bash
# Check engine status
"List all engines"
# Look for status: "running" vs "stopped"
```

**Solutions:**

1. **Use Different Engine:**
   ```
   "List engines"
   # Find an engine with status: "running"
   # Use that engine_id in your query
   ```

2. **Wait for Engine to Start:**
   ```
   # If engine is "starting", wait a few minutes
   # Then retry query
   ```

3. **Specify Engine Explicitly:**
   ```sql
   # When using execute_select, provide running engine_id
   # Get engine_id from list_engines first
   ```

### Query Rejected - Not a SELECT Statement

**Symptom:**
```
Error: Only SELECT queries are allowed
```

**Root Cause:** Attempted write operation (safety feature working correctly!)

**This is Expected Behavior:** The server is read-only by design

**Blocked Operations:**
- ❌ INSERT
- ❌ UPDATE
- ❌ DELETE
- ❌ DROP
- ❌ CREATE
- ❌ ALTER
- ❌ TRUNCATE
- ❌ MERGE

**Allowed Operations:**
- ✅ SELECT
- ✅ SELECT with JOINs
- ✅ SELECT with aggregations (COUNT, SUM, AVG, etc.)
- ✅ SELECT with subqueries

**Solution:** Rewrite as SELECT query or use watsonx.data console for write operations

### Column Not Found Errors

**Symptom:**
```
Error: Column 'column_name' not found in table
```

**Root Cause:** Incorrect column name or typo

**Diagnostics:**

```bash
# Check exact column names
"Describe the table_name table"
# Review column list carefully
```

**Solutions:**

1. **Use Exact Column Names:**
   ```sql
   -- Column names may be case-sensitive
   -- Check describe_table output for exact names
   ```

2. **Check for Typos:**
   ```sql
   -- customer_id (correct)
   -- customerid (wrong)
   -- Customer_ID (wrong casing)
   ```

---

## Performance Issues

### Slow Query Responses

**Symptom:** Queries take minutes to complete

**Root Causes:**
1. Large result sets without LIMIT
2. Full table scans (no partition pruning)
3. Wide tables with SELECT *
4. Engine is stopped or starting

**Diagnostics:**

```bash
# Check table size
"Describe table_name"
# Look at row count

# Check if partitioned
# Look for partition info in describe output

# Check engine status
"List engines"
# Verify engine is "running" not "stopped"
```

**Solutions:**

1. **Always Use LIMIT:**
   ```sql
   SELECT * FROM large_table LIMIT 100
   ```

2. **Filter on Partition Columns:**
   ```sql
   -- If table is partitioned by date
   SELECT * FROM events
   WHERE event_date >= DATE '2024-12-01'
   LIMIT 1000
   ```

3. **Select Specific Columns:**
   ```sql
   -- Instead of:
   SELECT * FROM wide_table

   -- Use:
   SELECT id, name, email FROM wide_table
   ```

4. **Add WHERE Clauses:**
   ```sql
   SELECT * FROM orders
   WHERE region = 'US-West'
     AND order_date >= CURRENT_DATE - INTERVAL '7' DAY
   LIMIT 1000
   ```

5. **Check Engine Health:**
   ```
   "List engines"
   # Ensure engine status is "running"
   # Use engine with more nodes for large queries
   ```

### High Memory Usage

**Symptom:** Server or Claude Desktop consuming excessive memory

**Root Cause:** Large query results being held in memory

**Solutions:**

1. **Reduce Result Set Size:**
   ```sql
   -- Add aggressive LIMIT
   SELECT * FROM table LIMIT 10
   ```

2. **Use Smaller Batches:**
   ```sql
   -- Instead of one large query
   -- Break into smaller queries with OFFSET/LIMIT

   SELECT * FROM table ORDER BY id LIMIT 100 OFFSET 0
   SELECT * FROM table ORDER BY id LIMIT 100 OFFSET 100
   ```

3. **Select Fewer Columns:**
   ```sql
   SELECT id, name FROM table  -- Not SELECT *
   ```

### Metadata Operations are Slow

**Symptom:** list_schemas or list_tables takes a long time

**Root Cause:** Large number of schemas/tables or slow metastore

**Solutions:**

1. **Filter by Catalog:**
   ```
   # Instead of listing all schemas
   "List schemas in iceberg_data catalog"
   ```

2. **Specify Engine:**
   ```
   # Use specific engine for metadata queries
   # (provide engine_id parameter)
   ```

3. **Cache Results:**
   ```
   # Ask Claude to remember schema/table lists
   # Avoid re-listing on every query
   ```

---

## Getting Additional Help

### Collect Diagnostic Information

Before requesting help, collect this information:

```bash
# 1. Environment details
python3.11 --version
uv --version
uname -a  # or 'systeminfo' on Windows

# 2. Server version
cd /path/to/ibm-watsonxdata-mcp-server
git log -1 --oneline

# 3. Configuration (REDACT SECRETS!)
env | grep WATSONX | sed 's/=.*/=***REDACTED***/'

# 4. Test server startup
uv run ibm-watsonxdata-mcp-server --debug 2>&1 | tee debug.log
# Press Ctrl+C after a few seconds

# 5. Generate a bearer token
curl -X POST   "https://iam.cloud.ibm.com/identity/token"   --header 'Content-Type: application/x-www-form-urlencoded'   --header 'Accept: application/json'   --data-urlencode 'grant_type=urn:ibm:params:oauth:grant-type:apikey'   --data-urlencode 'apikey='$WATSONX_DATA_API_KEY

# 6. Test API connectivity
curl -X 'GET' $WATSONX_DATA_BASE_URL/v2/instance -H 'accept: application/json' -H 'AuthInstanceId: '$WATSONX_DATA_INSTANCE_ID -H 'Authorization: Bearer '$BEARER_TOKEN

# 7. Claude Desktop logs (if applicable)
# macOS:
tail -100 ~/Library/Logs/Claude/mcp*.log > claude-logs.txt

# Windows:
# Copy from: %APPDATA%\Claude\logs\
```

### Check Documentation

Before opening an issue:

1. **[TOOLS.md](TOOLS.md)** - Tool usage and examples

### Search Existing Issues

Check if your problem is already reported:

- [GitHub Issues](https://github.com/IBM/ibm-watsonxdata-mcp-server/issues)
- Search for error messages and symptoms
- Check both open and closed issues

### Report a New Issue

When opening a GitHub issue, include:

**Required Information:**
1. **Environment:**
   - OS and version (macOS 14.1, Windows 11, Ubuntu 22.04, etc.)
   - Python version
   - uv version

2. **Configuration (redact secrets!):**
   ```bash
   # Example:
   WATSONX_DATA_BASE_URL=https://us-south.lakehouse.cloud.ibm.com
   WATSONX_DATA_API_KEY=***REDACTED***
   WATSONX_DATA_INSTANCE_ID=crn:v1:bluemix:public:...***REDACTED***
   ```

3. **Steps to Reproduce:**
   ```
   1. Run: uv run ibm-watsonxdata-mcp-server
   2. Ask Claude: "List all engines"
   3. Error occurs: ...
   ```

4. **Expected vs Actual Behavior:**
   ```
   Expected: List of engines returned
   Actual: Error 401 Unauthorized
   ```

5. **Logs and Error Messages:**
   ```
   # Include relevant logs with timestamps
   # Redact any sensitive information
   ```

6. **What You've Tried:**
   ```
   - Reinstalled dependencies (uv sync --force)
   - Regenerated API key
   - Verified instance is running
   ```

### Get Community Help

- **GitHub Discussions:** [Discussions](https://github.com/IBM/ibm-watsonxdata-mcp-server/discussions)
- **MCP Community:** [Model Context Protocol Discord](https://discord.gg/modelcontextprotocol)

### Contact IBM Cloud Support

For watsonx.data instance issues (not MCP server issues):

1. **IBM Cloud Console:** https://cloud.ibm.com/unifiedsupport/cases/add
2. **Documentation:** https://cloud.ibm.com/docs/watsonxdata
3. **Status Page:** https://cloud.ibm.com/status

---

## Common Solutions Checklist

Before asking for help, verify you've tried:

- [ ] Reinstalled dependencies: `uv sync --force`
- [ ] Used absolute paths in Claude Desktop config
- [ ] Restarted Claude Desktop completely (Cmd+Q, not just close)
- [ ] Verified API key is current and valid
- [ ] Checked instance is running in IBM Cloud Console
- [ ] Tested API connectivity with curl
- [ ] Validated JSON syntax in config files
- [ ] Reviewed Claude Desktop logs for errors
- [ ] Ensured Python 3.11+ is installed
- [ ] Verified uv is installed and in PATH
- [ ] Checked base URL has no trailing slash
- [ ] Confirmed instance ID is full CRN

---

**Last Updated:** 2024-01-20
**Need more help?** Open an issue on [GitHub](https://github.com/IBM/ibm-watsonxdata-mcp-server/issues)
