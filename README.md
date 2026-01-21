# VAST Admin MCP Server

[![PyPI Version](https://img.shields.io/pypi/v/vast-admin-mcp?color=blue&label=PyPI)](https://pypi.org/project/vast-admin-mcp/)
[![Python Version](https://img.shields.io/pypi/pyversions/vast-admin-mcp)](https://pypi.org/project/vast-admin-mcp/)
[![Docker Image](https://img.shields.io/badge/docker-latest-blue)](https://github.com/vast-data/vast-admin-mcp)
[![License](https://img.shields.io/badge/license-Apache-green)](LICENSE)

VAST Admin MCP Server is a Model Context Protocol (MCP) server for VAST Data administration tasks. It provides AI assistants with tools to interact with VAST clusters for monitoring, listing, and management operations. It is supported both for Cluster and Tenant admins.

<img src="static/vast-admin-mcp-fast.gif" alt="VAST Admin MCP Demo" width="90%">

## Features

- **MCP Integration**: Full MCP server implementation for AI assistant integration
- **Cluster Management**: List and monitor VAST clusters
- **Performance Metrics**: Retrieve performance data for cluster objects and graphs generation
- **Dynamic List Functions**: Automatically generate MCP functions from YAML templates for end user modifications
- **Secure Credentials**: Secure password storage using keyring
- **Read-only and Read-write Modes**: Control access level (read-write mode for create operations)

## Quick Start

### 1. Install 

Install vast-admin-mcp:

```bash
# If installed via pip
pip install vast-admin-mcp

```

### 2. Initial Setup

Configure your VAST cluster connection:

```bash
# If installed via pip
vast-admin-mcp setup


This will prompt you for:
- Cluster address (IP, FQDN, or URL like `https://host:port`)
- Username and password
- Tenant (for tenant admins)
- Tenant (for super admins - which tenant context to use)
```

### 3. Configure MCP Server in you AI assistance 

Use `mcpsetup` to get instructions for common AI assistance tools:

```bash
# create the syntax for popular ai assistances (currently has builtin support for cursor,claude-desktop,windsurf,vscode)
vast-admin-mcp mcpsetup vscode
🔧 Configuring MCP server for: vscode
   Detected command: vast-admin-mcp
   Detected args: ['mcp']

📋 VSCode Configuration Instructions
   Config file location: /Users/user/.vscode/mcp.json

    Create a new file if not exists, or add the VAST Admin MCP entry to the existing 'servers' section:
    {
        "servers": {
            "VAST Admin MCP": {
                "command": "vast-admin-mcp",
                "args": [
                    "mcp"
                ]
            }
        }
    }

📝 Next steps:
   1. Edit or create the config file at the location shown above
   2. Restart VSCode
   3. The MCP server should be available in VSCode's MCP tools
   4. Test by asking VSCode to list VAST clusters
```
** Add the --read-write flag as a 2nd argument to be able to make updates in VAST clusters  


## Prompt Examples
#### For Read-only mode 
```
List all VAST clusters 
List all views on cluster cluster1
Show me all tenants across all clusters
Create bandwidth and iops graph for cluster1 over the last hour
Show me the hardware topology for cluster cluster1 
Are there any issues with my configured data protection relationships ? 
Create a comprehensive report comparing all clusters
Find all users prefixed with "s3" on cluster cluster1 tenant tenant1
Are there any critical alerts on my clusters that were not acknoledged ?
List all snapshots for view path /data/app1 on cluster cluster1 tenant tenant1
Show me all quotas configured for tenant tenant1 on cluster cluster1
Get performance metrics for cnodes on cluster cluster1 over the last 7 day
Show me all view policies on cluster cluster1 that support S3 
First, get all available clusters. Then compare views with path "/" across all clusters, showing capcity information
Show me all tenants on cluster cluster1, for each tenant show me the 5 views with the highest used capacity
Get performance metrics for cluster cluster1, then get metrics for all cnodes, and finally get metrics for top 3 views. Show me a summary of IOPS and bandwidth for each object type
Find all views where logical used capacity is greater than 1TB. For each of these views, get their performance metrics over the last 24 hours and show which views have the highest IOPS
```
#### For Read-write mode 
```
Create a new NFS view on cluster cluster1 with path /data/newview in tenant tenant1
Create a view on cluster cluster1 with path /shared/data in tenant tenant1 that supports both NFS and S3 protocols
Create a snapshot named "backup-2024-01-15" for view path /data/app1 on cluster cluster1, tenant tenant1 and keep it for 24h
Create a clone from snapshot "backup-2024-01-15" of view /data/app1. The clone should be at path /data/app1-clone in tenant tenant1 on cluster cluster1
Set a hard quota of 10TB for view path /data/app1 on cluster cluster1, tenant tenant1
Create 3 new views for vmware based on template. 
Create a indestructible snapshot named resrote-point_<view name> for all vmware views on cluster1 
Refresh a clone from most recent snapshot of view /data/app1 at path /data/app1-clone in tenant tenant1 on cluster cluster1
```

## Installation

### Prerequisites

- **Python 3.10+**
- **jq**: Command-line JSON processor (required for field transformations in YAML templates)

#### Installing jq

**macOS:**
```bash
brew install jq
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install jq
```

**Linux (RHEL/CentOS):**
```bash
sudo yum install jq
```


### Basic Installation

```bash
pip install vast-admin-mcp
```

## Docker Installation

### Building the Docker Image

#### Quick Build (Recommended)

Use the provided build script that automatically handles versioning:

```bash
make build-docker
```

This will:
- Extract version from `src/vast_admin_mcp/__about__.py`
- Tag the image with both version number and `latest`
- Add metadata labels (version, build date, git commit)

#### Manual Build

```bash
# Simple build
docker build -t vast-admin-mcp:latest .

# Build with version tags
VERSION=$(grep -E '^__version__' src/vast_admin_mcp/__about__.py | sed 's/__version__\s*=\s*"\([^"]*\)"/\1/')
docker build \
  --build-arg VERSION="${VERSION}" \
  --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
  --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
  -t vast-admin-mcp:${VERSION} \
  -t vast-admin-mcp:latest \
  .
```

### Running CLI Commands in Docker

#### Option 1: Using the helper script (recommended)

```bash

# Run any CLI command
./vast-admin-mcp-docker.sh clusters
./vast-admin-mcp-docker.sh list views --cluster cluster1
./vast-admin-mcp-docker.sh setup
./vast-admin-mcp-docker.sh mcpsetup cursor
```

#### Option 2: Using docker run directly

```bash
# One-off command execution
docker run --rm -it \
  -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
  vast-admin-mcp:latest \
  vast-admin-mcp list clusters
```

#### Option 3: Using docker-compose

```bash
# Optional: Set version (defaults to 0.1.3)
export VERSION=0.1.8

# Start container in background
docker-compose up -d

# Execute commands in running container
docker-compose exec app vast-admin-mcp list clusters
docker-compose exec app vast-admin-mcp setup

# Stop container
docker-compose down
```

### Volume Mounts

The Docker setup mounts the following directories/files:

1. **Config Directory** (`~/.vast-admin-mcp` → `/root/.vast-admin-mcp`):
   - `config.json` - Cluster configurations
   - `mcp_list_template_modifications.yaml` - User template customizations
   - `vast_admin_mcp.log` - Log file

2. **Default Template File** (built into image):
   - `mcp_list_cmds_template.yaml` is copied into the image during build
   - To override, mount a custom template file:
     ```bash
     docker run --rm -it \
       -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
       -v /path/to/custom-template.yaml:/app/mcp_list_cmds_template.yaml:ro \
       vast-admin-mcp:latest \
       vast-admin-mcp list clusters
     ```

**Volume Mounting Behavior:**

- **`docker run`** (used by helper script by default): Volumes are mounted each time you run a command. No persistent container needed.
- **`docker exec`** (with persistent containers): Volumes must be mounted when the container is created, not during exec. This is a Docker limitation - `docker exec` runs in an existing container and cannot mount new volumes.

**Using the helper script** (`vast-admin-mcp-docker.sh`):
- By default, uses `docker run` which mounts volumes automatically each time
- Only uses `docker exec` if `DOCKER_CONTAINER_NAME` environment variable is set
- No persistent container needed for normal usage

**Using docker-compose:**
- `docker-compose.yml` automatically mounts the config directory when the container is created
- You can then use `docker-compose exec` to run commands in the persistent container

**Creating a persistent container manually:**
If you want to use `docker exec` with a persistent container, ensure volumes are mounted at creation:
  ```bash
  docker run -d --name vast-admin-mcp \
    -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
    vast-admin-mcp:latest tail -f /dev/null
  ```

### Configuring MCP Server for Desktop LLMs (Docker)

When running `mcpsetup` inside Docker, it will automatically detect Docker and generate appropriate `docker run` or `docker exec` commands:

```bash
# Inside Docker container
./vast-admin-mcp-docker.sh mcpsetup claude-desktop

# Or using docker-compose
docker-compose exec app vast-admin-mcp mcpsetup claude-desktop
```

The generated configuration will use Docker commands that the host can execute, with proper volume mounts for configuration files.

**Environment Variables for Docker:**
- `DOCKER_CONTAINER_NAME`: Use existing container name (for `docker exec`)
- `DOCKER_IMAGE`: Docker image name (for `docker run`)
- `DOCKER_CONFIG_MOUNT`: Container path for config mount (default: `/root/.vast-admin-mcp`)
- `DOCKER_TEMPLATE_FILE`: Host path to custom template file (optional, for override)

**Example:**
```bash
# Set container name for docker exec mode
export DOCKER_CONTAINER_NAME=vast-admin-mcp
./vast-admin-mcp-docker.sh mcpsetup cursor

# Or set image for docker run mode
export DOCKER_IMAGE=vast-admin-mcp:latest
./vast-admin-mcp-docker.sh mcpsetup claude-desktop

# With custom template file
export DOCKER_TEMPLATE_FILE=/path/to/custom-template.yaml
./vast-admin-mcp-docker.sh list clusters
```

## CLI

You can test functions:

### List Available Commands

```bash
vast-admin-mcp list
# Or
./vast-admin-mcp.sh list
```

### Execute a Dynamic Command

```bash
# List views
vast-admin-mcp list views --cluster vast3115-var

# List tenants with JSON output
vast-admin-mcp list tenants --format json

# List views with filters
vast-admin-mcp list views --cluster cluster1 --tenant mytenant

# Save output to file
vast-admin-mcp list views --cluster cluster1 --output views.csv --format csv
```

### Static Commands

```bash
# List clusters
vast-admin-mcp clusters

# List performance metrics
vast-admin-mcp performance --object-name tenant --cluster vast3115-var

# Query users
vast-admin-mcp query-users --cluster vast3115-var --prefix user
```

### Create Commands

```bash
# Create a view
vast-admin-mcp create view --cluster cluster1 --path /myview --protocols NFS

# Create a view from template
vast-admin-mcp create view-from-template --cluster cluster1 --template-name mytemplate

# Create a snapshot
vast-admin-mcp create snapshot --cluster cluster1 --path /myview --name mysnapshot

# Create a clone
vast-admin-mcp create clone --cluster cluster1 --source-path /myview --source-snapshot mysnapshot --destination-path /myclone

# Create or update quota
vast-admin-mcp create quota --cluster cluster1 --path /myview --hard-limit 10GB
```

### Output Formats

- `table` (default): Human-readable table format
- `json`: JSON output
- `csv`: CSV format

## MCP Tools

### Static List Tools

- **list_clusters_vast**: Retrieve information about VAST clusters, their status, capacity and usage
- **list_performance_vast**: Retrieve performance metrics for VAST cluster objects
- **query_users_vast**: Query user names from VAST cluster

### Dynamic List Tools

Additional list tools are automatically registered from the YAML template file located at `~/.vast-admin-mcp/mcp_list_cmds_template.yaml`. These tools follow the naming pattern `list_{command_name}_vast`.

**Note**: Commands with `create_mcp_tool: false` in the YAML template will not be registered as standalone MCP tools. They can still be used in merged commands and via CLI, but won't appear in the MCP tool list.

### Create Tools

The following create tools are available when the MCP server is started with `--read-write`:

- **create_view_vast**: Create a new VAST view
- **create_view_from_template_vast**: Create views from a predefined template
- **create_snapshot_vast**: Create a snapshot for a VAST view
- **create_clone_vast**: Create a clone from a snapshot
- **create_quota_vast**: Create or update quota for a specific path and tenant

**Note**: Create tools are always registered (visible to LLMs) but will raise an error if called when the server is not in read-write mode.

## Configuration

- **Config File**: `~/.vast-admin-mcp/config.json` (cluster configurations, no env var override)
- **Default Template File**: `mcp_list_cmds_template.yaml` in project root (shipped template)
- **Template Modifications File**: `~/.vast-admin-mcp/mcp_list_template_modifications.yaml` (user customizations)
- **View Templates File**: `~/.vast-admin-mcp/view_templates.json` (for view template-based creation). This file can be modified based on the template example `view_templates_example.yaml` in project root (shipped template)
- **Log Files**: `~/.vast-admin-mcp/vast_admin_mcp.log`

### Environment Variables

Template file paths can be overridden using environment variables:
- `VAST_ADMIN_MCP_DEFAULT_TEMPLATE_FILE`: Override default template file path
- `VAST_ADMIN_MCP_TEMPLATE_MODIFICATIONS_FILE`: Override template modifications file path
- `VAST_ADMIN_MCP_VIEW_TEMPLATE_FILE`: Override view templates file path

Example:
```bash
export VAST_ADMIN_MCP_DEFAULT_TEMPLATE_FILE=/custom/path/default_template.yaml
export VAST_ADMIN_MCP_TEMPLATE_MODIFICATIONS_FILE=/custom/path/modifications.yaml
export VAST_ADMIN_MCP_VIEW_TEMPLATE_FILE=/custom/path/view_templates.json
vast-admin-mcp list views
```

## API Whitelist

The API whitelist provides security by restricting which VAST API endpoints and HTTP methods can be accessed. It is configured in the YAML template file's `api_whitelist` section.

### Default Behavior

- **Simple format** (`- views`): Defaults to **GET only**
- **With methods** (`- views: [post]`): Allows **GET + specified methods**
  - Example: `- views: [post]` enables both GET and POST for the views endpoint
  - Example: `- quotas: [post, patch]` enables GET, POST, and PATCH for the quotas endpoint

### Configuration

The whitelist is defined in the YAML template file:

```yaml
api_whitelist:
  # Simple format - GET only
  - clusters
  - tenants
  
  # With methods - GET + specified methods
  - views: [post]  # GET + POST for create operations
  - snapshots: [post]  # GET + POST for create operations
  - quotas: [post, patch]  # GET + POST + PATCH for create/update operations
```

### Security Model

- **Restrictive by default**: If an endpoint is not in the whitelist, it is denied
- **Method validation**: Only specified HTTP methods are allowed
- **Sub-endpoint support**: If a parent endpoint is whitelisted (e.g., `monitors`), all sub-endpoints are allowed (e.g., `monitors.ad_hoc_query`)

### Why This Matters

All API calls are validated against the whitelist. This ensures:
- Only approved endpoints can be accessed
- Only approved HTTP methods can be used
- Create operations require explicit whitelist configuration (e.g., `- views: [post]`)

## YAML Template Structure

The YAML template file defines dynamic list functions. See [TEMPLATE_STRUCTURE.md](TEMPLATE_STRUCTURE.md) for complete documentation.

Each command in the YAML file defines:

- **api_endpoints**: Which VAST API endpoints to call
- **per_row_endpoints** (optional): Endpoints called for each row in the base dataset, with query parameters derived from row data using `$field_name` syntax
- **fields**: Output fields with transformations (jq, unit conversion, summaries)
- **arguments**: MCP tool parameters with validation
- **description**: Tool description for MCP context

See [TEMPLATE_STRUCTURE.md](TEMPLATE_STRUCTURE.md) for detailed examples and best practices.

## Architecture

The server uses:
- **fastmcp**: MCP server framework
- **vastpy**: VAST API client
- **template_parser**: YAML template parsing
- **command_executor**: Dynamic command execution
- **jq**: System command-line tool for JSON transformations (required for jq expressions in YAML templates)

## Create Functions

The server includes create functions for creating VAST objects. These functions are available when the MCP server is started with the `--read-write` flag:

- **create_view_vast**: Create a new VAST view
- **create_view_from_template_vast**: Create views from a predefined template
- **create_snapshot_vast**: Create a snapshot for a VAST view
- **create_clone_vast**: Create a clone from a snapshot
- **create_quota_vast**: Create or update quota for a specific path and tenant

**Important**: Create functions require the MCP server to be started with `--read-write` flag. If called in read-only mode, the LLM user will be notified that read-write mode is required.

**Security**: All create functions use API whitelisting to ensure only allowed endpoints and HTTP methods can be accessed. See [API Whitelist](#api-whitelist) section for details.

## Community & Support

VAST Admin MCP Server welcomes questions, feedback, and feature requests. Join the conversation on https://community.vastdata.com/

## License

Apache License 2.0

See [LICENSE](LICENSE) file for details.

## Author

Haim Marko <haim.marko@vastdata.com>

