# VAST Admin MCP Server

[![PyPI Version](https://img.shields.io/pypi/v/vast-admin-mcp?color=blue&label=PyPI)](https://pypi.org/project/vast-admin-mcp/)
[![Python Version](https://img.shields.io/pypi/pyversions/vast-admin-mcp)](https://pypi.org/project/vast-admin-mcp/)
[![License](https://img.shields.io/badge/license-Apache-green)](https://github.com/vast-data/vast-admin-mcp/blob/main/LICENSE)

A Model Context Protocol (MCP) server for VAST Data administration. Enables AI assistants to interact with VAST clusters for monitoring, management, and data protection operations.

<img src="https://raw.githubusercontent.com/vast-data/vast-admin-mcp/main/static/vast-admin-mcp-fast.gif" alt="VAST Admin MCP Demo" width="70%">

## Features

- 🤖 **Full MCP Integration** - Native support for Claude Desktop, Cursor, VSCode, and other MCP clients
- 🔧 **Cluster Management** - List and monitor VAST clusters, views, tenants, quotas, and more
- 📊 **Performance Metrics** - Real-time performance data and graph generation
- 🔐 **Secure Credentials** - Encrypted password storage using system keyring
- 🛡️ **API Whitelist** - Security through endpoint and method restrictions
- ✏️ **Read-Write Mode** - Optional write access for create operations (views, snapshots, quotas)
- 🎨 **Customizable** - YAML-based templates for adding custom list functions

## Installation

```bash
pip install vast-admin-mcp
```

**Requirements:**
- Python 3.10+
- `jq` command-line tool (for JSON transformations)

### Installing jq

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

## Quick Start

### 1. Configure VAST Cluster Connection

```bash
vast-admin-mcp setup
```

This will prompt you for:
- Cluster address (IP, FQDN, or URL)
- Username and password
- Tenant name (if applicable)

### 2. Configure Your AI Assistant

Get setup instructions for your AI tool:

```bash
# For Cursor
vast-admin-mcp mcpsetup cursor

# For Claude Desktop
vast-admin-mcp mcpsetup claude-desktop

# For VSCode
vast-admin-mcp mcpsetup vscode

# For Windsurf
vast-admin-mcp mcpsetup windsurf
```

**Note:** Add `--read-write` as an argument in the config to enable create operations.

### 3. Start Using with Your AI Assistant

Example prompts:
```
List all VAST clusters
Show me all views on cluster1 with capacity information
Create a bandwidth and IOPS graph for cluster1 over the last hour
List all snapshots for view path /data/app1
Show me any critical alarms that haven't been acknowledged
```

## CLI Usage

Test functions directly from the command line:

```bash
# List available commands
vast-admin-mcp list

# List clusters
vast-admin-mcp clusters

# List views with filters
vast-admin-mcp list views --cluster cluster1 --tenant mytenant

```

## Read-Only vs Read-Write Mode

By default, the MCP server runs in **read-only mode** for safety:
- ✅ All list/query operations
- ✅ Performance monitoring
- ✅ Viewing configurations
- ❌ Create operations disabled

Enable **read-write mode** to allow:
- ✅ Creating views
- ✅ Creating snapshots and clones
- ✅ Setting quotas
- ✅ Creating view templates

Add `--read-write` to the MCP server configuration to enable write operations.

## Available MCP Tools

### List Tools
- `list_clusters_vast` - Cluster information and status
- `list_views_vast` - Views with capacity and protocols
- `list_tenants_vast` - Tenant information and capacity
- `list_volumes_vast` - NVMe volumes/namespaces
- `list_quotas_vast` - Quota configurations
- `list_snapshots_vast` - Snapshot information
- `list_alarms_vast` - Active alarms and alerts
- `list_performance_vast` - Performance metrics
- And many more...

### Create Tools (read-write mode only)
- `create_view_vast` - Create new views
- `create_view_from_template_vast` - Create from templates
- `create_snapshot_vast` - Create snapshots
- `create_clone_vast` - Clone from snapshots
- `create_quota_vast` - Set quotas

## Docker Support

Docker images are available for easy deployment:

```bash
# Using helper script
./vast-admin-mcp-docker.sh setup
./vast-admin-mcp-docker.sh list clusters

# Or with docker directly
docker run --rm -it \
  -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
  vast-admin-mcp:latest \
  vast-admin-mcp list clusters
```

See the [full documentation](https://github.com/vast-data/vast-admin-mcp#readme) for complete Docker setup instructions.

## Configuration Files

- **Cluster Config:** `~/.vast-admin-mcp/config.json`
- **Template Customizations:** `~/.vast-admin-mcp/mcp_list_template_modifications.yaml`
- **View Templates:** `~/.vast-admin-mcp/view_templates.json`
- **Logs:** `~/.vast-admin-mcp/vast_admin_mcp.log`

## Documentation

For comprehensive documentation, including:
- Detailed installation instructions
- Template customization guide
- API whitelist configuration
- Advanced Docker usage
- Security best practices
- Full prompt examples

Visit: https://github.com/vast-data/vast-admin-mcp#readme

## Platform Support

- ✅ macOS
- ✅ Linux
- ❌ Windows (not supported)

## Community & Support

VAST Admin MCP Server welcomes questions, feedback, and feature requests. Join the conversation on https://community.vastdata.com/

## License

Apache License 2.0 - See [LICENSE](https://github.com/vast-data/vast-admin-mcp/blob/main/LICENSE) for details.

## Author

Haim Marko <<haim.marko@vastdata.com>>

## Links

- **Documentation:** https://github.com/vast-data/vast-admin-mcp#readme
- **Issues:** https://github.com/vast-data/vast-admin-mcp/issues
- **Source:** https://github.com/vast-data/vast-admin-mcp
- **PyPI:** https://pypi.org/project/vast-admin-mcp/

