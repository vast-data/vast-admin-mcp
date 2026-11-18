# VAST Admin MCP Server

[![PyPI Version](https://img.shields.io/pypi/v/vast-admin-mcp?color=blue&label=PyPI)](https://pypi.org/project/vast-admin-mcp/)
[![Python Version](https://img.shields.io/pypi/pyversions/vast-admin-mcp)](https://pypi.org/project/vast-admin-mcp/)
[![Docker Image](https://img.shields.io/badge/docker-latest-blue)](https://github.com/vast-data/vast-admin-mcp)
[![License](https://img.shields.io/badge/license-Apache-green)](LICENSE)

VAST Admin MCP exposes VAST cluster admin operations to MCP-capable AI tools (Cursor, Claude Desktop, VS Code, Windsurf). It works for both cluster and tenant admins.

<img src="static/vast-admin-mcp-fast.gif" alt="VAST Admin MCP Demo" width="90%">

## Table of contents

- [What it does](#what-it-does)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Prompt examples](#prompt-examples)
- [Installation](#installation)
- [Configuration](#configuration)
- [MCP tools](#mcp-tools)
- [CLI](#cli)
- [Docker](#docker)
- [Network deployment (HTTP)](#network-deployment-http)
- [API whitelist](#api-whitelist)
- [YAML templates](#yaml-templates)
- [Architecture](#architecture)
- [Community & support](#community--support)

## What it does

- List and monitor clusters, views, tenants, quotas, snapshots, alerts, and hardware
- Performance metrics and graphs; dataflow diagrams
- Optional `--read-write` create/delete (views, snapshots, quotas, users, and more)
- Desktop (stdio) or network (HTTP) with bearer token or OAuth/OIDC
- Credentials via keyring, env vars, or Kubernetes secrets
- Customize list tools via YAML templates

## Requirements

- **Python 3.10+**
- **jq** — required for field transformations in YAML templates

```bash
# macOS
brew install jq

# Ubuntu/Debian
sudo apt-get install jq

# RHEL/CentOS
sudo yum install jq
```

## Quick start

### 1. Install

```bash
pip install vast-admin-mcp
```

### 2. Configure cluster connection

```bash
vast-admin-mcp setup
```

Prompts for:

- Cluster address (IP, FQDN, or URL like `https://host:port`)
- Username and password
- Tenant (tenant admins, or which tenant context to use for super admins)

### 3. Configure your AI assistant

```bash
vast-admin-mcp mcpsetup cursor
# also: claude-desktop | windsurf | vscode
```

Example VS Code entry (`~/.vscode/mcp.json`):

```json
{
  "servers": {
    "VAST Admin MCP": {
      "command": "vast-admin-mcp",
      "args": ["mcp"]
    }
  }
}
```

Restart the IDE, then ask it to list VAST clusters.

> **Read-write:** add `"--read-write"` as a second arg (`"args": ["mcp", "--read-write"]`) to enable create/delete tools.

## Prompt examples

### Inventory and capacity

```
List all VAST clusters
Show me all tenants on cluster1; for each, the 5 views with the highest used capacity
Show me all quotas configured for tenant tenant1 on cluster cluster1
Are there any critical alerts on my clusters that were not acknowledged?
```

### Performance and dataflow

```
Create bandwidth and IOPS graph for cluster1 over the last hour
Create dataflow diagram for cluster1 for /path view on tenant3 for the last hour
Show me the hardware topology for cluster cluster1
```

### Read-write (requires `--read-write`)

```
Create a new NFS view on cluster cluster1 with path /data/newview in tenant tenant1
Create a snapshot named "backup-2024-01-15" for view path /data/app1 on cluster cluster1, tenant tenant1 and keep it for 24h
Set a hard quota of 10TB for view path /data/app1 on cluster cluster1, tenant tenant1
```

More examples: [docs/prompt-examples.md](docs/prompt-examples.md).

## Installation

```bash
pip install vast-admin-mcp
```

### Optional dependencies

```bash
# HTTP server (network deployment)
pip install 'vast-admin-mcp[http]'

# Kubernetes secrets
pip install 'vast-admin-mcp[k8s]'

# SOCKS proxy
pip install 'vast-admin-mcp[socks]'

# Everything
pip install 'vast-admin-mcp[all]'
```

## Configuration

| Path | Purpose |
|------|---------|
| `~/.vast-admin-mcp/config.json` | Cluster configurations |
| `mcp_list_cmds_template.yaml` (project root) | Shipped default list-tool template |
| `~/.vast-admin-mcp/mcp_list_template_modifications.yaml` | User template customizations |
| `~/.vast-admin-mcp/view_templates.json` | View template-based creation (see `view_templates_example.json`) |
| `~/.vast-admin-mcp/vast_admin_mcp.log` | Log file |

### Capacity unit base

Defaults to SI decimal (base-1000), matching the VAST GUI (`1 TB = 10^12` bytes). For binary (base-1024):

```json
{
  "clusters": [ ... ],
  "capacity_unit_base": 1024
}
```

Accepted: `1000` / `"decimal"` / `"si"` (default), or `1024` / `"binary"` / `"iec"` / `"base2"`. With base `1024`, AUTO capacity fields use IEC labels (`TiB`, `GiB`, …). Explicit IEC strings (`1TiB`) are always binary.

### Environment variables

**HTTP auth**

```bash
export VAST_ADMIN_MCP_AUTH_TOKEN="vamt_your-secure-token"
```

**Template paths**

- `VAST_ADMIN_MCP_DEFAULT_TEMPLATE_FILE`
- `VAST_ADMIN_MCP_TEMPLATE_MODIFICATIONS_FILE`
- `VAST_ADMIN_MCP_VIEW_TEMPLATE_FILE`

**Proxy** — `HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`, `NO_PROXY`. Full details: [docs/proxy.md](docs/proxy.md).

## MCP tools

### Static list tools

- **list_clusters_vast** — clusters, status, capacity, usage
- **list_performance_vast** — performance metrics for cluster objects
- **query_users_vast** — query user names

### Dynamic list tools

Registered from the YAML template (`list_{command_name}_vast`). Commands with `create_mcp_tool: false` stay available via CLI/merged commands but are not standalone MCP tools.

### Create tools (`--read-write`)

- **create_view_vast** — NFS/S3/SMB/ENDPOINT/DATABASE/KAFKA (Kafka needs `kafka_vip_pools`)
- **create_view_from_template_vast**
- **create_snapshot_vast** / **create_clone_vast**
- **create_quota_vast**
- **create_tenant_vast** — with `client_ip_ranges`
- **create_vippool_vast** — supports explicit tenantless pools
- **create_user_vast** / **create_viewpolicy_vast**
- **create_s3_key_vast** — via `query_users`; max 2 keys per user

### Delete tools (`--read-write`)

YAML-driven from `delete_cmds`:

- **delete_vippools_vast**, **delete_users_vast**, **delete_viewpolicies_vast**, **delete_views_vast**, **delete_s3_keys_vast**

Tenant delete is not exposed. Extend via `delete_cmds` + `api_whitelist` — see [TEMPLATE_STRUCTURE.md](TEMPLATE_STRUCTURE.md).

> Create/delete tools are registered only with `--read-write`. In read-only mode they are not exposed to LLMs. All mutating calls still pass the [API whitelist](#api-whitelist).

## CLI

```bash
# Discover commands
vast-admin-mcp list

# Dynamic list commands
vast-admin-mcp list views --cluster cluster1
vast-admin-mcp list tenants --format json
vast-admin-mcp list views --cluster cluster1 --output views.csv --format csv

# Static commands
vast-admin-mcp clusters
vast-admin-mcp performance --object-name tenant --cluster cluster1
vast-admin-mcp query-users --cluster cluster1 --prefix user

# Create
vast-admin-mcp create view --cluster cluster1 --path /myview --protocols NFS
vast-admin-mcp create view-from-template --cluster cluster1 --template-name mytemplate
vast-admin-mcp create snapshot --cluster cluster1 --path /myview --name mysnapshot
vast-admin-mcp create clone --cluster cluster1 --source-path /myview --source-snapshot mysnapshot --destination-path /myclone
vast-admin-mcp create quota --cluster cluster1 --path /myview --hard-limit 10GB
```

Output formats: `table` (default), `json`, `csv`.

## Docker

```bash
make build-docker

./vast-admin-mcp-docker.sh setup
./vast-admin-mcp-docker.sh list clusters
./vast-admin-mcp-docker.sh mcpsetup cursor
```

Or:

```bash
docker run --rm -it \
  -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
  vast-admin-mcp:latest \
  vast-admin-mcp list clusters
```

Mount `~/.vast-admin-mcp` for config and logs. Full build options, volume behavior, compose, and HTTP mode: [docs/docker.md](docs/docker.md).

## Network deployment (HTTP)

Default transport is stdio (desktop). For remote clients, Kubernetes, or containers, use HTTP:

```bash
vast-admin-mcp mcp --transport http
vast-admin-mcp mcp --transport http --host 0.0.0.0 --port 8000

export VAST_ADMIN_MCP_AUTH_TOKEN="your-secure-token"
vast-admin-mcp mcp --transport http --host 0.0.0.0

vast-admin-mcp mcp --transport http --host 0.0.0.0 \
    --ssl-cert /path/to/cert.pem \
    --ssl-key /path/to/key.pem
```

Endpoints: `http://hostname:8000/mcp/` (MCP), `http://hostname:8000/health` (health).

### Authentication

**Bearer token**

```bash
export VAST_ADMIN_MCP_AUTH_TOKEN="vamt_your-secure-token"
```

Clients send `Authorization: Bearer vamt_your-secure-token`.

**OAuth/OIDC** — via `vast-admin-mcp setup` → option 5 (HTTP settings). Built-in: GitHub, Google, Azure AD. Generic OIDC: Okta, Keycloak, Auth0, and others.

**Interactive setup** — `vast-admin-mcp setup` options 5 (host/port/auth/TLS) and 6 (generate/view/rotate bearer tokens).

### SSL/TLS

```bash
vast-admin-mcp gencert
vast-admin-mcp gencert --cn mcp.example.com --days 365 --san mcp.example.com --san 192.168.1.100

vast-admin-mcp mcp --transport http --host 0.0.0.0 \
    --ssl-cert ~/.vast-admin-mcp/ssl/cert.pem \
    --ssl-key ~/.vast-admin-mcp/ssl/key.pem
```

### Per-user VAST credentials

Opt-in per cluster with `"auth_method": "per_request"`. The cluster stores no credentials; each HTTP request carries the caller's VMS token in `X-VAST-Bearer`. Full behavior and security notes: [docs/per-request-auth.md](docs/per-request-auth.md).

### systemd

```bash
sudo cp deploy/systemd/vast-admin-mcp.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vast-admin-mcp
```

Details: [deploy/systemd/README.md](deploy/systemd/README.md).

### Kubernetes / OpenShift

Manifests in [`deploy/kubernetes/`](deploy/kubernetes/). Guide: [deploy/kubernetes/README.md](deploy/kubernetes/README.md).

```bash
kubectl create namespace vast-admin-mcp
# Edit credentials/config in the manifest, then:
kubectl apply -f deploy/kubernetes/vast-admin-mcp.yaml -n vast-admin-mcp
```

| Access | URL |
|--------|-----|
| NodePort | `http(s)://<node-ip>:30800/mcp/` |
| Ingress | `https://mcp.example.com/mcp/` |
| OpenShift Route | `https://mcp.apps.cluster.com/mcp/` |
| Port-forward | `http://localhost:8000/mcp/` |

K8s secret references use `k8s:<namespace>/<secret-name>/<key>` in `config.json`. OpenShift: apply `openshift-route.yaml` for external access (edge / passthrough / re-encrypt).

## API whitelist

Restricts which VAST API endpoints and HTTP methods the server may call (`api_whitelist` in the YAML template).

- Simple (`- views`) → **GET only**
- With methods (`- views: [post]`) → **GET + listed methods**
- Unknown endpoints are denied; parent whitelist covers sub-endpoints (e.g. `monitors` → `monitors.ad_hoc_query`)

```yaml
api_whitelist:
  - clusters
  - tenants
  - views: [post]
  - snapshots: [post]
  - quotas: [post, patch]
```

Create/delete operations require explicit method entries. See [TEMPLATE_STRUCTURE.md](TEMPLATE_STRUCTURE.md).

## YAML templates

Dynamic list (and delete) tools come from the YAML template. Each command defines `api_endpoints`, optional `per_row_endpoints`, `fields`, `arguments`, and `description`.

Full reference: [TEMPLATE_STRUCTURE.md](TEMPLATE_STRUCTURE.md).

## Architecture

- **fastmcp** — MCP server framework
- **vastpy** — VAST API client
- **template_parser** / **command_executor** — YAML-driven tools
- **jq** — JSON field transformations

## Community & support

Questions and feature requests: [community.vastdata.com](https://community.vastdata.com/)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
