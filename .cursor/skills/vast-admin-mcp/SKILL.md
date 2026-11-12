---
name: vast-admin-mcp
description: Use VAST Admin MCP tools to inspect, monitor, and manage VAST Data clusters, tenants, views, volumes, quotas, snapshots, alerts, protection, users, performance, dataflow, support bundles, and optional read-write create operations. Use when the user asks about VAST clusters, VMS, storage administration, capacity, performance, views, quotas, snapshots, data protection, support bundles, or configuring/running vast-admin-mcp.
---

# VAST Admin MCP

## Purpose

VAST Admin MCP is an MCP server for VAST Data administration. It exposes cluster inventory, monitoring, performance, dataflow, identity, storage, data protection, and optional create operations to agents.

Default mode is read-only. Create/delete tools are registered only when the MCP server is started with `--read-write`, and are still restricted by the API whitelist.

## First Steps

1. Confirm the MCP server is configured and reachable before promising results.
2. If the user asks for "all clusters" or does not name a cluster, call `list_clusters_vast` first and use the returned cluster names in later calls.
3. Before using an unfamiliar VAST tool, inspect its schema or call `describe_tool_vast` for accepted arguments, defaults, examples, and field names.
4. For list tools, call `list_fields_vast` when you need valid filter, sort, or output field names.
5. Prefer precise filters such as `cluster`, `tenant`, `path`, `name`, `prefix`, or timeframe arguments to avoid large responses.

## Safety Rules

- Treat every create/update action as potentially destructive. Confirm intent, target cluster, tenant, path/name, and retention or quota values before calling create tools unless the user already gave exact values.
- Do not call create tools unless the MCP server is in read-write mode and the user explicitly requested the change.
- If a tool returns an API whitelist or read-write error, explain the missing permission or mode instead of retrying with broader actions.
- Never invent cluster, tenant, policy, view, quota, snapshot, user, or VIP pool names. Discover them with list tools when uncertain.
- For support bundles, confirm timeframe, component scope, bundle name, upload preference, and private-data handling.

## Core Tools

Use these static tools when available:

- `list_clusters_vast`: Discover configured clusters and cluster names.
- `list_performance_vast`: Retrieve performance metrics for cluster, cnode, host, user, vippool, view, and tenant objects.
- `list_monitors_vast`: List predefined monitors for graph generation.
- `list_performance_graph_vast`: Generate a graph image. Display or reference the returned image resource, not just the URI text.
- `list_dataflow_vast`: Return tabular host, VIP, cnode, and view traffic flows.
- `list_dataflow_diagram_vast`: Return a Mermaid dataflow topology diagram.
- `list_view_instances_vast`: Discover views with tenant, path, protocols, and bucket details.
- `list_fields_vast`: Show fields and filter/sort metadata for dynamic commands.
- `describe_tool_vast`: Show schema, examples, defaults, and accepted formats for a tool.
- `query_users_vast`: Query VAST user names by prefix.

Dynamic list tools are generated from the YAML template and use the pattern `list_<command>_vast`. Common tools include:

- `list_views_vast`
- `list_viewpolicies_vast`
- `list_volumes_vast`
- `list_tenants_vast`
- `list_vippools_vast`
- `list_quotas_vast`
- `list_cnodes_vast`
- `list_dnodes_vast`
- `list_events_vast`
- `list_alarms_vast`
- `list_data_protection_vast`
- `list_protection_policies_vast`
- `list_snapshots_vast`
- `list_active_directory_vast`
- `list_local_providers_vast`
- `list_identity_policies_vast`
- `list_s3_keys_vast`
- `list_support_bundles_vast`

Read-write tools, available only when enabled:

- `create_view_vast`: Create a VAST view (NFS/S3/SMB/ENDPOINT/DATABASE/KAFKA; Kafka needs `kafka_vip_pools`).
- `create_view_from_template_vast`: Create views from a predefined template.
- `create_snapshot_vast`: Create a view snapshot.
- `create_clone_vast`: Create a clone from a snapshot.
- `create_quota_vast`: Create or update a quota for a path and tenant.
- `create_support_bundle_vast`: Create a diagnostic support bundle.
- `create_tenant_vast`: Create a tenant with `client_ip_ranges`.
- `create_vippool_vast`: Create a VIP pool; pass empty/`none` tenant for tenantless pools.
- `create_user_vast`: Create a local user.
- `create_viewpolicy_vast`: Create a view policy.
- `create_s3_key_vast`: Create a local S3 access key via `query_users` lookup (max 2 keys/user).
- YAML-driven deletes: `delete_vippools_vast`, `delete_users_vast`, `delete_viewpolicies_vast`, `delete_views_vast`, `delete_s3_keys_vast` (no tenant delete).

For Event Broker setup: create tenant → tenantless vippool → user → view policy → Kafka view (`S3,DATABASE,KAFKA` + `kafka_vip_pools`). Filter Kafka views with `list_views_vast(protocol="KAFKA")`. Use `create_s3_key_vast` / `delete_s3_keys_vast` for local S3 keys.

## Common Workflows

### Inventory

1. Call `list_clusters_vast`.
2. Use specific cluster names in later tools.
3. Use `list_tenants_vast`, `list_views_vast`, `list_volumes_vast`, `list_vippools_vast`, or `list_viewpolicies_vast` based on the object requested.

### Capacity And Quotas

1. Use `list_views_vast` for logical and physical capacity by view.
2. Use `list_quotas_vast` for quota limits and usage.
3. Filter by `cluster`, `tenant`, `path`, or `name` when possible.

### Performance

1. Use `list_performance_vast` for metric summaries.
2. Use `list_monitors_vast` before graph requests if the monitor name is unknown.
3. Use `list_performance_graph_vast` for time-series graph output.
4. Use explicit time ranges such as the last hour, last 24 hours, or last 7 days when the user provides them.

### Dataflow

Use `list_dataflow_vast` for tabular traffic details. Use `list_dataflow_diagram_vast` when the user asks for a topology, diagram, flow map, or visualization.

### Alerts And Events

Use `list_alarms_vast` for active or historical alerts. Use `list_events_vast` for event history. Filter by severity, acknowledgement state, cluster, and time range when available.

### Protection And Snapshots

Use `list_data_protection_vast`, `list_protection_policies_vast`, and `list_snapshots_vast` to inspect replication, protection policies, protected paths, and snapshots. For create requests, verify cluster, tenant, path, snapshot name, and retention before calling `create_snapshot_vast`.

### Identity And S3

Use `list_active_directory_vast`, `list_local_providers_vast`, `list_identity_policies_vast`, `query_users_vast`, and `list_s3_keys_vast` for identity and user-related questions.

## Setup And Configuration

Install and configure locally:

```bash
pip install vast-admin-mcp
vast-admin-mcp setup
vast-admin-mcp mcpsetup cursor
```

Start the stdio MCP server:

```bash
vast-admin-mcp mcp
```

Enable read-write tools only when needed:

```bash
vast-admin-mcp mcp --read-write
```

Run HTTP transport:

```bash
vast-admin-mcp mcp --transport http --host 0.0.0.0 --port 8000
```

Important files:

- `~/.vast-admin-mcp/config.json`: Cluster configuration.
- `~/.vast-admin-mcp/mcp_list_template_modifications.yaml`: User template overrides.
- `~/.vast-admin-mcp/view_templates.json`: View creation templates.
- `~/.vast-admin-mcp/vast_admin_mcp.log`: Logs.

Template environment overrides:

- `VAST_ADMIN_MCP_DEFAULT_TEMPLATE_FILE`
- `VAST_ADMIN_MCP_TEMPLATE_MODIFICATIONS_FILE`
- `VAST_ADMIN_MCP_VIEW_TEMPLATE_FILE`

HTTP authentication uses `VAST_ADMIN_MCP_AUTH_TOKEN` for bearer token mode.

## Response Style

- Summarize the result in plain language first.
- Include the cluster, tenant, path, and time range used for the query.
- Prefer compact tables for object lists when they improve readability.
- For large result sets, show the most relevant rows and explain the filters used.
- When data is missing, say which tool was called and what was not found.
