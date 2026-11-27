# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Fixed

- **Expired cache entries** -- `CacheManager` now drops expired entries of a cache on every write. Before, an expired entry was only removed when the same key was read again, so per-request clients (`bearer_client`, keyed by token fingerprint) piled up for every rotated VMS token and were never freed on a long-running HTTP server.

## [0.2.8] - 2026-11-18

### Added

- `QoS Policies List Tool` -- new `list_qospolicies_vast` dynamic list tool (`qospolicies` command in `mcp_list_cmds_template.yaml`) with a GET-only `qospolicies` entry in `api_whitelist`. Lists QoS policies per cluster with name, mode, policy type, static limits, capacity limits, default flag, and the views each policy is attached to. The whitelist entry also lets `create_view_vast` resolve a `qos_policy` name (to be confirmed). Field names and the views join are not yet verified against a live cluster.
- `list_views_vast` protocol filter via server-side `protocols__contains` (never silently-ignored `protocols=`)
- `list_tenants_vast` now exposes `client_ip_ranges` and `client_ip_ranges_summary`
- New create tools (require `--read-write`): `create_tenant_vast`, `create_vippool_vast`, `create_user_vast`, `create_viewpolicy_vast`, `create_s3_key_vast`
- `create_view_vast` extended for `DATABASE`/`KAFKA` (DATABASE requires S3; KAFKA requires `kafka_vip_pools`)
- `create_s3_key_vast` locates users via `query_users`, enforces max 2 local keys, and POSTs `users/{id}/access_keys/`
- New YAML-driven `delete_cmds` section auto-registers `delete_vippools_vast`, `delete_users_vast`, `delete_viewpolicies_vast`, `delete_views_vast`, `delete_s3_keys_vast` (tenant delete intentionally omitted)
- VIP pool create always sends explicit `tenant_id`, including JSON `null` for tenantless/shared pools
- **Per-user VAST credentials** -- opt-in per cluster with `"auth_method": "per_request"`. The cluster stores no credentials; each MCP HTTP request carries the caller's own VMS session token in `X-VAST-Bearer: <cluster>=<token>`, sent to VMS as `Authorization: Bearer`. VAST applies that user's roles and records their name. Clients are cached per token (separate `bearer_client` cache, 1 h), a missing or refused token fails the call with a clear error (never an empty list, never another login), and graphs from these clusters get unguessable file names. Clusters with stored credentials are unchanged and ignore the header. See [docs/per-request-auth.md](docs/per-request-auth.md).

### Changed

- **Read-write tool exposure** -- Create and delete MCP tools are registered only when the server is started with `--read-write`. In readonly mode they are omitted from the tool list (previously always visible, failing at call time).

### Fixed

- **env: password references** -- `retrieve_password_secure` no longer raises `UnboundLocalError` on `os` when resolving `env:<VAR>` credentials. Redundant function-local `import os` in the keyring branch had shadowed the module-level name for the whole function, so Docker/`--env-file` and systemd setups using `"password": "env:…"` failed on every tool call. The same local-import shadowing is removed for `sys` in `handle_performance_graph_command` (missing `--monitor-name` now exits with code 1 instead of crashing) and for unused local `import json` in `start_mcp`.

## [0.2.7] - 2026-11-02

### Fixed

- **View Performance Metrics** -- `list_performance_vast` for views now uses the same `ViewMetrics,*__time_avg` (bandwidth/IOPS) and latency `__avg` series as VMS Views Bandwidth / Views IOPS / Views latency monitors, instead of `__rate`. Bandwidth values labeled `MB/Sec` are treated as bytes/s (no erroneous MiB rescale). Per-column NaN handling prevents unrelated empty metrics (e.g. Kafka) from wiping valid results.

## [0.2.6] - 2026-10-07

### Changed

- **list_clusters Capacity Fields** -- Cluster capacity columns now mirror the VAST GUI dashboard: Usable/Logical Used (excluding pending deletion), Usable/Logical Pending Deletion, Usable/Logical Free, Usable/Logical Capacity totals, and DRR. Replaces the previous Logical/Physical Used/Free fields that mixed pending deletion into "Used" and reported raw free physical instead of free usable.

### Fixed

- **Capacity Unit Conversion** -- Capacity displays (`convert: AUTO` / explicit KB–PB) default to SI decimal (base-1000) so labels match the VAST GUI (1 TB = 10^12 bytes). Previously values were divided by 1024^n but labeled with SI unit names, understating TB-scale capacity by ~9.95% vs the GUI. The same convention applies to capacity filters and create-quota size strings (`100TB`, `>500GB`). Override with `"capacity_unit_base": 1024` in `config.json` for binary (base-2) conversion; AUTO then uses IEC labels (`TiB`, `GiB`, …). Explicit IEC units (`1TiB`) are always binary.

## [0.2.5] - 2026-10-04

### Fixed

- **FastMCP 4 Compatibility** -- Import `ToolResult` from the public `fastmcp.tools` path and constrain the dependency to `fastmcp>=3.2,<5`, so installs work with both FastMCP 3.x and 4.x.

## [0.2.4] - 2026-09-17

### Added

- **Hardware Topology HW Type/Model** -- `list_hardware_topology_vast` (via `cnodes` / `dnodes`) now exposes hardware identity fields from the VAST API:

### Fixed

- **Docker Image CVEs** -- Dockerfile upgrades Debian packages (`gzip`, `libc6`/`libc-bin`, `libpcre2-8-0`, `libsqlite3-0`, `perl-base`), upgrades `pip>=26.2.0` (CVE-2026-13346), and removes pip `bom.cdx.json` / unused setuptools to clear Trivy false positives for msgpack/setuptools. 

## [0.2.3] - 2026-06-12

### Added

- **Cluster API Token Authentication** -- now cluster authentication is support also using API tokens and not only username and password. API tokens are also kept securely using key ring like tokens. 

## [0.2.2] - 2026-06-04

### Added

- **HTTP MCP Graph Links** -- Performance graph PNGs generated by `list_performance_graph_vast` are now served through the HTTP MCP server and returned as HTTP URLs when using HTTP transport.
- **Graph Link Authentication Control** -- HTTP server setup now lets users choose whether generated graph PNG links require authentication, using the `http_server.disable_output_auth` configuration option.
- **Public Graph Base URL** -- Added `VAST_ADMIN_MCP_PUBLIC_BASE_URL` support for generating graph URLs behind a reverse proxy or public endpoint.
- **EVENTS** -- added support for the `list_events_tool` MCP tool to be able to get data about the events

## [0.2.1] - 2026-04-20

### Added

- **MCP over HTTP/HTTPS** -- Run VAST Admin MCP as a standalone HTTP server for remote access from MCP clients (Cursor, Claude Desktop, etc.).
- **Kubernetes/OpenShift Deployment** -- Production-ready manifests for K8s and OpenShift with HTTP and HTTPS options, health probes, RBAC for secrets, and multiple access methods (NodePort, Ingress, OpenShift Route).
  - New `create-tls-secret.sh` helper for generating TLS certificates.
  - Comprehensive deployment guide in `deploy/kubernetes/README.md`.

### Fixed

- **API Pagination Bug** -- Fixed pagination logic bug

## [0.2.0] - 2026-03-09

### Added

- **Create Support Bundle** -- New `create_support_bundle_vast` MCP tool for creating support bundles on VAST clusters (requires `--read-write` mode).
  - Supports all 19 bundle preset types: standard, default, debug, micro, mini, management, performance, traces_and_metrics, nfsv3, nfsv4, smb, s3, estore, raid, hardware, permission_issues, rca, dr, inspect_metadata.
  - Flexible time specification: duration only ("last 5 minutes"), start_time + duration, end_time + duration, or explicit start/end times.
  - Automatic timestamp normalization and converts to the API-required format.
  - Node filtering by name pattern -- `cnode_filter` and `dnode_filter` parameters resolve node IDs automatically via wildcard matching (e.g., `cnode-128*`), with matched node names returned in the output.
  - Support for `send_now` (upload to VAST support), `obfuscated` (encrypt private data), `cnodes_only`, `dnodes_only`, and `luna_args` parameters.
- **List S3 Access Keys** -- New `list_s3_keys_vast` MCP tool for listing S3 access keys from VAST clusters.
  - Filterable by tenant, user/owner, and access key (with wildcard support).
  - Shows tenant, user, access key, enabled status, and creation time.
  - Added `locals3keys` to the API whitelist.

## [0.1.9] - 2026-02-14

### Added

- **Dataflow Analytics** -- New `list_dataflow_vast` and `list_dataflow_diagram_vast` MCP tools for visualizing how hosts communicate with VAST components (views, VIPs, cnodes).
  - `list_dataflow_vast` returns tabular data with bandwidth and IOPS metrics per traffic flow.
  - `list_dataflow_diagram_vast` returns a ready-to-render Mermaid topology diagram (Hosts -> CNodes -> Views) with performance labels.
  - Supports relative timeframes (`5m`, `1h`, `24h`) and absolute ISO 8601 time ranges.
  - Rich client-side wildcard filtering on user, host, tenant, view path, VIP, VIP pool, and cnode columns.
  - Top-N truncation for diagram readability (configurable via `top_n_diagram`, default: 5).
  - Automatic rate normalization for timeframe-based queries (converts accumulated sums to per-second averages).
- **Proxy Support** -- Full HTTP/HTTPS and SOCKS proxy support for reaching VAST clusters through corporate/enterprise networks.
  - Detects proxy from standard environment variables (`HTTPS_PROXY`, `HTTP_PROXY`, `ALL_PROXY`) with proper precedence.
  - Respects `NO_PROXY` / `no_proxy` for bypassing proxy on specific hosts (exact match, domain suffix, wildcard).
  - SOCKS4/SOCKS4a/SOCKS5/SOCKS5h support via optional `PySocks` dependency (`pip install 'vast-admin-mcp[socks]'`).
  - Added `[socks]` optional dependency group in `pyproject.toml`.
  - Added unit tests for proxy detection and pool manager creation (`tests/test_proxy.py`).

### Security
- **Docker pip Upgrade** -- Dockerfile now upgrades pip to >=26.0 to fix CVE-2026-1703 (information disclosure via path traversal).
