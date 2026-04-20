# Changelog

All notable changes to this project will be documented in this file.

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
