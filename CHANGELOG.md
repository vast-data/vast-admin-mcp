# Changelog

All notable changes to this project will be documented in this file.

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
