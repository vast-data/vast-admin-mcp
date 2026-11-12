# Proxy Configuration

VAST Admin MCP supports HTTP/HTTPS and SOCKS proxies for reaching VAST clusters through corporate networks. Proxies use standard environment variables.

## Environment variables

| Variable | Role |
|----------|------|
| `HTTPS_PROXY` / `https_proxy` | Highest precedence (recommended; VAST API uses HTTPS) |
| `HTTP_PROXY` / `http_proxy` | Fallback |
| `ALL_PROXY` / `all_proxy` | Catch-all; preferred for SOCKS |
| `NO_PROXY` / `no_proxy` | Comma-separated hosts that bypass the proxy (`*` = all) |

```bash
# Skip proxy for internal VAST clusters
export NO_PROXY=vast-cluster1.internal,10.0.0.5
```

## HTTP/HTTPS proxy

```bash
export HTTPS_PROXY=http://proxy.example.com:8080

# With authentication
export HTTPS_PROXY=http://username:password@proxy.example.com:8080

vast-admin-mcp clusters
vast-admin-mcp list views --cluster cluster1
```

## SOCKS proxy

Requires the optional dependency:

```bash
pip install 'vast-admin-mcp[socks]'
# or: pip install pysocks
```

```bash
# SOCKS5 (client-side DNS)
export ALL_PROXY=socks5://proxy.example.com:1080

# SOCKS5h (remote DNS — recommended for internal hostnames)
export ALL_PROXY=socks5h://proxy.example.com:1080

# With authentication
export ALL_PROXY=socks5h://username:password@proxy.example.com:1080

# SOCKS4
export ALL_PROXY=socks4://proxy.example.com:1080
```

## Proxy types

| Type | Description | Env var | Dependency |
|------|-------------|---------|------------|
| HTTP/HTTPS | Standard corporate proxies | `HTTPS_PROXY` / `HTTP_PROXY` | Built-in |
| SOCKS5 | Client-side DNS | `ALL_PROXY` | PySocks |
| SOCKS5h | Remote DNS (recommended for privacy) | `ALL_PROXY` | PySocks |
| SOCKS4 | Legacy SOCKS4 | `ALL_PROXY` | PySocks |
| SOCKS4a | SOCKS4 with remote DNS | `ALL_PROXY` | PySocks |

> Any proxy env var can carry any scheme, but use `ALL_PROXY` for SOCKS to follow common conventions.
