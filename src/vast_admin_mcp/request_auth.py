"""Per-request VAST credentials for HTTP deployments behind a gateway.

Opt-in per cluster: a cluster whose config entry has ``"auth_method": "per_request"`` stores no
credentials. Each MCP request must carry the caller's own VMS session (Bearer) token for it in the
``X-VAST-Bearer`` header, so VAST applies that user's own roles and records their name::

    X-VAST-Bearer: cluster1=<access token>, cluster2=<access token>

Keys match a cluster's ``cluster`` address or ``cluster_name`` (case-insensitive). The header is only
honoured for clusters configured as ``per_request``: the address always comes from config.json, so a
caller can't point the server at another host, and clusters with stored credentials behave exactly
as before. A per_request cluster without a token for it raises PerRequestAuthRequired; there is never
a fallback to other credentials.

The token is usually a VMS access token from ``POST /api/token/`` (username/password, AD users too),
obtained by the gateway in front of this server; the gateway refreshes it with ``/api/token/refresh/``.
"""

import hashlib
import logging
from typing import Any, Dict, Optional

PER_REQUEST_AUTH_METHOD = "per_request"
BEARER_HEADER = "x-vast-bearer"


class PerRequestAuthRequired(ValueError):
    """A per_request cluster was called without the caller's VAST token for it."""


class PerRequestTokenRejected(PerRequestAuthRequired):
    """VMS refused the caller's token (expired, revoked or not a token): not a permission answer."""


def is_token_rejection(status: int, body: bytes) -> bool:
    """VMS answers a bad session token with 401, or 403 "Given token not valid ..." (code token_not_valid)."""
    if status == 401:
        return True
    return status == 403 and (b"token_not_valid" in (body or b"") or b"Given token not valid" in (body or b""))


def uses_per_request_auth(cluster_info: Dict[str, Any]) -> bool:
    return str(cluster_info.get("auth_method", "")).strip().lower() == PER_REQUEST_AUTH_METHOD


def _request_headers() -> Dict[str, str]:
    """Headers of the current MCP HTTP request; empty outside one (stdio, CLI, tests)."""
    try:
        from fastmcp.server.dependencies import get_http_headers
    except Exception:  # fastmcp without the helper: no per-request credentials
        return {}
    try:
        return {k.lower(): v for k, v in (get_http_headers() or {}).items()}
    except Exception:
        return {}


def _strip_bearer(value: str) -> str:
    value = value.strip()
    return value[7:].strip() if value[:7].lower() == "bearer " else value


def parse_bearer_header(value: Optional[str]) -> Dict[str, str]:
    """``a=tok1, b=tok2`` -> {"a": "tok1", "b": "tok2"} (keys lower-cased; malformed parts ignored).

    A leading ``Bearer `` on the value or on one token is ignored: gateways such as LiteLLM (on-behalf-of,
    ``upstream_token_header``) always add it."""
    tokens: Dict[str, str] = {}
    if not value:
        return tokens
    for part in _strip_bearer(value).split(","):
        key, sep, token = part.strip().partition("=")
        key, token = key.strip().lower(), _strip_bearer(token)
        if sep and key and token:
            tokens[key] = token
    return tokens


def bearer_token_for(cluster_address: str, cluster_info: Dict[str, Any]) -> Optional[str]:
    """The caller's token for this cluster from the current request, or None."""
    tokens = parse_bearer_header(_request_headers().get(BEARER_HEADER))
    if not tokens:
        return None
    for key in (cluster_address, cluster_info.get("cluster"), cluster_info.get("cluster_name")):
        if key and str(key).lower() in tokens:
            return tokens[str(key).lower()]
    return None


def require_bearer_token(cluster_address: str, cluster_info: Dict[str, Any]) -> str:
    token = bearer_token_for(cluster_address, cluster_info)
    if not token:
        name = cluster_info.get("cluster_name") or cluster_address
        raise PerRequestAuthRequired(
            f"Cluster {name} uses per-user VAST credentials: this request has no VAST token for it "
            f"(header X-VAST-Bearer: {name}=<VMS access token>)"
        )
    return token


def token_fingerprint(token: str) -> str:
    """Short, non-reversible id for cache keys and logs (never log the token itself)."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def log_per_request_client(cluster_address: str, token: str) -> None:
    logging.debug("Using per-request VAST token %s for %s", token_fingerprint(token), cluster_address)
