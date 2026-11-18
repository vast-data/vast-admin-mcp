# Per-user VAST Credentials (Gateway Mode)

By default every caller uses the credentials stored for a cluster in `config.json`. When the server runs behind a gateway that knows its users (for example a portal signed in against the same Active Directory as the clusters), a cluster can instead use **each caller's own VMS session**. VAST then applies that user's roles and records their name.

## Cluster config

```json
{
  "cluster": "vms1.example.com",
  "cluster_name": "cluster1",
  "auth_method": "per_request",
  "user_type": "SUPER_ADMIN",
  "tenant": ""
}
```

- The cluster entry stores no username, password, or token.
- `cluster_name` is required (the server never logs in to look the name up).

## Request header

Each MCP HTTP request carries the caller's VMS access token:

```
X-VAST-Bearer: cluster1=<access token>, cluster2=<access token>
```

- Keys are a cluster's `cluster_name` or address (case-insensitive).
- The gateway obtains the token with `POST https://<vms>/api/token/` (username/password; AD users work) and refreshes with `/api/token/refresh/`.

## Behavior

- The header only applies to clusters with `auth_method: "per_request"`.
- The cluster address always comes from `config.json` — callers cannot point the server at another host.
- Clusters with stored credentials ignore the header.
- Missing token for a `per_request` cluster → clear error (never falls back to other credentials).
- Refused/expired token → `"VAST refused your token"` (not an empty list).
- Clients are cached per token for up to one hour.
- Graphs from `per_request` clusters get an unguessable file name.

## Security

Only the gateway should reach the MCP server. Protect it with the HTTP bearer token (or OAuth) and do not expose it directly. See [Network deployment](../README.md#network-deployment-http) in the main README.
