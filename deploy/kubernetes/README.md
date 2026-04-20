# Kubernetes / OpenShift Deployment Guide

This directory contains Kubernetes manifests and helper scripts for deploying VAST Admin MCP Server on Kubernetes or OpenShift clusters.

## Overview

The VAST Admin MCP Server can be deployed on Kubernetes with the following features:

- **HTTP or HTTPS transport** - Serve MCP over HTTP or HTTPS (TLS)
- **Bearer token authentication** - Secure API access with tokens
- **K8s Secrets integration** - Store credentials securely in Kubernetes Secrets
- **Health checks** - Liveness and readiness probes for reliability
- **Multiple access methods** - ClusterIP, NodePort, Ingress, or OpenShift Route

## Files

| File | Description |
|------|-------------|
| `vast-admin-mcp.yaml` | Main deployment manifest (HTTP/HTTPS) |
| `openshift-route.yaml` | OpenShift Route configuration |
| `create-tls-secret.sh` | Helper script to generate TLS certificates |
| `README.md` | This documentation |

## Quick Start

### 1. Create Namespace

```bash
kubectl create namespace vast-admin-mcp
```

### 2. Configure Credentials

Edit `vast-admin-mcp.yaml` and update:

**Secret section** - Add your VAST cluster passwords:
```yaml
stringData:
  vms1-password: "your-actual-password"
  auth-token: "vamt_your-secure-token"
```

**ConfigMap section** - Configure your VAST clusters:
```yaml
config.json: |
  {
    "clusters": [
      {
        "cluster": "vms1.example.com",
        "username": "admin",
        "password": "k8s:vast-admin-mcp/vast-admin-mcp-secrets/vms1-password",
        ...
      }
    ]
  }
```

### 3. Deploy

```bash
kubectl apply -f vast-admin-mcp.yaml -n vast-admin-mcp
```

### 4. Verify

```bash
# Check pod status
kubectl get pods -n vast-admin-mcp

# Check logs
kubectl logs -n vast-admin-mcp -l app=vast-admin-mcp

# Test health endpoint
kubectl port-forward svc/vast-admin-mcp 8000:8000 -n vast-admin-mcp &
curl http://localhost:8000/health
```

## Deployment Options

### Option A: HTTP with NodePort (Simplest)

Use the default `vast-admin-mcp.yaml` manifest. Access via:
```
http://<node-ip>:30800/mcp/
```

### Option B: HTTPS with NodePort (Recommended)

1. Generate TLS certificate:
   ```bash
   ./create-tls-secret.sh -n vast-admin-mcp
   ```

2. Enable HTTPS in `vast-admin-mcp.yaml`:
   - Uncomment the TLS volume and volumeMount sections
   - Replace the HTTP command with the HTTPS command (search for "HTTPS:" comments)
   - Add `scheme: HTTPS` to the health probes
   - Uncomment the NodePort service if needed

3. Deploy:
   ```bash
   kubectl apply -f vast-admin-mcp.yaml -n vast-admin-mcp
   ```

4. Access via:
   ```
   https://<node-ip>:30800/mcp/
   ```

### Option C: Ingress with TLS Termination

1. Deploy HTTP manifest (Ingress handles TLS)
2. Install an Ingress controller (nginx, traefik)
3. Configure Ingress with your domain and TLS certificate
4. Access via your configured domain

### Option D: OpenShift Route

1. Deploy the manifest (with or without HTTPS enabled)
2. Apply the OpenShift route:
   ```bash
   oc apply -f openshift-route.yaml -n vast-admin-mcp
   ```
3. Access via:
   ```
   https://vast-admin-mcp.apps.<cluster-domain>/mcp/
   ```

## Configuration Reference

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FORCE_ENCRYPTED_STORAGE` | Use encrypted storage for credentials | `true` |

### K8s Secrets Integration

The application can read credentials from Kubernetes Secrets using the pattern:
```
k8s:<namespace>/<secret-name>/<key>
```

Example:
```json
{
  "password": "k8s:vast-admin-mcp/vast-admin-mcp-secrets/vms1-password"
}
```

This requires:
1. ServiceAccount with secret read permissions
2. Role and RoleBinding (included in manifests)

### TLS Certificate Generation

The `create-tls-secret.sh` script generates self-signed certificates:

```bash
# Basic usage
./create-tls-secret.sh

# Custom namespace
./create-tls-secret.sh -n production

# Custom validity (2 years)
./create-tls-secret.sh -d 730

# Add Subject Alternative Names
./create-tls-secret.sh --san "mcp.example.com,10.0.0.1"

# Show all options
./create-tls-secret.sh --help
```

### Health Checks

The server exposes a `/health` endpoint for Kubernetes probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
    scheme: HTTP  # or HTTPS for TLS
  initialDelaySeconds: 10
  periodSeconds: 30

readinessProbe:
  httpGet:
    path: /health
    port: 8000
    scheme: HTTP  # or HTTPS for TLS
  initialDelaySeconds: 5
  periodSeconds: 10
```

## MCP Client Configuration

### Cursor IDE

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "VAST Admin MCP": {
      "url": "https://<your-endpoint>:30800/mcp/",
      "headers": {
        "Authorization": "Bearer vamt_your-token-here"
      }
    }
  }
}
```

### Claude Desktop

Add to Claude's MCP configuration:

```json
{
  "mcpServers": {
    "vast-admin": {
      "url": "https://<your-endpoint>:30800/mcp/",
      "headers": {
        "Authorization": "Bearer vamt_your-token-here"
      }
    }
  }
}
```

## Troubleshooting

### Pod not starting

```bash
# Check pod events
kubectl describe pod -n vast-admin-mcp -l app=vast-admin-mcp

# Check logs
kubectl logs -n vast-admin-mcp -l app=vast-admin-mcp --previous
```

### Cannot read secrets

Ensure the ServiceAccount, Role, and RoleBinding are created:
```bash
kubectl get serviceaccount vast-admin-mcp -n vast-admin-mcp
kubectl get role vast-admin-mcp-secret-reader -n vast-admin-mcp
kubectl get rolebinding vast-admin-mcp-secret-reader -n vast-admin-mcp
```

### TLS certificate issues

Regenerate the certificate:
```bash
./create-tls-secret.sh -n vast-admin-mcp
kubectl rollout restart deployment/vast-admin-mcp -n vast-admin-mcp
```

### Cannot connect to VAST cluster

1. Verify network connectivity from pod:
   ```bash
   kubectl exec -n vast-admin-mcp -it <pod-name> -- python3 -c "
   import socket
   print(socket.gethostbyname('your-cluster.example.com'))
   "
   ```

2. Check if cluster is reachable:
   ```bash
   kubectl exec -n vast-admin-mcp -it <pod-name> -- curl -k https://your-cluster.example.com/api/latest/
   ```

3. For cross-network access, you may need to configure network policies or use `hostNetwork: true`.

## Upgrading

To upgrade to a new version:

```bash
# Update the image tag in the manifest
# Then apply and restart
kubectl apply -f vast-admin-mcp.yaml -n vast-admin-mcp
kubectl rollout restart deployment/vast-admin-mcp -n vast-admin-mcp

# Watch the rollout
kubectl rollout status deployment/vast-admin-mcp -n vast-admin-mcp
```

## Uninstalling

```bash
kubectl delete -f vast-admin-mcp.yaml -n vast-admin-mcp
kubectl delete namespace vast-admin-mcp
```

## Security Considerations

1. **Use HTTPS in production** - Always enable TLS for production deployments
2. **Rotate tokens regularly** - Update the auth-token periodically
3. **Limit secret access** - The RBAC rules only allow reading specific secrets
4. **Network policies** - Consider adding NetworkPolicy resources to restrict traffic
5. **Pod security** - The default security context can be tightened based on your requirements

## Support

For issues and questions:
- GitHub Issues: https://github.com/vast-data/vast-admin-mcp/issues
- Documentation: https://github.com/vast-data/vast-admin-mcp#readme
