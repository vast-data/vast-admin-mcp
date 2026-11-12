# Docker Guide

Build, run, and configure VAST Admin MCP with Docker. For a short overview, see the [main README](../README.md#docker).

## Building the image

### Quick build (recommended)

```bash
make build-docker
```

This will:

- Extract version from `src/vast_admin_mcp/__about__.py`
- Tag the image with both the version number and `latest`
- Add metadata labels (version, build date, git commit)

### Manual build

```bash
# Simple build
docker build -t vast-admin-mcp:latest .

# Build with version tags
VERSION=$(grep -E '^__version__' src/vast_admin_mcp/__about__.py | sed 's/__version__\s*=\s*"\([^"]*\)"/\1/')
docker build \
  --build-arg VERSION="${VERSION}" \
  --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
  --build-arg VCS_REF="$(git rev-parse --short HEAD)" \
  -t vast-admin-mcp:${VERSION} \
  -t vast-admin-mcp:latest \
  .
```

## Running CLI commands

### Option 1: Helper script (recommended)

```bash
./vast-admin-mcp-docker.sh clusters
./vast-admin-mcp-docker.sh list views --cluster cluster1
./vast-admin-mcp-docker.sh setup
./vast-admin-mcp-docker.sh mcpsetup cursor
```

### Option 2: `docker run`

```bash
docker run --rm -it \
  -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
  vast-admin-mcp:latest \
  vast-admin-mcp list clusters
```

### Option 3: docker-compose

```bash
# Optional: pin image tag (see docker-compose.yml for the default)
export VERSION=0.2.8

docker-compose up -d
docker-compose exec app vast-admin-mcp list clusters
docker-compose exec app vast-admin-mcp setup
docker-compose down
```

## Volume mounts

1. **Config directory** (`~/.vast-admin-mcp` → `/root/.vast-admin-mcp`):
   - `config.json` — cluster configurations
   - `mcp_list_template_modifications.yaml` — user template customizations
   - `vast_admin_mcp.log` — log file

2. **Default template file** (built into the image):
   - `mcp_list_cmds_template.yaml` is copied into the image during build
   - To override, mount a custom template:

     ```bash
     docker run --rm -it \
       -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
       -v /path/to/custom-template.yaml:/app/mcp_list_cmds_template.yaml:ro \
       vast-admin-mcp:latest \
       vast-admin-mcp list clusters
     ```

### Mounting behavior

- **`docker run`** (helper script default): volumes are mounted each time you run a command. No persistent container needed.
- **`docker exec`** (persistent containers): volumes must be mounted when the container is created. `docker exec` cannot add new mounts.

**Helper script** (`vast-admin-mcp-docker.sh`):

- Uses `docker run` by default (mounts volumes automatically)
- Uses `docker exec` only if `DOCKER_CONTAINER_NAME` is set

**docker-compose:**

- `docker-compose.yml` mounts the config directory when the container is created
- Use `docker-compose exec` to run commands in the persistent container

**Persistent container manually:**

```bash
docker run -d --name vast-admin-mcp \
  -v ~/.vast-admin-mcp:/root/.vast-admin-mcp \
  vast-admin-mcp:latest tail -f /dev/null
```

## MCP setup from Docker

`mcpsetup` inside Docker detects the container environment and generates host-runnable `docker run` / `docker exec` configs:

```bash
./vast-admin-mcp-docker.sh mcpsetup claude-desktop

# Or with docker-compose
docker-compose exec app vast-admin-mcp mcpsetup claude-desktop
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `DOCKER_CONTAINER_NAME` | Existing container name (use `docker exec`) |
| `DOCKER_IMAGE` | Image name for `docker run` |
| `DOCKER_CONFIG_MOUNT` | Container config path (default: `/root/.vast-admin-mcp`) |
| `DOCKER_TEMPLATE_FILE` | Host path to custom template (optional) |

```bash
export DOCKER_CONTAINER_NAME=vast-admin-mcp
./vast-admin-mcp-docker.sh mcpsetup cursor

export DOCKER_IMAGE=vast-admin-mcp:latest
./vast-admin-mcp-docker.sh mcpsetup claude-desktop

export DOCKER_TEMPLATE_FILE=/path/to/custom-template.yaml
./vast-admin-mcp-docker.sh list clusters
```

## HTTP mode

```bash
MCP_TRANSPORT=http docker-compose up -d

VAST_ADMIN_MCP_AUTH_TOKEN="your-token" MCP_TRANSPORT=http docker-compose up -d

# Server: http://localhost:8000/mcp/
```

See also [Network deployment](../README.md#network-deployment-http) in the main README.
