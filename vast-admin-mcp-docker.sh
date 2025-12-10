#!/bin/bash
# Helper script to run VAST Admin MCP CLI commands in Docker container
#
# Usage:
#   ./vast-admin-mcp-docker.sh <command> [args...]
#
# Examples:
#   ./vast-admin-mcp-docker.sh list clusters
#   ./vast-admin-mcp-docker.sh list views --cluster vast.example.com
#   ./vast-admin-mcp-docker.sh setup
#   ./vast-admin-mcp-docker.sh mcpsetup cursor
#   ./vast-admin-mcp-docker.sh mcp --read-write
#
# Environment variables:
#   DOCKER_IMAGE: Docker image name (default: vast-admin-mcp:latest)
#   DOCKER_CONTAINER_NAME: Use existing container name (for docker exec)
#   DOCKER_CONFIG_DIR: Host config directory to mount (default: ~/.vast-admin-mcp)
#   DOCKER_TEMPLATE_FILE: Host path to default template file (optional, for override)

set -e

# Default values
DOCKER_IMAGE="${DOCKER_IMAGE:-vast-admin-mcp:latest}"
DOCKER_CONTAINER_NAME="${DOCKER_CONTAINER_NAME:-}"
DOCKER_CONFIG_DIR="${DOCKER_CONFIG_DIR:-$HOME/.vast-admin-mcp}"
DOCKER_TEMPLATE_FILE="${DOCKER_TEMPLATE_FILE:-}"

# Ensure config directory exists on host
mkdir -p "$DOCKER_CONFIG_DIR"

# Check if we should use docker exec (existing container)
if [ -n "$DOCKER_CONTAINER_NAME" ]; then
    # Use docker exec for existing running container
    # Note: Volumes must be mounted when container is created (e.g., via docker-compose)
    docker exec -i "$DOCKER_CONTAINER_NAME" vast-admin-mcp "$@"
else
    
    # Build volume mount arguments
    VOLUME_MOUNTS=(
        -v "$DOCKER_CONFIG_DIR:/root/.vast-admin-mcp"
    )
    
    # Add template file mount if specified
    if [ -n "$DOCKER_TEMPLATE_FILE" ] && [ -f "$DOCKER_TEMPLATE_FILE" ]; then
        VOLUME_MOUNTS+=(-v "$DOCKER_TEMPLATE_FILE:/app/mcp_list_cmds_template.yaml:ro")
        echo "   Mounting template file: $DOCKER_TEMPLATE_FILE"
    fi
    
    
    # Get absolute path to vast-admin-mcp-docker.sh script (for MCP command generation)
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    DOCKER_RUN_SCRIPT_PATH="${SCRIPT_DIR}/vast-admin-mcp-docker.sh"
    
    # Set environment variables so mcpsetup can detect Docker and generate correct commands
    ENV_VARS=(
        -e DOCKER_CONTAINER=true
        -e DOCKER_IMAGE="$DOCKER_IMAGE"
        -e HOST_CONFIG_DIR="$DOCKER_CONFIG_DIR"
        -e DOCKER_RUN_SCRIPT_PATH="$DOCKER_RUN_SCRIPT_PATH"
    )
    
    docker run --rm -i \
        "${ENV_VARS[@]}" \
        "${VOLUME_MOUNTS[@]}" \
        "$DOCKER_IMAGE" \
        vast-admin-mcp "$@"
fi

