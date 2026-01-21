# Dockerfile for VAST Admin MCP
# Build and tag with: docker build -t vast-admin-mcp:latest .
# Or use the Makefile task: make build-docker
ARG SOURCE_IMAGE_PREFIX=""
FROM ${SOURCE_IMAGE_PREFIX}python:3.13-slim

# Build arguments for versioning
ARG VERSION=0.1.8
ARG BUILD_DATE
ARG VCS_REF

# Metadata labels
LABEL org.opencontainers.image.title="VAST Admin MCP Server" \
      org.opencontainers.image.description="MCP server for VAST Data administration tasks" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/vast-data/vast-admin-mcp" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.vendor="VAST Data" \
      org.opencontainers.image.authors="Haim Marko <haim.marko@vastdata.com>" \
      org.opencontainers.image.licenses="MIT"

# Install jq (required for field transformations)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

COPY mcp_list_cmds_template.yaml ./

# Install the package
RUN --mount=type=bind,source=dist,target=/tmp/dist \
    pip install --no-cache-dir /tmp/dist/*.whl

# Create directory for user config (will be mounted from host)
# This directory contains:
# - config.json (cluster configurations)
# - mcp_list_template_modifications.yaml (user template customizations)
# - vast_admin_mcp.log (log file)
RUN mkdir -p /root/.vast-admin-mcp

# Set environment variables
# Disable keyring in Docker to force encrypted file storage
# This ensures passwords work reliably in containers
ENV DOCKER_CONTAINER=true \
    VAST_ADMIN_MCP_VERSION=${VERSION} \
    FORCE_ENCRYPTED_STORAGE=true

# Note: The default template file (mcp_list_cmds_template.yaml) is copied into the image
# during build. To override it, mount a custom template file at runtime:
# -v /host/path/template.yaml:/app/mcp_list_cmds_template.yaml:ro

# Default command (can be overridden)
CMD ["vast-admin-mcp"]

