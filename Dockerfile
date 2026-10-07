# Dockerfile for VAST Admin MCP
# Build and tag with: docker build -t vast-admin-mcp:latest .
# Or use the Makefile task: make build-docker
ARG SOURCE_IMAGE_PREFIX=""
FROM ${SOURCE_IMAGE_PREFIX}python:3.13-slim

# Build arguments for versioning
ARG VERSION=0.2.6
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

# Install jq and upgrade Debian packages with known CVEs from Trivy
# gzip, glibc (libc6/libc-bin), pcre2, sqlite3, perl-base
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    jq \
    && apt-get install -y --no-install-recommends --only-upgrade \
    gzip \
    libc-bin \
    libc6 \
    libpcre2-8-0 \
    libsqlite3-0 \
    perl-base \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

COPY mcp_list_cmds_template.yaml ./

# Upgrade pip to fix CVE-2026-1703 and CVE-2026-13346 (requires pip>=26.2.0).
# pip>=26.2 ships pip/_vendor/bom.cdx.json listing vendored msgpack/setuptools
# versions that Trivy reports as false positives (trivy#11031) — delete it.
# Remove base-image setuptools (not needed at runtime; wheel install only).
RUN pip install --no-cache-dir --upgrade "pip>=26.2.0" \
    && pip uninstall -y setuptools || true \
    && find /usr/local/lib -path '*/pip/_vendor/bom.cdx.json' -delete \
    && find /usr/local/lib -path '*/ensurepip/_bundled/setuptools*' -delete

# Copy and install the package with HTTP and K8s support
# Using COPY instead of --mount for Podman/Docker compatibility
# [http] - for HTTP transport (uvicorn, starlette)
# [k8s] - for reading secrets from Kubernetes API
COPY dist/*.whl /tmp/
RUN pip install --no-cache-dir "/tmp/vast_admin_mcp-${VERSION}-py3-none-any.whl[http,k8s]" \
    && rm -f /tmp/*.whl

# Create directories for user config and SSL certificates
# Config directory contains:
# - config.json (cluster configurations)
# - mcp_list_template_modifications.yaml (user template customizations)
# - vast_admin_mcp.log (log file)
# - ssl/ (SSL certificates)
RUN mkdir -p /root/.vast-admin-mcp/ssl

# Set environment variables
# Disable keyring in Docker to force encrypted file storage
# This ensures passwords work reliably in containers
ENV DOCKER_CONTAINER=true \
    VAST_ADMIN_MCP_VERSION=${VERSION} \
    FORCE_ENCRYPTED_STORAGE=true

# Expose HTTP port for network MCP access
EXPOSE 8000

# Health check for HTTP mode
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Note: The default template file (mcp_list_cmds_template.yaml) is copied into the image
# during build. To override it, mount a custom template file at runtime:
# -v /host/path/template.yaml:/app/mcp_list_cmds_template.yaml:ro

# Default command (can be overridden)
# For stdio mode: vast-admin-mcp mcp
# For HTTP mode: vast-admin-mcp mcp --transport http --host 0.0.0.0
CMD ["vast-admin-mcp"]

