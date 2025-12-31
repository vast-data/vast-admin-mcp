# Makefile for VAST Admin MCP Docker image with version tagging

# Get version from __about__.py
VERSION := $(shell grep '__version__' src/vast_admin_mcp/__about__.py | cut -d'"' -f2)

# Get git commit hash (short)
VCS_REF := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Get build date in ISO 8601 format
BUILD_DATE := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')

# Docker image name (can be overridden via environment variable)
IMAGE_NAME ?= vast-admin-mcp
IMAGE_TAG ?= $(VERSION)

.PHONY: build push info help

# Default target
all: build

## build: Build the Docker image with version tags
build-docker: build-python
	@echo "=================================================="
	@echo "Building VAST Admin MCP Docker Image"
	@echo "=================================================="
	@echo "Version:    $(IMAGE_TAG)"
	@echo "Git Commit: $(VCS_REF)"
	@echo "Build Date: $(BUILD_DATE)"
	@echo "Image Name: $(IMAGE_NAME)"
	@echo "=================================================="
	DOCKER_BUILDKIT=1 docker build \
		--build-arg VERSION="$(VERSION)" \
		--build-arg BUILD_DATE="$(BUILD_DATE)" \
		--build-arg VCS_REF="$(VCS_REF)" \
		-t "$(IMAGE_NAME):$(IMAGE_TAG)" \
		-t "$(IMAGE_NAME):latest" \
		.
	@echo ""
	@echo "✅ Build complete!"
	@echo ""
	@echo "Tagged images:"
	@echo "  - $(IMAGE_NAME):$(IMAGE_TAG)"
	@echo "  - $(IMAGE_NAME):latest"

build-python:
	@echo "=================================================="
	@echo "Building VAST Admin MCP Python Package"
	@echo "=================================================="
	@echo "Version:    $(VERSION)"
	@echo "=================================================="
	python -m build
	@echo ""
	@echo "✅ Build complete!"
	@echo ""
	@echo "Artifacts:"
	@echo "  - dist/vast_admin_mcp-$(VERSION)-py3-none-any.whl"
	@echo "  - dist/vast_admin_mcp-$(VERSION).tar.gz"

build: build-docker build-python

## push: Push the Docker image to registry
push:
	@echo "Pushing $(IMAGE_NAME):$(IMAGE_TAG)..."
	docker push "$(IMAGE_NAME):$(IMAGE_TAG)"
	@echo "Pushing $(IMAGE_NAME):latest..."
	docker push "$(IMAGE_NAME):latest"
	@echo "✅ Push complete!"

## info: Show build information and useful commands
info:
	@echo "Version:    $(IMAGE_TAG)"
	@echo "Git Commit: $(VCS_REF)"
	@echo "Build Date: $(BUILD_DATE)"
	@echo "Image Name: $(IMAGE_NAME)"
	@echo ""
	@echo "To inspect image labels:"
	@echo "  docker inspect $(IMAGE_NAME):$(IMAGE_TAG) | jq '.[0].Config.Labels'"

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /'
	@echo ""
	@echo "Variables:"
	@echo "  IMAGE_NAME    Docker image name (default: vast-admin-mcp)"
	@echo "                Override with: make build IMAGE_NAME=myregistry/vast-admin-mcp"
