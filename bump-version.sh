#!/bin/bash
# Version bump script for VAST Admin MCP

set -e

ABOUT_FILE="src/vast_admin_mcp/__about__.py"
README_FILE="README.md"
DOCKER_COMPOSE_FILE="docker-compose.yml"
DOCKERFILE="Dockerfile"

# Function to display usage
usage() {
    echo "Usage: $0 [major|minor|patch|VERSION]"
    echo ""
    echo "Examples:"
    echo "  $0 patch      # 0.1.0 -> 0.1.1"
    echo "  $0 minor      # 0.1.0 -> 0.2.0"
    echo "  $0 major      # 0.1.0 -> 1.0.0"
    echo "  $0 1.5.2      # Set version to 1.5.2"
    exit 1
}

# Check if argument is provided
if [ $# -eq 0 ]; then
    usage
fi

# Get current version
CURRENT_VERSION=$(grep -E '^__version__' "$ABOUT_FILE" | grep -oE '"[^"]+"' | tr -d '"')

echo "Current version: ${CURRENT_VERSION}"

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Determine new version
case "$1" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
        ;;
    patch)
        PATCH=$((PATCH + 1))
        NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
        ;;
    *)
        # Assume it's a specific version number
        if [[ ! "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "❌ Error: Invalid version format. Use semantic versioning (e.g., 1.2.3)"
            usage
        fi
        NEW_VERSION="$1"
        ;;
esac

echo "New version:     ${NEW_VERSION}"
echo ""

# Confirm with user
read -p "Update version to ${NEW_VERSION}? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Version bump cancelled"
    exit 1
fi

# Update version in __about__.py
sed -i.bak "s/__version__ = \"${CURRENT_VERSION}\"/__version__ = \"${NEW_VERSION}\"/" "$ABOUT_FILE"
rm -f "${ABOUT_FILE}.bak"
echo "✅ Version updated in ${ABOUT_FILE}"

# Update version in README.md (export VERSION=0.1.3)
sed -i.bak "s/export VERSION=${CURRENT_VERSION}/export VERSION=${NEW_VERSION}/" "$README_FILE"
rm -f "${README_FILE}.bak"
echo "✅ Version updated in ${README_FILE}"

# Update version in docker-compose.yml (two occurrences)
# VERSION: ${VERSION:-0.1.3}
# - VAST_ADMIN_MCP_VERSION=${VERSION:-0.1.3}
sed -i.bak "s/VERSION:-${CURRENT_VERSION}/VERSION:-${NEW_VERSION}/g" "$DOCKER_COMPOSE_FILE"
rm -f "${DOCKER_COMPOSE_FILE}.bak"
echo "✅ Version updated in ${DOCKER_COMPOSE_FILE}"

# Update version in Dockerfile (ARG VERSION=0.1.3)
sed -i.bak "s/ARG VERSION=${CURRENT_VERSION}/ARG VERSION=${NEW_VERSION}/" "$DOCKERFILE"
rm -f "${DOCKERFILE}.bak"
echo "✅ Version updated in ${DOCKERFILE}"

echo ""
echo "📝 Files updated:"
echo "  - ${ABOUT_FILE}"
echo "  - ${README_FILE}"
echo "  - ${DOCKER_COMPOSE_FILE}"
echo "  - ${DOCKERFILE}"
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff ${ABOUT_FILE} ${README_FILE} ${DOCKER_COMPOSE_FILE} ${DOCKERFILE}"
echo "  2. Commit changes: git add ${ABOUT_FILE} ${README_FILE} ${DOCKER_COMPOSE_FILE} ${DOCKERFILE} && git commit -m \"Bump version to ${NEW_VERSION}\""
echo "  3. Tag release:    git tag -a v${NEW_VERSION} -m \"Release v${NEW_VERSION}\""
echo "  4. Build package:  make build-python"
echo "  5. Build Docker:   make build-docker"
echo "  6. Push to PyPI:   python -m twine upload dist/vast_admin_mcp-${NEW_VERSION}*"
echo ""
