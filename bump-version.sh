#!/bin/bash
# Version bump script for VAST Admin MCP

set -e

ABOUT_FILE="src/vast_admin_mcp/__about__.py"

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
echo ""
echo "Next steps:"
echo "  1. Review changes: git diff ${ABOUT_FILE}"
echo "  2. Commit changes: git add ${ABOUT_FILE} && git commit -m \"Bump version to ${NEW_VERSION}\""
echo "  3. Tag release:    git tag -a v${NEW_VERSION} -m \"Release v${NEW_VERSION}\""
echo "  4. Build package:  make build-python"
echo "  5. Build Docker:   make build-docker"
echo "  6. Push to PyPI:   python -m twine upload dist/vast_admin_mcp-${NEW_VERSION}*"
echo ""

