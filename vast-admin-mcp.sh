#!/bin/bash
# VAST Admin MCP Server - Convenience script for running commands
# Usage: ./vast-admin-mcp.sh [command] [options]
# Examples:
#   ./vast-admin-mcp.sh setup
#   ./vast-admin-mcp.sh mcp
#   ./vast-admin-mcp.sh mcp --read-write
#   ./vast-admin-mcp.sh mcp --read-write --debug

cd "$(dirname "$0")"

# Clear Python cache and force fresh imports
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete 2>/dev/null

# Check if we're in a virtual environment or use system Python
if [ -z "$VIRTUAL_ENV" ]; then
    PYTHON_CMD="python3"
else
    PYTHON_CMD="python"
fi

# Set PYTHONPATH to include src directory for development mode
# This allows running the script before installation
PYTHONPATH=src "$PYTHON_CMD" -B -m vast_admin_mcp "$@"

