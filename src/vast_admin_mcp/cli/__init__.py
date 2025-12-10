"""CLI module for vast-admin-mcp.

This package contains the command-line interface components split into focused modules:
- parsers: Argument parser creation
- handlers: Command handlers
- mcp_codegen: MCP code generation
- config_helpers: Configuration utilities

Note: The main() function is still in the parent cli.py module for now.
This package structure is set up for future refactoring.
"""

# Import main directly from the parent cli.py file to avoid circular imports
import importlib.util
import os

# Get the path to the parent cli.py file
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_cli_py_path = os.path.join(_parent_dir, 'cli.py')

# Load the module directly from the file
_spec = importlib.util.spec_from_file_location("vast_admin_mcp.cli_module", _cli_py_path)
_cli_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_cli_module)

# Export main function
main = _cli_module.main

__all__ = ['main']

