"""Argument parser creation for CLI commands."""

import argparse
from typing import Optional

from ..template_parser import TemplateParser
from ..utils import to_cli_name


def create_list_parser() -> argparse.ArgumentParser:
    """Create parser for list command with dynamic arguments."""
    parser = argparse.ArgumentParser(
        prog='vast-admin-mcp list',
        description='Execute dynamic list commands or list available commands',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'list_command',
        nargs='?',
        help="Command name to execute (e.g., 'views', 'tenants')"
    )
    
    parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    
    parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    
    parser.add_argument(
        '--order',
        type=str,
        help='Sort results by field. Format: "field_name:direction" using colon separator. Use underscores for field names (e.g., "logical_used" not "logical used"). Examples: "physical_used:desc", "logical_used:asc", "name:desc". Direction: a/as/asc/ascending or d/de/desc/descending. Default: asc. Multiple: "field1:desc,field2:asc"'
    )
    
    parser.add_argument(
        '--top',
        type=int,
        help='Limit output to top N results'
    )
    
    parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    parser.add_argument(
        '--instance',
        action='store_true',
        help='Include full original API response in JSON output (under "instance" field). Only works with --format json or --output <file>.json'
    )
    
    return parser


def add_dynamic_arguments(parser: argparse.ArgumentParser, command_name: str, template_parser: TemplateParser, is_merged: bool = False) -> None:
    """Add dynamic arguments from template to parser.
    
    Args:
        parser: ArgumentParser instance
        command_name: Name of the command
        template_parser: TemplateParser instance
        is_merged: If True, use get_merged_arguments instead of get_arguments
    """
    if is_merged:
        args_config = template_parser.get_merged_arguments(command_name)
    else:
        args_config = template_parser.get_arguments(command_name)
    
    for arg_config in args_config:
        arg_name = arg_config.get('name')
        arg_type = arg_config.get('type', 'str')
        arg_mandatory = arg_config.get('mandatory', False)
        arg_default = arg_config.get('default')
        
        # Convert arg name to CLI format using helper function
        cli_name = '--' + to_cli_name(arg_name)
        
        # Get description for CLI help (auto-generated if not provided)
        arg_description = arg_config.get('description', '')
        
        if arg_type == 'bool':
            parser.add_argument(
                cli_name,
                action='store_true',
                help=arg_description,
                required=arg_mandatory
            )
        elif arg_type == 'int':
            # For int arguments with filter:true, use str type to allow filter syntax (e.g., ">1TB")
            # The actual parsing and validation happens in command_executor
            if arg_config.get('filter', False):
                parser.add_argument(
                    cli_name,
                    type=str,
                    help=arg_description,
                    required=arg_mandatory,
                    default=arg_default
                )
            else:
                parser.add_argument(
                    cli_name,
                    type=int,
                    help=arg_description,
                    required=arg_mandatory,
                    default=arg_default
                )
        elif arg_type == 'list':
            # For list type, check if argument_list is true (comma-separated string)
            # or false (multiple CLI arguments)
            argument_list = arg_config.get('argument_list', False)
            is_filter = arg_config.get('filter', False)
            
            # Enhance description for list fields with filter:true
            if is_filter and arg_description:
                filter_help = " Filter syntax: exact match (e.g., 'user1'), 'in:value' (e.g., 'in:user1'), wildcards (e.g., '*admin*'), or substring (e.g., 'admin' matches 'admin1', 'admin2'). Case-insensitive."
                arg_description = arg_description + filter_help
            
            if argument_list:
                # Comma-separated string (e.g., "cluster1,cluster2")
                parser.add_argument(
                    cli_name,
                    type=str,
                    help=arg_description,
                    required=arg_mandatory
                )
            else:
                # Multiple CLI arguments (e.g., --arg val1 --arg val2)
                parser.add_argument(
                    cli_name,
                    nargs='+',
                    help=arg_description,
                    required=arg_mandatory
                )
        else:  # str or other
            parser.add_argument(
                cli_name,
                type=str,
                help=arg_description,
                required=arg_mandatory,
                default=arg_default
            )

