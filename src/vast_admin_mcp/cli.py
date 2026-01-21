import os
import sys
import argparse
import re
from typing import Optional, Dict, Tuple, List, Any

from .config import CONFIG_FILE, TEMPLATE_MODIFICATIONS_FILE, get_default_template_path
from .mcp_server import start_mcp
from .setup import setup_config
from .utils import output_results, logging_main, to_cli_name, handle_errors
from .template_parser import TemplateParser
from .functions import list_dynamic, list_merged, list_clusters, list_performance, list_performance_graph, list_monitors, list_view_instances, list_fields, describe_tool, query_users
from .create_functions import create_view, create_view_from_template, create_snapshot, create_clone, create_quota


def create_list_parser():
    """Create parser for list command with dynamic arguments"""
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
    """Add dynamic arguments from template to parser
    
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


def handle_list_command(list_args=None):
    """Handle the list command with dynamic argument parsing"""
    if list_args is None:
        # Find 'list' in sys.argv to get the position
        try:
            list_idx = sys.argv.index('list')
            # Get all args after 'list'
            list_args = sys.argv[list_idx + 1:]
        except ValueError:
            list_args = sys.argv[1:]
    
    # Check for help request before parsing (to handle command-specific help)
    if '--help' in list_args or '-h' in list_args:
        # Check if there's a command name before --help
        help_idx = list_args.index('--help') if '--help' in list_args else list_args.index('-h')
        if help_idx > 0 and not list_args[help_idx - 1].startswith('-'):
            # There's a command name, show command-specific help
            command_name = list_args[help_idx - 1]
            # Don't initialize logging for help - it's not needed and causes duplicates
            default_template_path = get_default_template_path()
            if not os.path.exists(TEMPLATE_MODIFICATIONS_FILE) and not default_template_path:
                print(f"Template modifications file {TEMPLATE_MODIFICATIONS_FILE} not found and no default template available.", file=sys.stderr)
                sys.exit(1)
            template_parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
            template = template_parser.get_command_template(command_name)
            merged_template = template_parser.get_merged_command_template(command_name)
            if template:
                # Don't include description in CLI help - only for MCP integration
                help_parser = create_list_parser()
                add_dynamic_arguments(help_parser, command_name, template_parser, is_merged=False)
                help_parser.print_help()
            elif merged_template:
                # Merged command
                help_parser = create_list_parser()
                add_dynamic_arguments(help_parser, command_name, template_parser, is_merged=True)
                help_parser.print_help()
            else:
                # Fall through to normal help
                base_parser = create_list_parser()
                base_parser.print_help()
            sys.exit(0)
    
    # Create base parser to get command name and basic options
    base_parser = create_list_parser()
    
    # Parse known args first to get command name and basic options
    args, unknown = base_parser.parse_known_args(list_args)
    
    logging_main(debug=args.debug)
    
    # Load template parser
    default_template_path = get_default_template_path()
    if not os.path.exists(TEMPLATE_MODIFICATIONS_FILE) and not default_template_path:
        print(f"Template modifications file {TEMPLATE_MODIFICATIONS_FILE} not found and no default template available.", file=sys.stderr)
        sys.exit(1)
    
    try:
        template_parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Get the list command name (renamed from 'command' to 'list_command' to avoid conflict)
    list_command_name = getattr(args, 'list_command', None)
    
    # Execute command
    if not list_command_name:
        # Check if help was requested
        if '--help' in unknown or '-h' in unknown:
            base_parser.print_help()
            sys.exit(0)
        print("Error: Command name is required. Use --list-commands to see available commands.", file=sys.stderr)
        sys.exit(1)
    
    # Check if help was requested for a specific command
    if '--help' in unknown or '-h' in unknown:
        template = template_parser.get_command_template(list_command_name)
        merged_template = template_parser.get_merged_command_template(list_command_name)
        if template:
            # Create parser with dynamic args to show full help
            # Don't include description in CLI help - only for MCP integration
            help_parser = create_list_parser()
            add_dynamic_arguments(help_parser, list_command_name, template_parser, is_merged=False)
            help_parser.print_help()
        elif merged_template:
            # Merged command
            help_parser = create_list_parser()
            add_dynamic_arguments(help_parser, list_command_name, template_parser, is_merged=True)
            help_parser.print_help()
        else:
            print(f"Error: Command '{list_command_name}' not found.", file=sys.stderr)
        sys.exit(0)
    
    # Note: 'performance' and 'clusters' are now top-level commands, not list subcommands
    # If someone tries 'list performance' or 'list clusters', they'll get an error from template lookup
    
    # Check if command exists in template or merged commands
    template = template_parser.get_command_template(list_command_name)
    merged_template = template_parser.get_merged_command_template(list_command_name)
    is_merged = merged_template is not None
    
    if not template and not merged_template:
        print(f"Error: Command '{list_command_name}' not found. Use --list-commands to see available commands.", file=sys.stderr)
        sys.exit(1)
    
    # Create full parser with dynamic arguments
    full_parser = create_list_parser()
    add_dynamic_arguments(full_parser, list_command_name, template_parser, is_merged=is_merged)
    
    # Don't add description to CLI help - only for MCP integration
    
    # Parse all arguments (including dynamic ones) using the full list_args
    try:
        full_args = full_parser.parse_args(list_args)
    except SystemExit:
        sys.exit(1)
    
    # Auto-detect format from output file extension if format not explicitly set
    output_format = full_args.format
    output_file = full_args.output
    
    # If output is specified and format is default (table), try to detect from extension
    if output_file and output_format == 'table':
        output_lower = output_file.lower()
        if output_lower.endswith('.json'):
            output_format = 'json'
        elif output_lower.endswith('.csv'):
            output_format = 'csv'
        # If output is just "json", "csv", or "table" (no extension), treat as format
        elif output_file.lower() in ['json', 'csv', 'table']:
            output_format = output_file.lower()
            output_file = None  # Don't write to file, just use format
    
    # Build arguments dict from parsed args (including dynamic ones)
    cli_args = {}
    for key, value in vars(full_args).items():
        if key not in ['list_command', 'format', 'output', 'debug', 'order', 'top', 'mcp', 'instance'] and value is not None:
            # Convert --cluster-name back to cluster_name
            # For fields with underscores (from spaces), keep underscores
            # For fields with dashes, convert to underscores
            cli_args[key.replace('-', '_')] = value
    
    # Handle order and top separately (they're not dynamic arguments)
    if hasattr(full_args, 'order') and full_args.order is not None:
        cli_args['order'] = full_args.order
    if hasattr(full_args, 'top') and full_args.top is not None:
        cli_args['top'] = full_args.top
    
    # Handle mcp debug flag
    if hasattr(full_args, 'mcp') and full_args.mcp:
        cli_args['mcp'] = True
    
    # Handle instance flag - pass both instance flag and output format
    if hasattr(full_args, 'instance') and full_args.instance:
        cli_args['instance'] = True
    # Always pass output format (needed for instance check)
    cli_args['_output_format'] = output_format
    
    try:
        # Use list_merged for merged commands, list_dynamic for regular commands
        if is_merged:
            results = list_merged(list_command_name, **cli_args)
        else:
            results = list_dynamic(list_command_name, **cli_args)
        
        # Special handling for --mcp flag: print Python code instead of table/JSON
        if hasattr(full_args, 'mcp') and full_args.mcp and results and len(results) > 0:
            # Check if result contains Python code
            if isinstance(results[0], dict) and '_mcp_python_code' in results[0]:
                print(results[0]['_mcp_python_code'])
                return
        
        output_results(results, format=output_format, output_file=output_file)
    except Exception as e:
        print(f"Error executing command '{list_command_name}': {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_performance_command(args):
    """Handle performance command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_performance_mcp_code()
        print(python_code)
        return
    
    # Validate required arguments when not using --mcp
    if not args.object_name:
        print("Error: object_name is required. Choose one of: cluster, cnode, host, user, vippool, view, tenant", file=sys.stderr)
        sys.exit(1)
    
    if not args.cluster:
        print("Error: --cluster/-c is required", file=sys.stderr)
        sys.exit(1)
    
    try:
        results = list_performance(
            object_name=args.object_name,
            cluster=args.cluster,
            timeframe=args.timeframe,
            instances=args.instances
        )
        
        # Convert dict to list of lists for table output
        # Results is a dict: {instance_name: [list of metric rows]}
        output_data = []
        for instance_name, rows in results.items():
            output_data.extend(rows)
        
        output_results(output_data, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing performance command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_list_monitors_command(args):
    """Handle list-monitors command"""
    logging_main(debug=args.debug)
    
    if not args.cluster:
        print("Error: --cluster/-c is required", file=sys.stderr)
        sys.exit(1)
    
    try:
        monitors = list_monitors(
            cluster=args.cluster,
            object_type=args.object_type if hasattr(args, 'object_type') and args.object_type else None
        )
        
        # Format output
        output_data = []
        for monitor in monitors:
            output_data.append({
                'id': monitor.get('id', 'N/A'),
                'name': monitor.get('name', 'N/A'),
                'object_type': monitor.get('object_type', 'N/A'),
                'prop_list_count': len(monitor.get('prop_list', [])),
                'time_frame': monitor.get('time_frame', 'N/A'),
                'granularity': monitor.get('granularity', 'N/A')
            })
        
        output_results(output_data, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing list-monitors command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_performance_graph_command(args):
    """Handle performance-graph command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_performance_graph_mcp_code()
        print(python_code)
        return
    
    # Validate required arguments when not using --mcp
    if not args.monitor_name:
        print("Error: --monitor-name/-m is required. Use 'list-monitors' command to see available monitors.", file=sys.stderr)
        sys.exit(1)
    
    if not args.cluster:
        print("Error: --cluster/-c is required", file=sys.stderr)
        sys.exit(1)
    
    if not args.object_name:
        print("Error: --object-name is required. Use 'list-monitors' command to see available monitors and their object types.", file=sys.stderr)
        sys.exit(1)
    
    try:
        results = list_performance_graph(
            monitor_name=args.monitor_name,
            cluster=args.cluster,
            timeframe=args.timeframe,
            instances=args.instances,
            object_name=args.object_name,
            format='png'  # Image format is always PNG
        )
        
        # Output the results
        if args.format == 'json':
            import json
            output = json.dumps(results, indent=2)
        else:
            # Table format - show key information and statistics
            output = f"Performance graph generated successfully!\n\n"
            output += f"Resource URI: {results.get('resource_uri', 'N/A')}\n"
            output += f"File Path: {results.get('file_path', 'N/A')}\n"
            output += f"Monitor Name: {results.get('monitor_name', 'N/A')}\n"
            output += f"Timeframe: {results.get('timeframe', 'N/A')}\n"
            output += f"Instances: {', '.join(results.get('instances', []))}\n\n"
            
            # Display statistics tables
            statistics = results.get('statistics', {})
            
            # Display per-instance tables only if instances were explicitly specified by user
            instances_specified = args.instances is not None and args.instances.strip() != ''
            if instances_specified and 'instances' in statistics and statistics['instances']:
                for instance_stat in statistics['instances']:
                    instance_name = instance_stat.get('instance_name', 'Unknown')
                    metrics = instance_stat.get('metrics', [])
                    
                    if metrics:
                        output += f"\n{'='*80}\n"
                        output += f"Statistics for Instance: {instance_name}\n"
                        output += f"{'='*80}\n\n"
                        
                        # Format statistics table
                        table_data = []
                        for metric in metrics:
                            metric_name = metric.get('metric_name', 'Unknown')
                            unit = metric.get('unit', 'unknown')
                            
                            # Format values based on unit
                            avg_val = metric.get('avg', 0)
                            p95_val = metric.get('p95', 0)
                            max_val = metric.get('max', 0)
                            
                            if unit == 'iops':
                                row = {
                                    'Metric': metric_name,
                                    'Avg': int(avg_val),
                                    'P95': int(p95_val),
                                    'Max': int(max_val)
                                }
                            elif unit == 'latency':
                                row = {
                                    'Metric': metric_name,
                                    'Avg (ms)': f"{avg_val/1000:.2f}",
                                    'P95 (ms)': f"{p95_val/1000:.2f}",
                                    'Max (ms)': f"{max_val/1000:.2f}"
                                }
                            elif unit == 'bw':
                                from vast_admin_mcp.utils import pretty_size
                                row = {
                                    'Metric': metric_name,
                                    'Avg': pretty_size(avg_val) + '/s',
                                    'P95': pretty_size(p95_val) + '/s',
                                    'Max': pretty_size(max_val) + '/s'
                                }
                            else:
                                row = {
                                    'Metric': metric_name,
                                    'Avg': f"{avg_val:.2f}",
                                    'P95': f"{p95_val:.2f}",
                                    'Max': f"{max_val:.2f}"
                                }
                            table_data.append(row)
                        
                        if table_data:
                            from vast_admin_mcp.utils import output_results
                            import io
                            import sys
                            # Capture table output
                            old_stdout = sys.stdout
                            sys.stdout = buffer = io.StringIO()
                            output_results(table_data, format='table')
                            table_output = buffer.getvalue()
                            sys.stdout = old_stdout
                            output += table_output + "\n"
            
            # Always display summary table
            if 'summary' in statistics and statistics['summary'].get('metrics'):
                output += f"\n{'='*80}\n"
                output += f"Summary Statistics (All Instances)\n"
                output += f"{'='*80}\n\n"
                
                summary_metrics = statistics['summary']['metrics']
                table_data = []
                for metric in summary_metrics:
                    metric_name = metric.get('metric_name', 'Unknown')
                    unit = metric.get('unit', 'unknown')
                    
                    # Format values based on unit
                    avg_val = metric.get('avg', 0)
                    p95_val = metric.get('p95', 0)
                    max_val = metric.get('max', 0)
                    
                    if unit == 'iops':
                        row = {
                            'Metric': metric_name,
                            'Avg': int(avg_val),
                            'P95': int(p95_val),
                            'Max': int(max_val)
                        }
                    elif unit == 'latency':
                        row = {
                            'Metric': metric_name,
                            'Avg (ms)': f"{avg_val/1000:.2f}",
                            'P95 (ms)': f"{p95_val/1000:.2f}",
                            'Max (ms)': f"{max_val/1000:.2f}"
                        }
                    elif unit == 'bw':
                        from vast_admin_mcp.utils import pretty_size
                        row = {
                            'Metric': metric_name,
                            'Avg': pretty_size(avg_val) + '/s',
                            'P95': pretty_size(p95_val) + '/s',
                            'Max': pretty_size(max_val) + '/s'
                        }
                    else:
                        row = {
                            'Metric': metric_name,
                            'Avg': f"{avg_val:.2f}",
                            'P95': f"{p95_val:.2f}",
                            'Max': f"{max_val:.2f}"
                        }
                    table_data.append(row)
                
                if table_data:
                    from vast_admin_mcp.utils import output_results
                    import io
                    import sys
                    # Capture table output
                    old_stdout = sys.stdout
                    sys.stdout = buffer = io.StringIO()
                    output_results(table_data, format='table')
                    table_output = buffer.getvalue()
                    sys.stdout = old_stdout
                    output += table_output + "\n"
        
        if args.output:
            with open(args.output, 'w') as f:
                if args.format == 'json':
                    f.write(output)
                else:
                    f.write(output)
            print(f"Results written to {args.output}")
        else:
            print(output)
            
    except Exception as e:
        print(f"Error executing performance-graph command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_query_users_command(args):
    """Handle query-users command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_query_users_mcp_code()
        print(python_code)
        return
    
    # Validate required arguments when not using --mcp
    if not args.cluster:
        print("Error: --cluster/-c is required", file=sys.stderr)
        sys.exit(1)
    
    if not args.prefix or len(args.prefix.strip()) < 1:
        print("Error: --prefix/-p is required and must be at least 1 character", file=sys.stderr)
        sys.exit(1)
    
    try:
        results = query_users(
            cluster=args.cluster,
            tenant=args.tenant or 'default',
            prefix=args.prefix,
            top=args.top
        )
        
        output_results(results, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing query-users command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_clusters_command(args):
    """Handle clusters command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_clusters_mcp_code()
        print(python_code)
        return
    
    try:
        results = list_clusters(clusters=args.clusters)
        output_results(results, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing clusters command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def _generate_performance_mcp_code() -> str:
    """Generate Python code representation of list_performance MCP function"""
    return '''    @mcp.tool(name="list_performance_vast", description="Retrieve performance metrics for VAST cluster objects")
    async def list_performance_mcp(
        object_name: str,
        cluster: str,
        timeframe: str = '5m',
        instances: str = ''
    ) -> Dict:
        """
        Use this tool to retrieve performance metrics for VAST cluster objects using the ad_hoc_query API.

        CRITICAL: The object_name parameter must be the OBJECT TYPE only (e.g., "view", "cnode", "tenant"), NOT a specific instance name.
        If you want metrics for a specific instance like "view-142", you MUST:
        1. Set object_name to the type: "view"
        2. Set instances to the specific instance in the correct format: "tenant_name:view_name" (e.g., "tenant1:view-142")

        Args:
            object_name (str): Object TYPE to get metrics for. Must be EXACTLY one of: cluster, cnode, host, user, vippool, view, tenant.
                              DO NOT include instance names or identifiers here (e.g., use "view" NOT "view-142" or "view_name").
                              If you have a specific instance like "view-142", set object_name="view" and use instances parameter.
            cluster (str): Target cluster address or name (required).
            timeframe (str): Time frame for metrics (e.g., 5m, 1h, 24h). Defaults to '5m'.
            instances (str): Comma-separated list of specific instance identifiers.
                            - For most object types: provide instance names directly (e.g., "cnode1,cnode2").
                            - For views (which are tenant-specific): MUST use format "tenant_name:view_name" (e.g., "tenant1:myview").
                              Use view name, NOT view path. Example: "tenant1:view1,tenant2:view2".
                            - Leave empty string to get metrics for all instances of the object type.
                            - If object_name is "view" and you want specific views, instances is REQUIRED in "tenant:view_name" format.

        Returns:
            A dict where each key is the instance name which contains a list of performance metrics. Each item contains metric, IOPS 95th Percentile, IOPS Average, IOPS Max, BW 95th Percentile, BW Average, BW Max, LATENCY 95th Percentile, LATENCY Average, LATENCY Max for the different objects.
            
        Examples:
            - Get all view metrics: object_name="view", cluster="vast3115-var", instances=""
            - Get specific view "view-142": object_name="view", cluster="vast3115-var", instances="tenant1:view-142"
            - Get all cnode metrics: object_name="cnode", cluster="vast3115-var", instances=""
            - Get specific cnodes: object_name="cnode", cluster="vast3115-var", instances="cnode1,cnode2"
        """
        try:
            from vast_admin_mcp.functions import list_performance
            performance_data = list_performance(
                object_name=object_name,
                cluster=cluster,
                timeframe=timeframe or '5m',
                instances=instances or None
            )
            return performance_data
        except Exception as e:
            import logging
            logging.error(f"Error listing performance metrics: {e}")
            raise
'''


def _generate_query_users_mcp_code() -> str:
    """Generate Python code representation of query_users MCP function"""
    return '''    @mcp.tool(name="query_users_vast", description="Query user names from VAST cluster using users/names endpoint")
    async def query_users_mcp(
        cluster: str,
        tenant: str = 'default',
        prefix: str = '',
        top: int = 20
    ) -> list:
        """
        Use this tool to query user names from a VAST cluster using the users/names endpoint.
        
        Args:
            cluster (str): Target cluster address or name (required). Use list_clusters_vast() first to discover available cluster names.
            tenant (str): Tenant name to query (required, defaults to 'default').
            prefix (str): Prefix to filter usernames (required, must be at least 1 character).
            top (int): Maximum number of results to return (optional, defaults to 20, maximum: 50 due to API limit).
        
        Returns:
            A list of user dictionaries. Each dictionary contains:
            - login_name: Login name
            - fqdn: Fully qualified domain name
            - name: Display name
            - uid: User ID
            - gid: Leading group GID
            - group: Leading group name
            - primary group: Primary group name
            - groups: Comma-separated list of group names
            - origins: Comma-separated key:value pairs of user origins
            - s3 info: Comma-separated S3 permissions and connection info (e.g., "allow create bucket", "allow delete bucket", "superuser", "s3 connectons:N")
            - access_keys: Comma-separated list of enabled S3 access keys
            - identity policies: Comma-separated list of S3 identity policy names
            
        Examples:
            - Query users with prefix "hmarko": cluster="vast3115-var", tenant="default", prefix="hmarko"
            - Query users with custom limit: cluster="vast3115-var", tenant="tenant1", prefix="admin", top=50
        """
        try:
            from vast_admin_mcp.functions import query_users
            users_data = query_users(
                cluster=cluster,
                tenant=tenant or 'default',
                prefix=prefix,
                top=top
            )
            return users_data
        except Exception as e:
            import logging
            logging.error(f"Error querying users: {e}")
            raise
'''


def _generate_clusters_mcp_code() -> str:
    """Generate Python code representation of list_clusters MCP function"""
    return '''\n\n    @mcp.tool(name="list_clusters_vast", description="Retrieve information about configured VAST clusters")
    async def list_clusters_mcp(
        clusters: str = ''
    ) -> List[Dict]:
        """
        Use this tool to retrieve information about configured VAST clusters, including status, version, capacity, 
        and performance metrics.

        Args:
            clusters (str): Comma-separated list of specific cluster addresses to list. If empty, lists all 
            configured clusters. Defaults to empty string.

        Returns:
            A list of dictionaries containing cluster information including Cluster, State, Version, Uptime, 
            Logical Used, Physical Used, Logical Free, Physical Free, IOPS, and Throughput.
        """
        try:
            from vast_admin_mcp.functions import list_clusters
            clusters_result = list_clusters(
                clusters=clusters if clusters else None
            )
            return clusters_result
        except Exception as e:
            import logging
            logging.error(f"Error listing clusters: {e}")
            raise
'''


def handle_view_instances_command(args):
    """Handle view-instances command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_view_instances_mcp_code()
        print(python_code)
        return
    
    try:
        results = list_view_instances(
            cluster=args.cluster,
            tenant=args.tenant,
            name=args.name,
            path=args.path
        )
        output_results(results, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing view-instances command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_fields_command(args):
    """Handle fields command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_fields_mcp_code()
        print(python_code)
        return
    
    try:
        results = list_fields(command_name=args.command_name)
        output_results(results, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing fields command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def handle_describe_command(args):
    """Handle describe command"""
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp:
        # Generate Python code representation
        python_code = _generate_describe_mcp_code()
        print(python_code)
        return
    
    try:
        results = describe_tool(tool_name=args.tool_name)
        output_results(results, format=args.format, output_file=args.output)
    except Exception as e:
        print(f"Error executing describe command: {e}", file=sys.stderr)
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def _generate_view_instances_mcp_code() -> str:
    """Generate Python code representation of list_view_instances MCP function"""
    return '''    @mcp.tool(name="list_view_instances_vast", description="List view instances to help discover available views")
    async def list_view_instances_mcp(
        cluster: str,
        tenant: str = '',
        name: str = '',
        path: str = ''
    ) -> list:
        """
        List view instances to help discover available views.
        
        Args:
            cluster (str): Target cluster address or name (required)
            tenant (str): Filter by tenant name (optional, supports wildcards)
            name (str): Filter by view name (optional, supports wildcards like *pvc*)
            path (str): Filter by view path (optional, supports wildcards like */data/*)
        
        Returns:
            A list of dictionaries, each containing tenant, name, path, protocols, and has_bucket.
        """
        try:
            from vast_admin_mcp.functions import list_view_instances
            result = list_view_instances(
                cluster=cluster,
                tenant=tenant if tenant else None,
                name=name if name else None,
                path=path if path else None
            )
            return result
        except Exception as e:
            import logging
            logging.error(f"Error listing view instances: {e}")
            raise
'''


def _generate_fields_mcp_code() -> str:
    """Generate Python code representation of list_fields MCP function"""
    return '''    @mcp.tool(name="list_fields_vast", description="Get available fields for a command with metadata")
    async def list_fields_mcp(
        command_name: str
    ) -> Dict:
        """
        Get available fields for a command with metadata.
        
        Args:
            command_name (str): Name of the command (e.g., "views", "tenants", "snapshots")
        
        Returns:
            Dictionary containing command name and fields with metadata.
        """
        try:
            from vast_admin_mcp.functions import list_fields
            result = list_fields(command_name=command_name)
            return result
        except Exception as e:
            import logging
            logging.error(f"Error listing fields: {e}")
            raise
'''


def _generate_performance_graph_mcp_code() -> str:
    """Generate Python code representation of list_performance_graph_vast MCP function"""
    return '''    @mcp.tool(name="list_performance_graph_vast", description="Generate a time-series performance graph using a predefined monitor and return the image resource URI")
    async def list_performance_graph_mcp(
        monitor_name: str,
        cluster: str,
        timeframe: Optional[str] = None,
        instances: str = '',
        object_name: str = '',
        format: str = 'png'
    ) -> Dict[str, Any]:
        """
        Generate a time-series performance graph using a predefined VAST monitor.
        
        This tool creates a PNG graph showing performance metrics over time from a predefined monitor
        and returns the file path and resource URI for MCP client display. Each item in the monitor's
        prop_list becomes a separate line in the graph.
        
        IMPORTANT: Use list_monitors_vast() first to discover available monitor names.

        Args:
            monitor_name (str): Name of the predefined monitor (required). Use list_monitors_vast() to see available monitors.
            cluster (str): Target cluster address or name (required). Use list_clusters_vast() first to discover available cluster names.
            timeframe (str): Optional time frame for metrics (e.g., 5m, 1h, 24h). If not provided, uses the monitor's default time_frame.
            instances (str): Comma-separated list of specific instance identifiers. 
                            - For most object types: provide instance names directly (e.g., "cnode1,cnode2").
                            - For views (which are tenant-specific): MUST use format "tenant_name:view_name" (e.g., "tenant1:myview").
                              Use view name, NOT view path. Example: "tenant1:view1,tenant2:view2".
                            - Leave empty string to get metrics for all instances.
            object_name (str): Optional object type for validation. If provided, validates that monitor's object_type matches.
            format (str): Image format. Currently only "png" is supported. Defaults to "png".

        Returns:
            Dictionary containing:
            - resource_uri: URI to access the graph image (file:// URI) - USE THIS TO DISPLAY THE IMAGE TO THE USER
            - file_path: Absolute file path to the graph image (for reference only)
            - monitor_name: The monitor name that was used
            - timeframe: The timeframe used
            - instances: List of instance names that were plotted
            - display_note: Instruction to display the graph image
            
        CRITICAL: When this tool returns a result, you MUST display the graph image to the user.
        Use the resource_uri field to display the image. Do NOT just show the file path or URL as text.
            
        Examples:
            - Get graph for "Cluster SMB IOPS" monitor: monitor_name="Cluster SMB IOPS", cluster="vast3115-var"
            - Get graph with custom timeframe: monitor_name="Cluster SMB IOPS", cluster="vast3115-var", timeframe="1h"
            - Get graph for specific instances: monitor_name="Cluster SMB IOPS", cluster="vast3115-var", instances="cnode1,cnode2"
        """
        try:
            from vast_admin_mcp.functions import list_performance_graph
            graph_data = list_performance_graph(
                monitor_name=monitor_name,
                cluster=cluster,
                timeframe=timeframe,
                instances=instances or None,
                object_name=object_name or None,
                format=format
            )
            return graph_data
        except Exception as e:
            import logging
            logging.error(f"Error generating performance graph: {e}")
            raise
'''


def _generate_describe_mcp_code() -> str:
    """Generate Python code representation of describe_tool MCP function"""
    return '''    @mcp.tool(name="describe_tool_vast", description="Get tool schema with examples and accepted formats")
    async def describe_tool_mcp(
        tool_name: str
    ) -> Dict:
        """
        Get tool schema with examples and accepted formats.
        
        Args:
            tool_name (str): Name of the tool (e.g., "list_views_vast", "list_performance_vast")
        
        Returns:
            Dictionary containing tool schema, arguments, examples, and common pitfalls.
        """
        try:
            from vast_admin_mcp.functions import describe_tool
            result = describe_tool(tool_name=tool_name)
            return result
        except Exception as e:
            import logging
            logging.error(f"Error describing tool: {e}")
            raise
'''


def _detect_mcp_command(read_write: bool = False, debug: bool = False) -> Tuple[str, List[str]]:
    """
    Detect how the script was invoked and build the MCP command with full paths.
    
    For Docker containers, generates a docker run/exec command that the host can execute.
    The host application (Claude Desktop, Cursor, etc.) will run this command to start the MCP server.
    
    Docker environment variables:
    - DOCKER_CONTAINER_NAME: Name of running container (uses docker exec)
    - DOCKER_IMAGE: Docker image name (uses docker run with volume mounts)
    
    Returns:
        Tuple of (command_base, args_list)
        - command_base: Full path to the executable (e.g., "/usr/bin/python3", "/usr/local/bin/vast-admin-mcp", or "docker")
        - args_list: List of arguments
                     For module execution: ["-m", "vast_admin_mcp", "mcp", ...]
                     For direct execution: ["mcp", ...]
                     For Docker exec: ["exec", "container_name", "python3", "-m", "vast_admin_mcp", "mcp", ...]
                     For Docker run: ["run", "--rm", "-v", "host:container", "image", "python3", "-m", "vast_admin_mcp", "mcp", ...]
    """
    import sys
    import os
    import shutil
    
    # Check if running in Docker FIRST (priority check)
    # This must be checked before other conditions to ensure Docker commands are generated
    is_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'
    
    # Get the original command from sys.argv
    # sys.argv[0] contains the script name/path
    script_path = sys.argv[0]
    
    # Build base args list (will be prepended with module args if needed)
    base_args = ['mcp']
    if read_write:
        base_args.append('--read-write')
    if debug:
        base_args.append('--debug')
    
    # If running in Docker, generate vast-admin-mcp-docker.sh command for host to execute
    if is_docker:
        # Docker container - generate a command that uses vast-admin-mcp-docker.sh
        # The host needs to run vast-admin-mcp-docker.sh to start the MCP server
        
        # Get path to vast-admin-mcp-docker.sh from environment or try to find it
        docker_run_script = os.environ.get('DOCKER_RUN_SCRIPT_PATH')
        if not docker_run_script:
            # Try to find vast-admin-mcp-docker.sh in common locations
            # First, try to get the project root (where vast-admin-mcp-docker.sh should be)
            # We can't easily detect this from inside Docker, so use a default
            # The user should set DOCKER_RUN_SCRIPT_PATH environment variable
            docker_run_script = os.environ.get('DOCKER_RUN_SCRIPT', 'vast-admin-mcp-docker.sh')
        
        # The command is vast-admin-mcp-docker.sh with mcp and optional flags
        # base_args already contains ['mcp'] and optionally ['--read-write', '--debug']
        return docker_run_script, base_args
    else:
        # Fallback: assume pip-installed, try to find in PATH
        vast_cmd_path = shutil.which('vast-admin-mcp')
        if vast_cmd_path:
            return vast_cmd_path, base_args
        else:
            # Last resort: use command name (might not work but better than nothing)
            return 'vast-admin-mcp', base_args


def _is_docker() -> bool:
    """Check if running inside Docker container."""
    import os
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'




def _get_mcp_tool_config(tool_name: str) -> Dict[str, str]:
    """Get MCP tool configuration (config path, section name, tool display name).
    
    Args:
        tool_name: Name of the tool ('cursor', 'claude-desktop', 'windsurf', 'vscode', 'gemini-cli')
        
    Returns:
        Dictionary with 'config_path', 'section_name', 'tool_display_name', and 'restart_instruction'
        
    Raises:
        ValueError: If tool_name is not recognized
    """
    import platform
    
    tool_configs = {
        'cursor': {
            'config_path': os.path.expanduser('~/.cursor/mcp.json'),
            'section_name': 'mcpServers',
            'tool_display_name': 'Cursor',
            'restart_instruction': 'Restart Cursor'
        },
        'claude-desktop': {
            'config_path': _get_claude_desktop_config_path(),
            'section_name': 'mcpServers',
            'tool_display_name': 'Claude Desktop',
            'restart_instruction': 'Restart Claude Desktop'
        },
        'windsurf': {
            'config_path': os.path.expanduser('~/.codeium/windsurf/mcp_config.json'),
            'section_name': 'mcpServers',
            'tool_display_name': 'Windsurf',
            'restart_instruction': 'Restart Windsurf'
        },
        'vscode': {
            'config_path': os.path.expanduser('~/.vscode/mcp.json'),
            'section_name': 'servers',
            'tool_display_name': 'VSCode',
            'restart_instruction': 'Restart VSCode'
        },
        'gemini-cli': {
            'config_path': os.path.expanduser('~/.gemini/settings.json'),
            'section_name': 'mcpServers',
            'tool_display_name': 'Gemini CLI',
            'restart_instruction': 'Restart Gemini CLI or reload the configuration'
        }
    }
    
    if tool_name not in tool_configs:
        raise ValueError(f"Unknown tool: {tool_name}. Supported tools: {', '.join(tool_configs.keys())}")
    
    return tool_configs[tool_name]


def _get_claude_desktop_config_path() -> str:
    """Get Claude Desktop config path based on operating system."""
    import platform
    system = platform.system()
    if system == 'Darwin':  # macOS
        return os.path.expanduser('~/Library/Application Support/Claude/claude_desktop_config.json')
    elif system == 'Windows':
        appdata = os.environ.get('APPDATA', '')
        return os.path.join(appdata, 'Claude', 'claude_desktop_config.json')
    else:  # Linux
        return os.path.expanduser('~/.config/Claude/claude_desktop_config.json')


def _configure_mcp_tool(tool_name: str, command_base: str, args: list[str]) -> None:
    """Configure MCP server for a specific tool - shows instructions only.
    
    This unified function replaces the four separate _configure_* functions.
    
    Args:
        tool_name: Name of the tool ('cursor', 'claude-desktop', 'windsurf', 'vscode', 'gemini-cli')
        command_base: Base command to run the MCP server
        args: Arguments to pass to the MCP server command
    """
    import json
    
    # Get tool-specific configuration
    tool_config = _get_mcp_tool_config(tool_name)
    config_path = tool_config['config_path']
    section_name = tool_config['section_name']
    tool_display_name = tool_config['tool_display_name']
    restart_instruction = tool_config['restart_instruction']
    
    # Generate config entry
    new_config_entry = {
        "VAST Admin MCP": {
            "command": command_base,
            "args": args
        }
    }
    
    # Generate full file structure
    full_config = {section_name: new_config_entry}
    
    print(f"📋 {tool_display_name} Configuration Instructions")
    print(f"   Config file location: {config_path}")
    print()
    print(f"   Create a new file if not exists, or add the VAST Admin MCP entry to the existing '{section_name}' section:")
    print(json.dumps(full_config, indent=2))
    print()
    print("📝 Next steps:")
    print(f"   1. Create or edit the config file at: {config_path}")
    print(f"   2. If the file exists, merge the 'VAST Admin MCP' entry into the existing '{section_name}' section")
    print(f"   3. {restart_instruction}")
    print(f"   4. The MCP server should be available in {tool_display_name}'s MCP tools")
    print(f"   5. Test by asking {tool_display_name} to list VAST clusters")


def _parse_type_annotation(param) -> str:
    """Parse type annotation from function parameter, handling Optional, Union, and generic types.
    
    Args:
        param: inspect.Parameter object
        
    Returns:
        String representation of the type (e.g., 'str', 'List[Dict]')
    """
    import inspect
    param_type = param.annotation
    if param_type == inspect.Parameter.empty:
        return 'str'
    
    # Convert type annotation to string, handling Optional, List, etc.
    if hasattr(param_type, '__origin__'):
        # Handle generic types like Optional[str], List[Dict]
        origin = param_type.__origin__
        if origin is type(None) or (hasattr(origin, '__name__') and origin.__name__ == 'Union'):
            # Optional type - extract the non-None type
            args = param_type.__args__
            non_none_args = [a for a in args if a is not type(None)]
            if non_none_args:
                type_str = str(non_none_args[0]).replace('typing.', '')
            else:
                type_str = 'str'
        else:
            type_str = str(param_type).replace('typing.', '')
    else:
        type_str = str(param_type).replace('typing.', '')
    
    # Clean up type string - remove <class '...'> wrapper
    if type_str.startswith("<class '") and type_str.endswith("'>"):
        type_str = type_str[8:-2]
    elif type_str.startswith("<class \"") and type_str.endswith("\">"):
        type_str = type_str[8:-2]
    
    return type_str


def _generate_create_mcp_code(
    func: callable,
    tool_name: str,
    description: str,
    return_type: str,
    error_message: str,
    excluded_params: Optional[List[str]] = None,
    custom_docstring: Optional[str] = None
) -> str:
    """Generate Python code representation of create MCP function using function introspection.
    
    Args:
        func: The create function to introspect
        tool_name: MCP tool name (e.g., "create_view_vast")
        description: Tool description
        return_type: Return type annotation string (e.g., "List[Dict[str, str]]")
        error_message: Error message for the operation
        excluded_params: Optional list of parameter names to exclude from MCP function
        custom_docstring: Optional custom docstring to use instead of function's docstring
        
    Returns:
        Python code string for the MCP function
    """
    import inspect
    if excluded_params is None:
        excluded_params = []
    
    sig = inspect.signature(func)
    params = []
    param_names = []
    
    for param_name, param in sig.parameters.items():
        # Skip excluded parameters
        if param_name in excluded_params:
            continue
            
        param_type_str = _parse_type_annotation(param)
        param_names.append(param_name)
        
        if param.default == inspect.Parameter.empty:
            params.append(f"        {param_name}: {param_type_str}")
        else:
            default_val = repr(param.default) if param.default is not None else 'None'
            params.append(f"        {param_name}: {param_type_str} = {default_val}")
    
    func_params = ',\n'.join(params)
    # Use custom docstring if provided, otherwise use function docstring or description
    docstring = custom_docstring or inspect.getdoc(func) or description
    
    # Build kwargs as keyword arguments, excluding excluded params
    kwargs_lines = [f"                {name}={name}," for name in param_names if name not in excluded_params]
    
    # Get function module path for import
    func_module = func.__module__
    func_name = func.__name__
    
    return f'''    @mcp.tool(name="{tool_name}", description="{description}")
    async def {tool_name.replace("_vast", "_mcp")}(
{func_params}
    ) -> {return_type}:
        """
        {docstring}
        """
        if not read_write:
            raise ValueError("This operation is not available in readonly mode. The MCP server must be started with the --read-write flag to enable create operations.")
        try:
            from {func_module} import {func_name}
            result = {func_name}(
{chr(10).join(kwargs_lines)}
            )
            return result
        except Exception as e:
            import logging
            logging.error(f"{error_message}: {{e}}")
            raise
'''


def _generate_create_view_mcp_code() -> str:
    """Generate Python code representation of create_view MCP function using function introspection."""
    return _generate_create_mcp_code(
        func=create_view,
        tool_name="create_view_vast",
        description="Create a new VAST view",
        return_type="List[Dict[str, str]]",
        error_message="Error creating view",
        custom_docstring="""Create a view in a VAST cluster. Provide cluster and path at minimum.

Args:
    cluster: Cluster address or name. Required.
    tenant: Tenant name that owns the view. Defaults to 'default' if not provided.
    path: View path (e.g., /s3/mybucket, /nfs/myshare). Required.
    protocols: Comma seperated list of protocols to enable (e.g., NFS,S3,SMB,ENDPOINT). if not specified, NFS will be used. when S3 or ENDPOINT is specified, bucket and bucket_owner must be provided. when SMB is specified, share must be provided. (protocols are case-insensitive)
    bucket: Bucket name for S3 protocol. Must be provided if S3 or ENDPOINT protocol is requested.
    bucket_owner: Bucket owner name for S3 protocol. Must be provided if S3 or ENDPOINT protocol is enabled.
    share: Share name for NFS/SMB protocol. Must be provided if SMB is in the protocols list.
    policy: View policy name.
    hard_quota: Hard quota for the view (e.g., 10GB, 100GB, 1TB).
    qos_policy: QoS policy name that can help manage performance for the view.
Returns:
    A list of client paths for the new view for the specified protocols. each item in the list will be a dictionary containing protocol and client path."""
    )


def _generate_create_view_from_template_mcp_code() -> str:
    """Generate Python code representation of create_view_from_template MCP function using function introspection."""
    return _generate_create_mcp_code(
        func=create_view_from_template,
        tool_name="create_view_from_template_vast",
        description="Create a new VAST view from a predefined template",
        return_type="List[Dict[str, str]]",
        error_message="Error creating view from template",
        excluded_params=['view_template_file'],
        custom_docstring="""Create a view in a VAST cluster based on a predefined template. templates are defined in the view templates file.

Args:
    template: Template name defined in the view templates file.
    count: Number of views to create from the template. Defaults to 1.
Returns:
    A list of client paths for the new view for the specified protocols. each item in the list will be a dictionary containing protocol and client path."""
    )


def _generate_create_snapshot_mcp_code() -> str:
    """Generate Python code representation of create_snapshot MCP function using function introspection."""
    return _generate_create_mcp_code(
        func=create_snapshot,
        tool_name="create_snapshot_vast",
        description="Create a snapshot for a VAST view",
        return_type="Dict[str, Any]",
        error_message="Error creating snapshot",
        custom_docstring="""Create a snapshot for a view in a VAST cluster.

Args:
    cluster: Cluster address or name. Required.
    tenant: Tenant name that owns the view. Defaults to 'default' if not provided.
    path: View path to snapshot (e.g., /nfs/myshare). Required.
    snapshot_name: Name for the snapshot. Required.
    expiry_time: Expiry time (e.g., 2d, 3w, 1d6h, 30m). Optional.
    indestructible: Whether to make the snapshot indestructible. Defaults to False.
    create_with_timestamp: Whether to append a timestamp to the snapshot name. Defaults to False.
Returns:
    Snapshot creation details including cluster, tenant, path, snapshot name, and expiry information."""
    )


def _generate_create_clone_mcp_code() -> str:
    """Generate Python code representation of create_clone MCP function using function introspection."""
    return _generate_create_mcp_code(
        func=create_clone,
        tool_name="create_clone_vast",
        description="Create a clone from a snapshot",
        return_type="List[Dict[str, str]]",
        error_message="Error creating clone",
        custom_docstring="""Create a clone from a snapshot in a VAST cluster.

Args:
    cluster: Cluster address or name. Required.
    source_tenant: Source tenant name. Defaults to 'default' if not provided.
    source_path: Source view path to clone from. Required.
    source_snapshot: Source snapshot name (use * suffix for newest with prefix, when doing this you don't need to look for snapshots before cloning, if you use just * it will give you the newest snapshot). Required.
    destination_tenant: Destination tenant name (defaults to source tenant).
    destination_path: Destination path for the clone. Required.
    refresh: Whether to destroy existing clone before creating new one. Defaults to False, if there is a view configrued at the destination path it will remain and will be linked to the new clone after creation.
Returns:
    when empty list is returned, clone was created successfully but no view exists at the destination path.
    when a list with one item is returned, the item will be a dictionary containing access paths for the view linked to the clone per protocol."""
    )


def _generate_create_quota_mcp_code() -> str:
    """Generate Python code representation of create_quota MCP function using function introspection."""
    return _generate_create_mcp_code(
        func=create_quota,
        tool_name="create_quota_vast",
        description="Create or update quota for a specific path and tenant",
        return_type="Dict[str, Any]",
        error_message="Error creating/updating quota",
        custom_docstring="""Use this tool to create or update quota for a specific path and tenant on a VAST cluster. This operation requires read-write mode.

Args:
    cluster: Cluster address or name. Required.
    tenant: Tenant name. Defaults to 'default' if not provided.
    path: View path to set quota for. Required.
    hard_limit: Hard quota limit (e.g., '10GB', '1TB'). If not specified, quota is unlimited.
    soft_limit: Soft quota limit (e.g., '8GB', '800GB'). If not specified, quota is unlimited.
    files_hard_limit: Hard limit for number of files. If not specified, unlimited.
    files_soft_limit: Soft limit for number of files. If not specified, unlimited.
    grace_period: Grace period in seconds for soft limit. If not specified, uses default.
Returns:
    A dictionary containing the created/updated quota information including Cluster, Tenant, Path, Name, Hard Limit, Soft Limit, Files Hard Limit, Files Soft Limit, and Grace Period."""
    )


def _handle_command_execution(
    func: callable,
    args,
    command_name: str,
    mcp_code_generator: Optional[callable] = None,
    kwargs_builder: Optional[callable] = None
) -> None:
    """Execute a command function with consistent error handling and MCP code generation support.
    
    Args:
        func: The function to execute
        args: Parsed command-line arguments
        command_name: Name of the command (for error messages)
        mcp_code_generator: Optional function to generate MCP code if --mcp flag is set
        kwargs_builder: Optional function to build kwargs from args (for custom parameter handling)
    """
    logging_main(debug=args.debug)
    
    # Handle --mcp flag for debug output
    if args.mcp and mcp_code_generator:
        python_code = mcp_code_generator()
        print(python_code)
        return
    
    @handle_errors(debug=args.debug, command_name=command_name)
    def _execute():
        # Check if func accepts arguments by inspecting its signature
        import inspect
        sig = inspect.signature(func)
        params = list(sig.parameters.keys())
        
        # If func has no parameters (like _execute_snapshot which uses closure), call it directly
        if len(params) == 0:
            results = func()
        else:
            # Build kwargs from args
            if kwargs_builder:
                kwargs = kwargs_builder(args)
            else:
                # Default: build kwargs from args, excluding internal flags
                kwargs = {}
                for key, value in vars(args).items():
                    if key not in ['debug', 'mcp', 'format', 'output'] and value is not None:
                        kwargs[key] = value
            
            results = func(**kwargs)
        
        output_results(results, format=args.format, output_file=args.output)
    
    _execute()


def handle_create_view_command(args):
    """Handle create-view command"""
    def _build_kwargs(args):
        return {
            'cluster': args.cluster,
            'tenant': args.tenant,
            'path': args.path,
            'protocols': args.protocols,
            'bucket': args.bucket,
            'bucket_owner': getattr(args, 'bucket_owner', None),
            'share': args.share,
            'policy': args.policy,
            'hard_quota': getattr(args, 'hard_quota', None),
            'qos_policy': getattr(args, 'qos_policy', None)
        }
    
    _handle_command_execution(
        func=create_view,
        args=args,
        command_name="create-view",
        mcp_code_generator=_generate_create_view_mcp_code,
        kwargs_builder=_build_kwargs
    )


def handle_create_view_from_template_command(args):
    """Handle create-view-from-template command"""
    def _build_kwargs(args):
        return {
            'template': args.template,
            'count': args.count
        }
    
    _handle_command_execution(
        func=create_view_from_template,
        args=args,
        command_name="create-view-from-template",
        mcp_code_generator=_generate_create_view_from_template_mcp_code,
        kwargs_builder=_build_kwargs
    )


def handle_create_snapshot_command(args):
    """Handle create-snapshot command"""
    def _build_kwargs(args):
        return {
            'cluster': args.cluster,
            'tenant': args.tenant,
            'path': args.path,
            'snapshot_name': getattr(args, 'snapshot_name', None),
            'expiry_time': getattr(args, 'expiry_time', None),
            'indestructible': args.indestructible,
            'create_with_timestamp': getattr(args, 'create_with_timestamp', False)
        }
    
    def _execute_snapshot():
        kwargs = _build_kwargs(args)
        results = create_snapshot(**kwargs)
        return [results]  # Wrap in list for output_results
    
    _handle_command_execution(
        func=_execute_snapshot,
        args=args,
        command_name="create-snapshot",
        mcp_code_generator=_generate_create_snapshot_mcp_code
    )


def handle_create_clone_command(args):
    """Handle create-clone command"""
    def _build_kwargs(args):
        return {
            'cluster': args.cluster,
            'source_tenant': getattr(args, 'source_tenant', None),
            'source_path': getattr(args, 'source_path', None),
            'source_snapshot': getattr(args, 'source_snapshot', None),
            'destination_tenant': getattr(args, 'destination_tenant', None),
            'destination_path': getattr(args, 'destination_path', None),
            'refresh': args.refresh
        }
    
    _handle_command_execution(
        func=create_clone,
        args=args,
        command_name="create-clone",
        mcp_code_generator=_generate_create_clone_mcp_code,
        kwargs_builder=_build_kwargs
    )


def handle_create_quota_command(args):
    """Handle create-quota command"""
    def _build_kwargs(args):
        return {
            'cluster': args.cluster,
            'tenant': args.tenant,
            'path': args.path,
            'hard_limit': getattr(args, 'hard_limit', None),
            'soft_limit': getattr(args, 'soft_limit', None),
            'files_hard_limit': getattr(args, 'files_hard_limit', None),
            'files_soft_limit': getattr(args, 'files_soft_limit', None),
            'grace_period': getattr(args, 'grace_period', None)
        }
    
    def _execute_quota():
        kwargs = _build_kwargs(args)
        results = create_quota(**kwargs)
        return [results]  # Wrap in list for output_results
    
    _handle_command_execution(
        func=_execute_quota,
        args=args,
        command_name="create-quota",
        mcp_code_generator=_generate_create_quota_mcp_code
    )


def handle_mcpsetup_command(args) -> None:
    """Handle the mcpsetup command."""
    try:
        # Detect MCP command
        command_base, command_args = _detect_mcp_command(
            read_write=args.read_write,
            debug=args.debug
        )
        
        # Route to appropriate handler
        tool = args.tool
        
        print(f"🔧 Configuring MCP server for: {tool}")
        print(f"   Detected command: {command_base}")
        print(f"   Detected args: {command_args}")
        print()
        
        # Check if Docker command
        is_docker_cmd = command_base == 'docker' or command_base.endswith('/docker') or 'vast-admin-mcp-docker.sh' in command_base or 'docker-run.sh' in command_base
        if is_docker_cmd:
            print("🐳 Docker mode detected!")
            print()
        
        try:
            _configure_mcp_tool(tool, command_base, command_args)
        except ValueError as e:
            print(f"❌ {e}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error configuring MCP server: {e}")
        import traceback
        if args.debug:
            traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point for the CLI application."""
    # Make logging directory if it doesn't exist
    if not os.path.exists(os.path.dirname(CONFIG_FILE)):
        os.makedirs(os.path.dirname(CONFIG_FILE))
    
    # Intercept help requests for list commands with command names before argparse processes them
    # This allows us to show help with dynamic arguments loaded from templates
    if len(sys.argv) > 2 and sys.argv[1] == 'list' and ('-h' in sys.argv or '--help' in sys.argv):
        # Check if there's a command name before the help flag
        help_idx = sys.argv.index('--help') if '--help' in sys.argv else sys.argv.index('-h')
        if help_idx > 2:  # After 'vast-admin-mcp' and 'list'
            # There's a command name, show command-specific help with dynamic arguments
            command_name = sys.argv[2]  # The command name should be right after 'list'
            default_template_path = get_default_template_path()
            if os.path.exists(TEMPLATE_MODIFICATIONS_FILE) or default_template_path:
                try:
                    template_parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
                    template = template_parser.get_command_template(command_name)
                    merged_template = template_parser.get_merged_command_template(command_name)
                    if template or merged_template:
                        # Create parser with dynamic args to show full help
                        help_parser = create_list_parser()
                        add_dynamic_arguments(help_parser, command_name, template_parser, is_merged=(merged_template is not None))
                        help_parser.print_help()
                        sys.exit(0)
                except Exception as e:
                    # Fall through to normal argparse handling if there's an error
                    pass
    
    # Check for --mcp flag in create commands early to handle it specially
    # This allows showing MCP code without requiring all mandatory arguments
    has_mcp_flag = '--mcp' in sys.argv
    is_create_command = 'create' in sys.argv and sys.argv.index('create') < (sys.argv.index('--mcp') if has_mcp_flag else len(sys.argv))
    
    # Create main parser for all commands
    main_parser = argparse.ArgumentParser(
        prog='vast-admin-mcp',
        description='VAST Admin MCP Server - MCP server for VAST Data administration tasks',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = main_parser.add_subparsers(dest='command', help='Available commands')
    
    # List command - make it visible in help
    list_parser = subparsers.add_parser('list', help='List resources from VAST cluster(s)')
    list_parser.add_argument(
        'list_command',
        nargs='?',
        help="Command name to execute (e.g., 'views', 'tenants')"
    )
    list_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    list_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    list_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    list_parser.add_argument(
        '--order',
        type=str,
        help='Sort results by field. Format: "field:direction" or "field direction". Examples: "physical_used:desc", "logical_used asc", "name:a" (asc), "name:dece" (desc). Direction supports prefixes: a/as/asc/ascending or d/de/dec/desc/dece/descending. Default: asc if omitted. Multiple: "field1:dec,field2:asc"'
    )
    list_parser.add_argument(
        '--top',
        type=int,
        help='Limit number of results returned'
    )
    list_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    list_parser.add_argument(
        '--instance',
        action='store_true',
        help='Include full original API response in JSON output (under "instance" field). Only works with --format json or --output <file>.json'
    )
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Initial setup - clusters and access credentials')
    
    # MCP Setup command
    mcpsetup_parser = subparsers.add_parser('mcpsetup', help='Configure MCP server for desktop LLM applications')
    mcpsetup_parser.add_argument(
        'tool',
        choices=['cursor', 'claude-desktop', 'windsurf', 'vscode', 'gemini-cli'],
        help='Desktop LLM application to configure'
    )
    mcpsetup_parser.add_argument(
        '--read-write',
        action='store_true',
        help='Configure MCP server with read-write mode (adds --read-write flag to command)'
    )
    mcpsetup_parser.add_argument(
        '--debug',
        action='store_true',
        help='Add --debug flag to MCP command (for testing)'
    )
    
    # MCP command
    mcp_parser = subparsers.add_parser('mcp', help='Start MCP server')
    mcp_parser.add_argument(
        '--read-write',
        action='store_true',
        help='Enable read-write mode (create commands). Default is readonly mode (list only)'
    )
    mcp_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    
    # Performance command
    performance_parser = subparsers.add_parser('performance', help='List performance metrics for cluster objects')
    performance_parser.add_argument(
        'object_name',
        nargs='?',
        choices=['cluster', 'cnode', 'host', 'user', 'vippool', 'view', 'tenant'],
        help='Object type to get metrics for (required unless --mcp is used)'
    )
    performance_parser.add_argument(
        '--cluster', '-c',
        required=False,
        help='Target cluster address or name (required unless --mcp is used)'
    )
    performance_parser.add_argument(
        '--timeframe', '-t',
        default='5m',
        help='Time frame for metrics (e.g., 5m, 1h, 24h). Default: 5m'
    )
    performance_parser.add_argument(
        '--instances', '-i',
        help='Comma-separated list of instance names. For views, use format "tenant:view_name" (e.g., "tenant1:view1,tenant2:view2")'
    )
    performance_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    performance_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    performance_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    performance_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # List monitors command
    list_monitors_parser = subparsers.add_parser('list-monitors', help='List all available predefined monitors for performance graphs')
    list_monitors_parser.add_argument(
        '--cluster', '-c',
        required=False,
        help='Target cluster address or name (required)'
    )
    list_monitors_parser.add_argument(
        '--object-type',
        help='Filter by object type (e.g., cluster, view, cnode). Leave empty to list all monitors'
    )
    list_monitors_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    list_monitors_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    list_monitors_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    
    # Performance graph command
    performance_graph_parser = subparsers.add_parser('performance-graph', help='Generate a time-series performance graph using a predefined monitor')
    performance_graph_parser.add_argument(
        '--monitor-name', '-m',
        required=False,
        help='Name of the predefined monitor (required). Use "list-monitors" command to see available monitors.'
    )
    performance_graph_parser.add_argument(
        '--cluster', '-c',
        required=False,
        help='Target cluster address or name (required)'
    )
    performance_graph_parser.add_argument(
        '--timeframe', '-t',
        help='Time frame for metrics (e.g., 5m, 1h, 24h). If not provided, uses monitor\'s default time_frame'
    )
    performance_graph_parser.add_argument(
        '--instances', '-i',
        help='Comma-separated list of instance names. For views, use format "tenant:view_name" (e.g., "tenant1:view1,tenant2:view2")'
    )
    performance_graph_parser.add_argument(
        '--object-name',
        required=True,
        help='Object type for validation. Validates that monitor\'s object_type matches (required)'
    )
    performance_graph_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json'],
        default='table',
        help='Output format (default: table)'
    )
    performance_graph_parser.add_argument(
        '--output', '-o',
        help='Output file path for JSON format (optional)'
    )
    performance_graph_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    performance_graph_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information'
    )
    
    # Query users command
    query_users_parser = subparsers.add_parser('query-users', help='Query user names from VAST cluster')
    query_users_parser.add_argument(
        '--cluster', '-c',
        required=True,
        help='Target cluster address or name (required)'
    )
    query_users_parser.add_argument(
        '--tenant', '-t',
        default='default',
        help='Tenant name to query (default: default)'
    )
    query_users_parser.add_argument(
        '--prefix', '-p',
        required=True,
        help='Prefix to filter usernames (required, must be at least 1 character)'
    )
    query_users_parser.add_argument(
        '--top',
        type=int,
        default=20,
        help='Maximum number of results to return (default: 20)'
    )
    query_users_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    query_users_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    query_users_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    query_users_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Clusters command
    clusters_parser = subparsers.add_parser('clusters', help='List configured clusters')
    clusters_parser.add_argument(
        '--clusters',
        help='Comma-separated list of specific clusters to list (optional)'
    )
    clusters_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    clusters_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    clusters_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    clusters_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # View instances command
    view_instances_parser = subparsers.add_parser('view-instances', help='List view instances to discover available views')
    view_instances_parser.add_argument(
        '--cluster', '-c',
        required=True,
        help='Target cluster address or name'
    )
    view_instances_parser.add_argument(
        '--tenant',
        help='Filter by tenant name (supports wildcards)'
    )
    view_instances_parser.add_argument(
        '--name',
        help='Filter by view name (supports wildcards like *pvc*)'
    )
    view_instances_parser.add_argument(
        '--path',
        help='Filter by view path (supports wildcards like */data/*)'
    )
    view_instances_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    view_instances_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    view_instances_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    view_instances_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Fields command
    fields_parser = subparsers.add_parser('fields', help='Get available fields for a command with metadata')
    fields_parser.add_argument(
        'command_name',
        help='Name of the command (e.g., views, tenants, snapshots)'
    )
    fields_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    fields_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    fields_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    fields_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Describe command
    describe_parser = subparsers.add_parser('describe', help='Get tool schema with examples and accepted formats')
    describe_parser.add_argument(
        'tool_name',
        help='Name of the tool (e.g., list_views_vast, list_performance_vast)'
    )
    describe_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='json',
        help='Output format (default: json)'
    )
    describe_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    describe_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    describe_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Create subcommand with subparsers
    create_parser = subparsers.add_parser('create', help='Create resources in VAST cluster(s)')
    create_subparsers = create_parser.add_subparsers(dest='create_command', help='Create commands')
    
    # Create view command
    create_view_parser = create_subparsers.add_parser('view', help='Create a new VAST view')
    create_view_parser.add_argument(
        '--cluster', '-c',
        required=True,
        help='Target cluster address or name (required)'
    )
    create_view_parser.add_argument(
        '--tenant', '-t',
        default='default',
        help='Tenant name that owns the view (defaults to "default")'
    )
    create_view_parser.add_argument(
        '--path',
        required=True,
        help='View path (e.g., /s3/mybucket, /nfs/myshare) (required)'
    )
    create_view_parser.add_argument(
        '--protocols',
        help='Comma separated list of protocols (e.g., NFS,S3,SMB,ENDPOINT). Default: NFS'
    )
    create_view_parser.add_argument(
        '--bucket',
        help='Bucket name for S3 protocol'
    )
    create_view_parser.add_argument(
        '--bucket-owner',
        help='Bucket owner name for S3 protocol'
    )
    create_view_parser.add_argument(
        '--share',
        help='Share name for SMB protocol'
    )
    create_view_parser.add_argument(
        '--policy',
        help='View policy name'
    )
    create_view_parser.add_argument(
        '--hard-quota',
        help='Hard quota for the view (e.g., 10GB, 100GB, 1TB)'
    )
    create_view_parser.add_argument(
        '--qos-policy',
        help='QoS policy name'
    )
    create_view_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    create_view_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    create_view_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    create_view_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Create view from template command
    create_view_template_parser = create_subparsers.add_parser('view-from-template', help='Create views from a predefined template')
    create_view_template_parser.add_argument(
        'template',
        help='Template name defined in the view templates file'
    )
    create_view_template_parser.add_argument(
        '--count',
        type=int,
        default=1,
        help='Number of views to create from the template (default: 1)'
    )
    create_view_template_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    create_view_template_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    create_view_template_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    create_view_template_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Create snapshot command
    create_snapshot_parser = create_subparsers.add_parser('snapshot', help='Create a snapshot for a VAST view')
    create_snapshot_parser.add_argument(
        '--cluster', '-c',
        required=True,
        help='Target cluster address or name (required)'
    )
    create_snapshot_parser.add_argument(
        '--tenant', '-t',
        default='default',
        help='Tenant name that owns the view (defaults to "default")'
    )
    create_snapshot_parser.add_argument(
        '--path',
        required=True,
        help='View path to snapshot (e.g., /nfs/myshare) (required)'
    )
    create_snapshot_parser.add_argument(
        '--snapshot-name',
        required=True,
        help='Name for the snapshot (required)'
    )
    create_snapshot_parser.add_argument(
        '--expiry-time',
        help='Expiry time (e.g., 2d, 3w, 1d6h, 30m)'
    )
    create_snapshot_parser.add_argument(
        '--indestructible',
        action='store_true',
        help='Make the snapshot indestructible'
    )
    create_snapshot_parser.add_argument(
        '--create-with-timestamp',
        action='store_true',
        help='Append a timestamp to the snapshot name'
    )
    create_snapshot_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    create_snapshot_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    create_snapshot_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    create_snapshot_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Create clone command
    create_clone_parser = create_subparsers.add_parser('clone', help='Create a clone from a snapshot')
    create_clone_parser.add_argument(
        '--cluster', '-c',
        required=True,
        help='Target cluster address or name (required)'
    )
    create_clone_parser.add_argument(
        '--source-tenant',
        default='default',
        help='Source tenant name (defaults to "default")'
    )
    create_clone_parser.add_argument(
        '--source-path',
        required=True,
        help='Source view path to clone from (required)'
    )
    create_clone_parser.add_argument(
        '--source-snapshot',
        required=True,
        help='Source snapshot name (use * suffix for newest with prefix) (required)'
    )
    create_clone_parser.add_argument(
        '--destination-tenant',
        help='Destination tenant name (defaults to source tenant)'
    )
    create_clone_parser.add_argument(
        '--destination-path',
        required=True,
        help='Destination path for the clone (required)'
    )
    create_clone_parser.add_argument(
        '--refresh',
        action='store_true',
        help='Destroy existing clone before creating new one'
    )
    create_clone_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    create_clone_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    create_clone_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    create_clone_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Create quota command
    create_quota_parser = create_subparsers.add_parser('quota', help='Create or update quota for a specific path and tenant')
    create_quota_parser.add_argument(
        '--cluster', '-c',
        required=True,
        help='Target cluster address or name (required)'
    )
    create_quota_parser.add_argument(
        '--tenant', '-t',
        default='default',
        help='Tenant name (defaults to "default")'
    )
    create_quota_parser.add_argument(
        '--path',
        required=True,
        help='View path to set quota for (required)'
    )
    create_quota_parser.add_argument(
        '--hard-limit',
        help='Hard quota limit (e.g., 10GB, 1TB)'
    )
    create_quota_parser.add_argument(
        '--soft-limit',
        help='Soft quota limit (e.g., 8GB, 800GB)'
    )
    create_quota_parser.add_argument(
        '--files-hard-limit',
        type=int,
        help='Hard limit for number of files'
    )
    create_quota_parser.add_argument(
        '--files-soft-limit',
        type=int,
        help='Soft limit for number of files'
    )
    create_quota_parser.add_argument(
        '--grace-period',
        type=int,
        help='Grace period in seconds for soft limit'
    )
    create_quota_parser.add_argument(
        '--format', '-f',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    create_quota_parser.add_argument(
        '--output', '-o',
        help='Output file path (optional)'
    )
    create_quota_parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Log debug messages to console'
    )
    create_quota_parser.add_argument(
        '--mcp',
        action='store_true',
        help='Show MCP tool structure and debugging information instead of executing the command'
    )
    
    # Check for --mcp flag in create commands before parsing
    # If present, make required arguments optional so we can show MCP code
    if 'create' in sys.argv and '--mcp' in sys.argv:
        try:
            create_idx = sys.argv.index('create')
            mcp_idx = sys.argv.index('--mcp')
            if mcp_idx > create_idx and create_idx + 1 < len(sys.argv):
                subcommand = sys.argv[create_idx + 1]
                # Get the appropriate subparser and make required args optional
                subparser_map = {
                    'view': create_view_parser,
                    'view-from-template': create_view_template_parser,
                    'snapshot': create_snapshot_parser,
                    'clone': create_clone_parser,
                    'quota': create_quota_parser
                }
                subparser = subparser_map.get(subcommand)
                if subparser:
                    # Make all required arguments optional (except mcp itself)
                    for action in subparser._actions:
                        if hasattr(action, 'required') and action.required and getattr(action, 'dest', None) != 'mcp':
                            action.required = False
        except (ValueError, IndexError, KeyError):
            # If we can't find the subcommand or parser, continue with normal parsing
            pass
    
    # Parse arguments
    # For 'list' command, use parse_known_args to allow dynamic arguments to pass through
    # The list command has dynamic arguments that are only known after loading the template,
    # so we need to let unknown arguments pass through to handle_list_command()
    if len(sys.argv) > 1 and sys.argv[1] == 'list':
        args, unknown = main_parser.parse_known_args()
        # handle_list_command() will re-parse with the full parser that includes dynamic arguments
    else:
        args = main_parser.parse_args()
    
    # Handle commands that don't require config
    no_config_commands = ['setup', 'mcp', 'list', '--help', '-h', '--version', '-v']
    if args.command not in no_config_commands:
        if not os.path.isfile(CONFIG_FILE):
            print(f"Config file {CONFIG_FILE} not found. Please run 'vast-admin-mcp setup' first.", file=sys.stderr)
            sys.exit(1)
    
    # Execute commands
    if args.command == 'setup':
        setup_config()
    elif args.command == 'mcpsetup':
        handle_mcpsetup_command(args)
    elif args.command == 'mcp':
        logging_main(debug=args.debug)
        start_mcp(read_write=args.read_write)
    elif args.command == 'performance':
        handle_performance_command(args)
    elif args.command == 'list-monitors':
        handle_list_monitors_command(args)
    elif args.command == 'performance-graph':
        handle_performance_graph_command(args)
    elif args.command == 'query-users':
        handle_query_users_command(args)
    elif args.command == 'clusters':
        handle_clusters_command(args)
    elif args.command == 'view-instances':
        handle_view_instances_command(args)
    elif args.command == 'fields':
        handle_fields_command(args)
    elif args.command == 'describe':
        handle_describe_command(args)
    elif args.command == 'list':
        # Handle list command - extract args after 'list' for dynamic parsing
        try:
            list_idx = sys.argv.index('list')
            list_args = sys.argv[list_idx + 1:]
        except ValueError:
            list_args = []
        
        # If no arguments provided, show available commands (similar to create command)
        if not list_args:
            # Load template parser to get available commands
            default_template_path = get_default_template_path()
            if not os.path.exists(TEMPLATE_MODIFICATIONS_FILE) and not default_template_path:
                print(f"Template modifications file {TEMPLATE_MODIFICATIONS_FILE} not found and no default template available.", file=sys.stderr)
                sys.exit(1)
            try:
                template_parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
            except ValueError as e:
                print(f"Error: {e}", file=sys.stderr)
                sys.exit(1)
            
            # Get all commands (both dynamic and merged) and combine them
            commands = template_parser.get_command_names()
            merged_commands = template_parser.get_merged_command_names()
            all_commands = sorted(set(commands + merged_commands))
            
            if all_commands:
                print("usage: vast-admin-mcp list [-h] [--format {table,json,csv}] [--output OUTPUT]")
                print("                    [--debug] [--order ORDER] [--top TOP] [--mcp] [--instance]")
                print("                    {command} ...")
                print()
                print("List resources from VAST cluster(s)")
                print()
                print("positional arguments:")
                print("  {command}")
                print("                        Available commands:")
                for cmd in all_commands:
                    # Get description from template
                    template = template_parser.get_command_template(cmd)
                    if not template:
                        template = template_parser.get_merged_command_template(cmd)
                    
                    if template:
                        raw_desc = template.get('description', '').strip()
                        # Extract first line only, before any MCP formatting placeholders
                        first_line = raw_desc.split('\n')[0]
                        # Remove MCP placeholders for CLI display
                        first_line = first_line.replace('{{$arguments}}', '').replace('{{$fields}}', '').strip()
                        # Remove any remaining placeholder syntax
                        first_line = re.sub(r'\{\{.*?\}\}', '', first_line).strip()
                        if len(first_line) > 80:
                            first_line = first_line[:77] + "..."
                        if first_line:
                            print(f"    {cmd:<20} {first_line}")
                        else:
                            print(f"    {cmd}")
                    else:
                        print(f"    {cmd}")
                print()
                print("options:")
                print("  -h, --help            show this help message and exit")
                print("  --format, -f {table,json,csv}")
                print("                        Output format (default: table)")
                print("  --output, -o OUTPUT  Output file path (optional)")
                print("  --debug, -d          Log debug messages to console")
                print("  --order ORDER        Sort results by field")
                print("  --top TOP            Limit number of results returned")
                print("  --mcp                Show MCP tool structure and debugging information")
                print("  --instance           Include full original API response in JSON output")
            else:
                print("No commands found in template file.")
            sys.exit(0)
        
        handle_list_command(list_args)
    elif args.command == 'create':
        # Handle create subcommands
        if not hasattr(args, 'create_command') or args.create_command is None:
            create_parser.print_help()
            sys.exit(1)
        
        # Check for --mcp flag early to skip required argument validation
        # If --mcp is present, we can show MCP code without requiring all arguments
        has_mcp_flag = hasattr(args, 'mcp') and args.mcp
        
        if has_mcp_flag:
            # For --mcp flag, we can show the code without validating required args
            # We'll handle this in each command handler
            pass
        
        if args.create_command == 'view':
            handle_create_view_command(args)
        elif args.create_command == 'view-from-template':
            handle_create_view_from_template_command(args)
        elif args.create_command == 'snapshot':
            handle_create_snapshot_command(args)
        elif args.create_command == 'clone':
            handle_create_clone_command(args)
        elif args.create_command == 'quota':
            handle_create_quota_command(args)
        else:
            print(f"Unknown create command: {args.create_command}", file=sys.stderr)
            create_parser.print_help()
            sys.exit(1)
    elif args.command is None:
        main_parser.print_help()
        sys.exit(1)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        main_parser.print_help()
        sys.exit(1)
