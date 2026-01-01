"""MCP server tools for vast-admin-mcp."""

import logging
import os
import types
from typing import Optional, List, Dict, Any

from fastmcp import FastMCP

from .functions import (
    list_clusters, list_performance, list_performance_graph, list_monitors, list_dynamic, list_view_instances, list_fields, describe_tool, query_users
)
from .config import TEMPLATE_MODIFICATIONS_FILE, get_default_template_path
from .template_parser import TemplateParser

# Import create functions (only used when read_write=True)
if True:  # Always import, but only register when read_write=True
    try:
        from .create_functions import (
            create_view, create_view_from_template, create_snapshot, create_clone, create_quota
        )
        CREATE_FUNCTIONS_AVAILABLE = True
    except ImportError as e:
        logging.warning(f"Create functions not available: {e}")
        CREATE_FUNCTIONS_AVAILABLE = False
        create_view = None
        create_view_from_template = None
        create_snapshot = None
        create_clone = None
        create_quota = None


def start_mcp(read_write: bool = False):
    
    mode_str = "read-write" if read_write else "readonly"
    logging.info(f"Starting MCP server in {mode_str} mode...")
    mcp = FastMCP("VAST Admin MCP Server")

    @mcp.tool(name="list_clusters_vast", description="Retrieve information about VAST clusters, their status, capacity and usage. IMPORTANT: Call this tool FIRST when you need to query 'all clusters' or discover available cluster names before using other tools like list_views_vast.")
    async def list_clusters_mcp(
        clusters: str = ''
    ) -> list:
        """
        Use this tool to retrieve a list of all clusters monitored under the same VMS.
        
        IMPORTANT WORKFLOW: When a user asks to query "all clusters" or "all available clusters", 
        you MUST call this tool FIRST to discover the list of available cluster names, then use 
        those cluster names (comma-separated) as the 'cluster' argument in other tools like 
        list_views_vast, list_tenants_vast, etc.

        Args:
            clusters (str): Comma-separated list of non-default cluster names, same credentials as the default credentials will be used. Defaults to None, will use the clusters from config.

        Returns:
            A list of clusters. Each item in the list will be a dictionary containing details regarding a specific cluster. The 'name' field contains the cluster name to use in other tools.
        """
        try:
            clusters_result = list_clusters(
                clusters=clusters if clusters else None
            )
            return clusters_result
        except Exception as e:
            logging.error(f"Error listing clusters: {e}")
            raise

    @mcp.tool(name="list_performance_vast", description="Retrieve performance metrics for VAST cluster objects (cluster, cnode, host, user, vippool, view, tenant)")
    async def list_performance_mcp(
        object_name: str,
        cluster: str,
        timeframe: Optional[str] = '5m',
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
                              - For "cluster": Returns overall cluster performance metrics (instances parameter is ignored).
                              - For other types: Returns metrics for all instances unless instances parameter is specified.
                              DO NOT include instance names or identifiers here (e.g., use "view" NOT "view-142" or "view_name").
                              If you have a specific instance like "view-142", set object_name="view" and use instances parameter.
            cluster (str): Target cluster address or name (required). Use list_clusters_vast() first to discover available cluster names.
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
            - Get cluster performance metrics: object_name="cluster", cluster="vast3115-var", instances=""
            - Get all view metrics: object_name="view", cluster="vast3115-var", instances=""
            - Get specific view "view-142": object_name="view", cluster="vast3115-var", instances="tenant1:view-142"
            - Get all cnode metrics: object_name="cnode", cluster="vast3115-var", instances=""
            - Get specific cnodes: object_name="cnode", cluster="vast3115-var", instances="cnode1,cnode2"
            - Get tenant metrics: object_name="tenant", cluster="vast3115-var", instances=""
        """
        try:
            performance_data = list_performance(
                object_name=object_name,
                cluster=cluster,
                timeframe=timeframe or '5m',
                instances=instances or None
            )

            return performance_data
        except Exception as e:
            logging.error(f"Error listing performance metrics: {e}")
            raise

    @mcp.tool(name="list_monitors_vast", description="List all available predefined monitors for performance graphs")
    async def list_monitors_mcp(
        cluster: str,
        object_type: str = ''
    ) -> List[Dict]:
        """
        List all available predefined monitors that can be used for performance graphs.
        
        Use this tool to discover available monitors before calling list_performance_graph_vast.
        Each monitor contains a prop_list that defines which metrics will be plotted.

        Args:
            cluster (str): Target cluster address or name (required). Use list_clusters_vast() first to discover available cluster names.
            object_type (str): Optional filter by object type (e.g., "cluster", "view", "cnode"). Leave empty to list all monitors.

        Returns:
            List of monitor dictionaries containing:
            - id: Monitor ID (used internally)
            - name: Monitor name (use this in list_performance_graph_vast)
            - object_type: Object type this monitor applies to
            - prop_list: List of metrics that will be plotted
            - time_frame: Default timeframe for this monitor
            - granularity: Data granularity
            
        Examples:
            - List all monitors: cluster="vast3115-var", object_type=""
            - List cluster monitors only: cluster="vast3115-var", object_type="cluster"
            - List view monitors: cluster="vast3115-var", object_type="view"
        """
        try:
            monitors = list_monitors(
                cluster=cluster,
                object_type=object_type or None
            )
            return monitors
        except Exception as e:
            logging.error(f"Error listing monitors: {e}")
            raise

    @mcp.tool(name="list_performance_graph_vast", description="Generate a time-series performance graph using a predefined monitor and return the image resource URI. IMPORTANT: You MUST display the graph image to the user using the resource_uri - do not just show the URL text.")
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
        
        CRITICAL: When this tool returns a result, you MUST display the graph image to the user.
        Use the resource_uri field to display the image. Do NOT just show the file path or URL as text.
        The resource_uri is a file:// URI that can be used to display the image in the chat interface.
        
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
            - statistics: Dictionary containing performance statistics:
              - instances: Optional list of per-instance statistics (only present when specific instances were queried).
                Each entry contains instance_name and metrics list with min, avg, p95, max, trimmed_mean, unit for each metric.
              - summary: Always present, contains aggregated statistics across all instances with same structure as instances metrics.
            
        Examples:
            - Get graph for "Cluster SMB IOPS" monitor: monitor_name="Cluster SMB IOPS", cluster="vast3115-var"
            - Get graph with custom timeframe: monitor_name="Cluster SMB IOPS", cluster="vast3115-var", timeframe="1h"
            - Get graph for specific instances: monitor_name="Cluster SMB IOPS", cluster="vast3115-var", instances="cnode1,cnode2"
        """
        try:
            graph_data = list_performance_graph(
                monitor_name=monitor_name,
                cluster=cluster,
                timeframe=timeframe,
                instances=instances or None,
                object_name=object_name or None,
                format=format or 'png'
            )
            return graph_data
        except Exception as e:
            logging.error(f"Error generating performance graph: {e}")
            raise

    @mcp.tool(name="list_view_instances_vast", description="List view instances to help discover available views with tenant, name, path, protocols, and bucket information")
    async def list_view_instances_mcp(
        cluster: str,
        tenant: str = '',
        name: str = '',
        path: str = ''
    ) -> list:
        """
        List view instances to help discover available views.
        
        This tool helps LLMs discover available view instances with their paths and tenants.
        It returns a list of views with tenant, name, path, protocols, and has_bucket information.
        
        Args:
            cluster (str): Target cluster address or name (required)
            tenant (str): Filter by tenant name (optional, supports wildcards)
            name (str): Filter by view name (optional, supports wildcards like *pvc*)
            path (str): Filter by view path (optional, supports wildcards like */data/*)
        
        Returns:
            A list of dictionaries, each containing:
            - tenant: Tenant name
            - name: View name
            - path: View path
            - protocols: List of enabled protocols (e.g., ["NFS", "S3"])
            - has_bucket: Boolean indicating if S3 bucket is configured
        """
        try:
            result = list_view_instances(
                cluster=cluster,
                tenant=tenant if tenant else None,
                name=name if name else None,
                path=path if path else None
            )
            return result
        except Exception as e:
            logging.error(f"Error listing view instances: {e}")
            raise

    @mcp.tool(name="list_fields_vast", description="Get available fields for a command with types, units, and sortable/filterable metadata")
    async def list_fields_mcp(
        command_name: str
    ) -> Dict:
        """
        Get available fields for a command with metadata.
        
        This tool helps LLMs understand what fields are available for a command,
        including their types, units, and whether they're sortable or filterable.
        
        Args:
            command_name (str): Name of the command (e.g., "views", "tenants", "snapshots")
        
        Returns:
            Dictionary containing:
            - command: Command name
            - fields: List of field dictionaries, each containing:
              - name: Field name
              - type: Field type (string, capacity, datetime, etc.)
              - unit: Unit for numeric/capacity fields (bytes, iops, etc.)
              - sortable: Whether the field can be used for sorting
              - filterable: Whether the field can be used for filtering
              - api_field: API parameter name (if different from display name)
              - description: Field description
        """
        try:
            result = list_fields(command_name=command_name)
            return result
        except Exception as e:
            logging.error(f"Error listing fields: {e}")
            raise

    @mcp.tool(name="describe_tool_vast", description="Get tool schema with examples, defaults, and accepted formats for any tool")
    async def describe_tool_mcp(
        tool_name: str
    ) -> Dict:
        """
        Get tool schema with examples and accepted formats.
        
        This tool returns comprehensive tool description including arguments with types,
        defaults, formats, examples, common pitfalls, and return structure.
        
        Args:
            tool_name (str): Name of the tool (e.g., "list_views_vast", "list_performance_vast", "list_view_instances_vast", "create_view_vast", "create_snapshot_vast")
        
        Returns:
            Dictionary containing:
            - tool_name: Tool name
            - description: Tool description
            - arguments: List of argument dictionaries with:
              - name: Argument name
              - type: Argument type
              - required: Whether argument is required
              - default: Default value (if any)
              - description: Argument description
              - examples: Example values
              - accepted_formats: Accepted format patterns
              - aliases: List of aliases (if any)
            - return_structure: Description of return value structure
            - examples: Usage examples
            - common_pitfalls: Common mistakes and how to avoid them
        """
        try:
            result = describe_tool(tool_name=tool_name)
            return result
        except Exception as e:
            logging.error(f"Error describing tool: {e}")
            raise

    try:
        @mcp.tool(name="query_users_vast", description="Query user names from VAST cluster using users/names endpoint")
        async def query_users_mcp(
            cluster: str,
            tenant: str = 'default',
            prefix: str = '',
            top: Optional[int] = 20
        ) -> List[Dict]:
            """
            Query user names from VAST cluster using users/names endpoint.
            
            This tool queries users matching a prefix and returns detailed user information
            including UID, GID, groups, origins, and S3 permissions.
            
            Args:
                cluster (str): Target cluster address or name (required). Use list_clusters_vast() first to discover available cluster names.
                tenant (str): Tenant name to query (required, defaults to 'default').
                prefix (str): Prefix to filter usernames (required, must be at least 1 character).
                top (Optional[int]): Maximum number of results to return (optional, defaults to 20, maximum: 50 due to API limit).
            
            Returns:
                List of user dictionaries. Each dictionary contains:
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
                users_data = query_users(
                    cluster=cluster,
                    tenant=tenant or 'default',
                    prefix=prefix,
                    top=top or 20
                )
                return users_data
            except Exception as e:
                logging.error(f"Error querying users: {e}")
                raise
    except Exception as e:
        logging.warning(f"Could not register query_users_vast tool: {e}")
        import traceback
        logging.debug(traceback.format_exc())

    # Dynamically register tools from YAML templates
    default_template_path = get_default_template_path()
    if os.path.exists(TEMPLATE_MODIFICATIONS_FILE) or default_template_path:
        try:
            parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
            all_commands = parser.get_all_commands()
        except Exception as e:
            logging.warning(f"Could not load template parser from {TEMPLATE_MODIFICATIONS_FILE}: {e}")
            parser = None
        
        if parser:
            for command_name in all_commands:
                try:
                    template = parser.get_template(command_name)
                    if not template:
                        continue
                    
                    # Check if MCP tool creation is disabled for this command
                    create_mcp_tool = template.get('create_mcp_tool', True)  # Default to True if not specified
                    if not create_mcp_tool:
                        logging.debug(f"Skipping MCP tool registration for '{command_name}' (create_mcp_tool: false)")
                        continue
                    
                    # Get description
                    description = parser.get_description(command_name) or f"Retrieve {command_name} from VAST cluster"
                    
                    # Get arguments
                    args_config = parser.get_arguments(command_name)
                    
                    # Add order and top arguments for all dynamic tools
                    args_config.append({
                        'name': 'order',
                        'type': 'str',
                        'mandatory': False,
                        'description': 'Sort results by field. Format: "field_name:direction" using colon separator. IMPORTANT: Use underscores for field names (e.g., "logical_used" not "logical used"). Examples: "physical_used:desc", "logical_used:asc", "name:desc". Direction: Use a/as/asc/ascending for ascending, or d/de/desc/descending for descending. Default is asc if omitted. Multiple fields: "field1:desc,field2:asc".'
                    })
                    args_config.append({
                        'name': 'top',
                        'type': 'int',
                        'mandatory': False,
                        'description': 'Limit output to top N results'
                    })
                    
                    # Create tool name - sanitize command_name to be a valid Python identifier
                    # Replace hyphens and spaces with underscores for function names
                    sanitized_command_name = command_name.replace('-', '_').replace(' ', '_')
                    tool_name = f"list_{sanitized_command_name}_vast"
                    # Use original command_name for the actual API call
                    
                    # Build function with explicit parameters (FastMCP doesn't support **kwargs in signature)
                    # Create function code dynamically with explicit parameters
                    # Convert argument names with spaces to valid Python identifiers (underscores)
                    param_defs = []
                    param_names = []
                    arg_name_mapping = {}  # Maps Python param name to original arg name
                    
                    for arg in args_config:
                        arg_name = arg.get('name')
                        arg_type = arg.get('type', 'str')
                        arg_mandatory = arg.get('mandatory', False)
                        is_filter = arg.get('filter', False)
                        
                        # Convert spaces to underscores for Python identifier
                        python_param_name = arg_name.replace(' ', '_')
                        arg_name_mapping[python_param_name] = arg_name
                        
                        param_names.append(python_param_name)
                        
                        # For int arguments with filter:true, use str type to allow filter syntax (e.g., ">1TB")
                        # This matches the CLI behavior
                        if arg_type == 'int' and is_filter:
                            arg_type_for_sig = 'str'
                        # For list type with argument_list=true, it's represented as str (comma-separated)
                        elif arg_type == 'list' and arg.get('argument_list', False):
                            arg_type_for_sig = 'str'
                        else:
                            arg_type_for_sig = arg_type
                        
                        # Set default value based on type
                        if arg_mandatory:
                            # Required parameter - no default
                            if arg_type_for_sig == 'int':
                                param_defs.append(f"{python_param_name}: int")
                            elif arg_type_for_sig == 'bool':
                                param_defs.append(f"{python_param_name}: bool")
                            else:
                                param_defs.append(f"{python_param_name}: str")
                        else:
                            # Optional parameter with default
                            if arg_type_for_sig == 'int':
                                param_defs.append(f"{python_param_name}: int = 0")
                            elif arg_type_for_sig == 'bool':
                                param_defs.append(f"{python_param_name}: bool = False")
                            else:
                                param_defs.append(f"{python_param_name}: str = ''")
                    
                    # Build docstring
                    # The description already includes arguments if {{arguments}} was in the template
                    # So we just use it as-is, or build it if it doesn't have arguments
                    if 'Args:' in description or 'Arguments:' in description:
                        # Description already has arguments - need to append order and top descriptions
                        order_arg = [arg for arg in args_config if arg.get('name') == 'order']
                        top_arg = [arg for arg in args_config if arg.get('name') == 'top']
                        
                        order_top_lines = []
                        if order_arg:
                            order_desc = order_arg[0].get('description', '')
                            order_top_lines.append(f"        order (str) (optional): {order_desc} Defaults to empty string.")
                        if top_arg:
                            top_desc = top_arg[0].get('description', '')
                            order_top_lines.append(f"        top (int) (optional): {top_desc} Defaults to 0.")
                        
                        if 'Returns:' not in description:
                            docstring = description + "\n" + "\n".join(order_top_lines) + "\n\n        Returns:\n            A list of dictionaries containing the requested information."
                        else:
                            # Insert order/top before Returns section
                            parts = description.rsplit('Returns:', 1)
                            docstring = parts[0] + "\n".join(order_top_lines) + "\n        Returns:" + parts[1]
                    else:
                        # Description doesn't have arguments, build full docstring
                        docstring_parts = [description, "\n        Args:"]
                        for arg in args_config:
                            arg_name = arg.get('name')
                            arg_desc = arg.get('description', '')
                            arg_mandatory = arg.get('mandatory', False)
                            arg_type = arg.get('type', 'str')
                            mandatory_str = " (required)" if arg_mandatory else ""
                            docstring_parts.append(f"            {arg_name} ({arg_type}): {arg_desc}{mandatory_str}")
                        docstring_parts.append("\n        Returns:")
                        docstring_parts.append("            A list of dictionaries containing the requested information.")
                        docstring = "\n        ".join(docstring_parts)
                    
                    # Build kwargs dict construction code
                    # Use original arg names (with spaces) in kwargs, but Python param names in code
                    kwargs_build = ["    kwargs = {}"]
                    for python_param_name in param_names:
                        original_arg_name = arg_name_mapping[python_param_name]
                        arg_config = next((a for a in args_config if a.get('name') == original_arg_name), {})
                        arg_type = arg_config.get('type', 'str')
                        
                        if arg_type == 'str':
                            kwargs_build.append(f"    if {python_param_name} and {python_param_name} != '': kwargs['{original_arg_name}'] = {python_param_name}")
                        elif arg_type == 'int':
                            kwargs_build.append(f"    if {python_param_name} is not None and {python_param_name} != 0: kwargs['{original_arg_name}'] = {python_param_name}")
                        elif arg_type == 'bool':
                            kwargs_build.append(f"    if {python_param_name} is not None: kwargs['{original_arg_name}'] = {python_param_name}")
                        elif arg_type == 'list':
                            # For list type, check if it's a non-empty string (comma-separated) or non-empty list
                            # Since list type is represented as str in function signature when argument_list is true
                            kwargs_build.append(f"    if {python_param_name} and {python_param_name} != '': kwargs['{original_arg_name}'] = {python_param_name}")
                    
                    # Build debug info for MCP debugging mode
                    import json
                    params_debug = []
                    for pn in param_names:
                        orig_name = arg_name_mapping.get(pn, pn)
                        arg_config = next((a for a in args_config if a.get('name') == orig_name), {})
                        params_debug.append({
                            "python_param_name": pn,
                            "original_arg_name": orig_name,
                            "type": arg_config.get('type', 'str'),
                            "mandatory": arg_config.get('mandatory', False)
                        })
                    
                    args_debug = []
                    for a in args_config:
                        args_debug.append({
                            "name": a.get('name'),
                            "type": a.get('type', 'str'),
                            "mandatory": a.get('mandatory', False),
                            "description": a.get('description', '')[:200]
                        })
                    
                    # Get fields info for debugging
                    fields_debug = []
                    for f in parser.get_fields(command_name):
                        if not f.get('hide', False):
                            # Infer field type
                            field_type = 'string'
                            if 'convert' in f:
                                field_type = 'capacity'
                            elif 'jq' in f:
                                field_type = 'string (from list)'
                            elif any(kw in f.get('name', '').lower() for kw in ['time', 'date']):
                                field_type = 'datetime'
                            
                            fields_debug.append({
                                "name": f.get('name'),
                                "field": f.get('field', ''),
                                "type": field_type
                            })
                    
                    # Serialize debug data as JSON strings (will be parsed in the function)
                    params_json_str = json.dumps(params_debug, indent=2)
                    args_json_str = json.dumps(args_debug, indent=2)
                    fields_json_str = json.dumps(fields_debug, indent=2)
                    description_json_str = json.dumps(docstring)
                    
                    # Build function code with explicit parameters
                    # Add mcp parameter for debugging (always last, always optional)
                    func_params_with_mcp = ', '.join(param_defs) + ', mcp: bool = False'
                    
                    # Use repr() to properly escape JSON strings for embedding in Python code
                    params_json_repr = repr(params_json_str)
                    args_json_repr = repr(args_json_str)
                    fields_json_repr = repr(fields_json_str)
                    description_json_repr = repr(description_json_str)
                    
                    # Build the debug_info dict construction code as separate lines to avoid f-string escaping issues
                    # Use string formatting to insert the repr values
                    debug_info_lines = [
                        '        import json',
                        '        debug_info = {',
                        f'            "tool_name": "{tool_name}",',
                        f'            "command_name": "{command_name}",',
                        f'            "description": json.loads({description_json_repr}),',
                        f'            "function_signature": "async def {tool_name}_func({func_params_with_mcp}) -> List[Dict]",',
                        f'            "parameters": json.loads({params_json_repr}),',
                        f'            "arguments_config": json.loads({args_json_repr}),',
                        f'            "fields": json.loads({fields_json_repr})',
                        '        }',
                        '        return [debug_info]'
                    ]
                    
                    # Build alias mapping code
                    alias_mapping_dict = {}  # Use dict to avoid duplicates
                    for arg in args_config:
                        arg_name = arg.get('name', '')
                        aliases = arg.get('aliases', [])
                        if aliases:
                            # Map each alias to the actual argument name
                            # Also map Python param name (with underscores) to actual name
                            python_param_name = arg_name.replace(' ', '_')
                            for alias in aliases:
                                # Map original alias
                                alias_mapping_dict[alias] = arg_name
                                # Map Python version (with underscores) if different
                                alias_python = alias.replace(' ', '_')
                                if alias_python != alias:
                                    alias_mapping_dict[alias_python] = arg_name
                            # Also map the Python param name itself if different from arg_name
                            if python_param_name != arg_name:
                                alias_mapping_dict[python_param_name] = arg_name
                    
                    # Convert to code lines
                    alias_mapping_lines = [f'        "{k}": "{v}",' for k, v in sorted(alias_mapping_dict.items())]
                    alias_mapping_code = '\n'.join(alias_mapping_lines) if alias_mapping_lines else '        # No aliases defined'
                    
                    # Escape backslashes in docstring to prevent invalid escape sequence warnings
                    # First escape backslashes, then escape braces for f-string
                    escaped_docstring = docstring.replace('\\', '\\\\').replace('{', '{{').replace('}', '}}')
                    
                    func_code = f"""async def {tool_name}_func({func_params_with_mcp}) -> List[Dict]:
    \"\"\"{escaped_docstring}\"\"\"
    # Debug mode: show MCP tool structure and description
    if mcp:
{chr(10).join(debug_info_lines)}
    
{chr(10).join(kwargs_build)}
    
    # Handle aliases: map alias parameter names to actual parameter names
    # This allows LLMs to use aliases like "clusters" instead of "cluster"
    alias_mapping = {{
{alias_mapping_code}
    }}
    # Replace any alias keys with actual argument names
    kwargs_normalized = {{}}
    for key, value in kwargs.items():
        if key in alias_mapping:
            kwargs_normalized[alias_mapping[key]] = value
        else:
            kwargs_normalized[key] = value
    kwargs = kwargs_normalized
    
    try:
        from vast_admin_mcp.functions import list_dynamic
        results = list_dynamic('{command_name}', **kwargs)
        return results
    except Exception as e:
        import logging
        logging.error(f"Error executing {command_name}: {{e}}")
        raise
"""
                    
                    # Execute function definition in clean namespace
                    exec_namespace = {
                        'List': List,
                        'Dict': Dict,
                        'logging': logging
                    }
                    exec(func_code, exec_namespace)
                    
                    # Get the function
                    tool_func = exec_namespace[f'{tool_name}_func']
                    
                    # Register with FastMCP
                    mcp.tool(name=tool_name, description=description)(tool_func)
                    
                    logging.info(f"Registered dynamic tool: {tool_name}")
                except Exception as e:
                    logging.warning(f"Could not register dynamic tool 'list_{command_name}_vast' from template file {TEMPLATE_MODIFICATIONS_FILE}: {e}")
                    import traceback
                    logging.debug(f"Traceback for {command_name}: {traceback.format_exc()}")
    else:
        logging.info(f"Template file {TEMPLATE_MODIFICATIONS_FILE} not found and no default template. Only static tools will be available.")

    # Register merged commands from YAML templates
    default_template_path = get_default_template_path()
    if os.path.exists(TEMPLATE_MODIFICATIONS_FILE) or default_template_path:
        try:
            parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
            merged_command_names = parser.get_merged_command_names()
            
            for merged_name in merged_command_names:
                merged_template = parser.get_merged_command_template(merged_name)
                if not merged_template:
                    continue
                
                # Get description
                description = parser.get_description(merged_name) or f"Retrieve merged {merged_name} from VAST cluster"
                
                # Get merged arguments
                args_config = parser.get_merged_arguments(merged_name)
                
                # Add order and top arguments for all merged tools
                args_config.append({
                    'name': 'order',
                    'type': 'str',
                    'mandatory': False,
                    'description': 'Sort results by field. Format: "field_name:direction" using colon separator. IMPORTANT: Use underscores for field names (e.g., "logical_used" not "logical used"). Examples: "physical_used:desc", "logical_used:asc", "name:desc". Direction: Use a/as/asc/ascending for ascending, or d/de/desc/descending for descending. Default is asc if omitted. Multiple fields: "field1:desc,field2:asc".'
                })
                args_config.append({
                    'name': 'top',
                    'type': 'int',
                    'mandatory': False,
                    'description': 'Limit output to top N results'
                })
                
                # Create tool name - sanitize merged_name to be a valid Python identifier
                sanitized_merged_name = merged_name.replace('-', '_').replace(' ', '_')
                tool_name = f"list_{sanitized_merged_name}_vast"
                
                # Build function with explicit parameters (same pattern as dynamic tools)
                param_defs = []
                param_names = []
                arg_name_mapping = {}
                
                for arg in args_config:
                    arg_name = arg.get('name')
                    arg_type = arg.get('type', 'str')
                    arg_mandatory = arg.get('mandatory', False)
                    is_filter = arg.get('filter', False)
                    
                    python_param_name = arg_name.replace(' ', '_')
                    arg_name_mapping[python_param_name] = arg_name
                    param_names.append(python_param_name)
                    
                    if arg_type == 'int' and is_filter:
                        arg_type_for_sig = 'str'
                    elif arg_type == 'list' and arg.get('argument_list', False):
                        arg_type_for_sig = 'str'
                    else:
                        arg_type_for_sig = arg_type
                    
                    if arg_mandatory:
                        if arg_type_for_sig == 'int':
                            param_defs.append(f"{python_param_name}: int")
                        elif arg_type_for_sig == 'bool':
                            param_defs.append(f"{python_param_name}: bool")
                        else:
                            param_defs.append(f"{python_param_name}: str")
                    else:
                        if arg_type_for_sig == 'int':
                            param_defs.append(f"{python_param_name}: int = 0")
                        elif arg_type_for_sig == 'bool':
                            param_defs.append(f"{python_param_name}: bool = False")
                        else:
                            param_defs.append(f"{python_param_name}: str = ''")
                
                # Build docstring
                if 'Args:' in description or 'Arguments:' in description:
                    # Description already has arguments - need to append order and top descriptions
                    order_arg = [arg for arg in args_config if arg.get('name') == 'order']
                    top_arg = [arg for arg in args_config if arg.get('name') == 'top']
                    
                    order_top_lines = []
                    if order_arg:
                        order_desc = order_arg[0].get('description', '')
                        order_top_lines.append(f"        order (str) (optional): {order_desc} Defaults to empty string.")
                    if top_arg:
                        top_desc = top_arg[0].get('description', '')
                        order_top_lines.append(f"        top (int) (optional): {top_desc} Defaults to 0.")
                    
                    if 'Returns:' not in description:
                        docstring = description + "\n" + "\n".join(order_top_lines) + "\n\n        Returns:\n            A list of dictionaries containing the requested information."
                    else:
                        # Insert order/top before Returns section
                        parts = description.rsplit('Returns:', 1)
                        docstring = parts[0] + "\n".join(order_top_lines) + "\n        Returns:" + parts[1]
                else:
                    docstring_parts = [description, "\n        Args:"]
                    for arg in args_config:
                        arg_name = arg.get('name')
                        arg_desc = arg.get('description', '')
                        arg_mandatory = arg.get('mandatory', False)
                        arg_type = arg.get('type', 'str')
                        mandatory_str = " (required)" if arg_mandatory else ""
                        docstring_parts.append(f"            {arg_name} ({arg_type}): {arg_desc}{mandatory_str}")
                    docstring_parts.append("\n        Returns:")
                    docstring_parts.append("            A list of dictionaries containing the requested information.")
                    docstring = "\n        ".join(docstring_parts)
                
                # Build kwargs dict construction code
                kwargs_build = ["    kwargs = {}"]
                for python_param_name in param_names:
                    original_arg_name = arg_name_mapping[python_param_name]
                    arg_config = next((a for a in args_config if a.get('name') == original_arg_name), {})
                    arg_type = arg_config.get('type', 'str')
                    
                    if arg_type == 'str':
                        kwargs_build.append(f"    if {python_param_name} and {python_param_name} != '': kwargs['{original_arg_name}'] = {python_param_name}")
                    elif arg_type == 'int':
                        kwargs_build.append(f"    if {python_param_name} is not None and {python_param_name} != 0: kwargs['{original_arg_name}'] = {python_param_name}")
                    elif arg_type == 'bool':
                        kwargs_build.append(f"    if {python_param_name} is not None: kwargs['{original_arg_name}'] = {python_param_name}")
                    elif arg_type == 'list':
                        kwargs_build.append(f"    if {python_param_name} and {python_param_name} != '': kwargs['{original_arg_name}'] = {python_param_name}")
                
                # Build debug info
                import json
                source_functions = merged_template.get('functions', [])
                params_debug = []
                for pn in param_names:
                    orig_name = arg_name_mapping.get(pn, pn)
                    arg_config = next((a for a in args_config if a.get('name') == orig_name), {})
                    params_debug.append({
                        "python_param_name": pn,
                        "original_arg_name": orig_name,
                        "type": arg_config.get('type', 'str'),
                        "mandatory": arg_config.get('mandatory', False)
                    })
                
                args_debug = []
                for a in args_config:
                    args_debug.append({
                        "name": a.get('name'),
                        "type": a.get('type', 'str'),
                        "mandatory": a.get('mandatory', False),
                        "description": a.get('description', '')[:200]
                    })
                
                # Get merged fields info for debugging
                merged_field_names = parser.get_merged_fields(merged_name)
                fields_debug = [{"name": fn, "type": "string"} for fn in merged_field_names]
                
                params_json_str = json.dumps(params_debug, indent=2)
                args_json_str = json.dumps(args_debug, indent=2)
                fields_json_str = json.dumps(fields_debug, indent=2)
                description_json_str = json.dumps(docstring)
                source_funcs_json_str = json.dumps(source_functions)
                
                func_params_with_mcp = ', '.join(param_defs) + ', mcp: bool = False'
                
                params_json_repr = repr(params_json_str)
                args_json_repr = repr(args_json_str)
                fields_json_repr = repr(fields_json_str)
                description_json_repr = repr(description_json_str)
                source_funcs_json_repr = repr(source_funcs_json_str)
                
                debug_info_lines = [
                    '        import json',
                    '        debug_info = {',
                    f'            "tool_name": "{tool_name}",',
                    f'            "merged_name": "{merged_name}",',
                    f'            "source_functions": json.loads({source_funcs_json_repr}),',
                    f'            "description": json.loads({description_json_repr}),',
                    f'            "function_signature": "async def {tool_name}_func({func_params_with_mcp}) -> List[Dict]",',
                    f'            "parameters": json.loads({params_json_repr}),',
                    f'            "arguments_config": json.loads({args_json_repr}),',
                    f'            "fields": json.loads({fields_json_repr})',
                    '        }',
                    '        return [debug_info]'
                ]
                
                # Build alias mapping code
                alias_mapping_dict = {}
                for arg in args_config:
                    arg_name = arg.get('name', '')
                    aliases = arg.get('aliases', [])
                    if aliases:
                        python_param_name = arg_name.replace(' ', '_')
                        for alias in aliases:
                            alias_mapping_dict[alias] = arg_name
                            alias_python = alias.replace(' ', '_')
                            if alias_python != alias:
                                alias_mapping_dict[alias_python] = arg_name
                        if python_param_name != arg_name:
                            alias_mapping_dict[python_param_name] = arg_name
                
                alias_mapping_lines = [f'        "{k}": "{v}",' for k, v in sorted(alias_mapping_dict.items())]
                alias_mapping_code = '\n'.join(alias_mapping_lines) if alias_mapping_lines else '        # No aliases defined'
                
                # Escape backslashes in docstring to prevent invalid escape sequence warnings
                escaped_docstring = docstring.replace('\\', '\\\\').replace('{', '{{').replace('}', '}}')
                
                func_code = f"""async def {tool_name}_func({func_params_with_mcp}) -> List[Dict]:
    \"\"\"{escaped_docstring}\"\"\"
    # Debug mode: show MCP tool structure and description
    if mcp:
{chr(10).join(debug_info_lines)}
    
{chr(10).join(kwargs_build)}
    
    # Handle aliases: map alias parameter names to actual parameter names
    alias_mapping = {{
{alias_mapping_code}
    }}
    # Replace any alias keys with actual argument names
    kwargs_normalized = {{}}
    for key, value in kwargs.items():
        if key in alias_mapping:
            kwargs_normalized[alias_mapping[key]] = value
        else:
            kwargs_normalized[key] = value
    kwargs = kwargs_normalized
    
    try:
        from vast_admin_mcp.functions import list_merged
        results = list_merged('{merged_name}', **kwargs)
        return results
    except Exception as e:
        import logging
        logging.error(f"Error executing merged {merged_name}: {{e}}")
        raise
"""
                
                # Execute function definition in clean namespace
                exec_namespace = {
                    'List': List,
                    'Dict': Dict,
                    'logging': logging
                }
                exec(func_code, exec_namespace)
                
                # Get the function
                tool_func = exec_namespace[f'{tool_name}_func']
                
                # Register with FastMCP
                mcp.tool(name=tool_name, description=description)(tool_func)
                
                logging.info(f"Registered merged tool: {tool_name}")
        except Exception as e:
            logging.warning(f"Could not load merged tools from template file {TEMPLATE_MODIFICATIONS_FILE}: {e}")

    # Register create functions (always visible to LLM, but check read_write at runtime)
    if CREATE_FUNCTIONS_AVAILABLE:
        readonly_error_msg = "This operation is not available in readonly mode. The MCP server must be started with the --read-write flag to enable create operations. Please restart the MCP server using: vast-admin-mcp mcp --read-write"
        
        @mcp.tool(name="create_view_vast", description="Create a new VAST view")
        async def create_view_mcp(
            cluster: str,
            tenant: str = 'default',
            path: str = '',
            protocols: Optional[str] = None,
            bucket: str = '',
            bucket_owner: str = '',
            share: str = '',
            policy: str = '',
            hard_quota: str = '',
            qos_policy: str = '',
        ) -> List[Dict[str, str]]:
            """
            Create a view in a VAST cluster. Provide cluster and path at minimum.

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
                A list of client paths for the new view for the specified protocols. each item in the list will be a dictionary containing protocol and client path.
            """
            if not read_write:
                raise ValueError(readonly_error_msg)
            try:
                paths = create_view(
                    cluster=cluster,
                    tenant=tenant or 'default',
                    path=path or None,
                    protocols=protocols or None,
                    bucket=bucket or None,
                    bucket_owner=bucket_owner or None,
                    share=share or None,
                    policy=policy or None,
                    hard_quota=hard_quota or None,
                    qos_policy=qos_policy or None
                )
                return paths
            except Exception as e:
                logging.error(f"Error creating view: {e}")
                raise
        
        @mcp.tool(name="create_view_from_template_vast", description="Create a new VAST view from a predefined template")
        async def create_view_from_template_mcp(
            template: str = '',
            count: int = 1
        ) -> List[Dict[str, str]]:
            """
            Create a view in a VAST cluster based on a predefined template. Templates are defined in the view templates file.

            Args:
                template: Template name defined in the view templates file.
                count: Number of views to create from the template. Defaults to 1.
            Returns:
                A list of client paths for the new view for the specified protocols. Each item in the list will be a dictionary containing protocol and client path.
            """
            if not read_write:
                raise ValueError(readonly_error_msg)
            try:
                paths = create_view_from_template(
                    template=template or None,
                    count=count or 1
                )
                return paths
            except Exception as e:
                logging.error(f"Error creating view from template: {e}")
                raise
        
        @mcp.tool(name="create_snapshot_vast", description="Create a snapshot for a VAST view")
        async def create_snapshot_mcp(
            cluster: str,
            tenant: str = 'default',
            path: str = '',
            snapshot_name: str = '',
            expiry_time: str = '',
            indestructible: bool = False,
            create_with_timestamp: bool = False
        ) -> Dict[str, Any]:
            """
            Create a snapshot for a view in a VAST cluster.

            Args:
                cluster: Cluster address or name. Required.
                tenant: Tenant name that owns the view. Defaults to 'default' if not provided.
                path: View path to snapshot (e.g., /nfs/myshare). Required.
                snapshot_name: Name for the snapshot. Required.
                expiry_time: Expiry time (e.g., 2d, 3w, 1d6h, 30m). Optional.
                indestructible: Whether to make the snapshot indestructible. Defaults to False.
                create_with_timestamp: Whether to append a timestamp to the snapshot name. Defaults to False.
            Returns:
                Snapshot creation details including cluster, tenant, path, snapshot name, and expiry information.
            """
            if not read_write:
                raise ValueError(readonly_error_msg)
            try:
                result = create_snapshot(
                    cluster=cluster,
                    tenant=tenant or 'default',
                    path=path or None,
                    snapshot_name=snapshot_name or None,
                    expiry_time=expiry_time or None,
                    indestructible=indestructible or False,
                    create_with_timestamp=create_with_timestamp or False
                )
                return result
            except Exception as e:
                logging.error(f"Error creating snapshot: {e}")
                raise
        
        @mcp.tool(name="create_clone_vast", description="Create a clone from a snapshot")
        async def create_clone_mcp(
            cluster: str,
            source_tenant: str = 'default',
            source_path: str = '',
            source_snapshot: str = '',
            destination_tenant: str = '',
            destination_path: str = '',
            refresh: bool = False
        ) -> List[Dict[str, str]]:
            """
            Create a clone from a snapshot in a VAST cluster.

            Args:
                cluster: Cluster address or name. Required.
                source_tenant: Source tenant name. Defaults to 'default' if not provided.
                source_path: Source view path to clone from. Required.
                source_snapshot: Source snapshot name (use * suffix for newest with prefix, when doing this you don't need to look for snapshots before cloning, if you use just * it will give you the newest snapshot). Required.
                destination_tenant: Destination tenant name (defaults to source tenant).
                destination_path: Destination path for the clone. Required.
                refresh: Whether to destroy existing clone before creating new one. Defaults to False, if there is a view configured at the destination path it will remain and will be linked to the new clone after creation.
            Returns:
                When empty list is returned, clone was created successfully but no view exists at the destination path.
                When a list with one item is returned, the item will be a dictionary containing access paths for the view linked to the clone per protocol.
            """
            if not read_write:
                raise ValueError(readonly_error_msg)
            try:
                result = create_clone(
                    cluster=cluster,
                    source_tenant=source_tenant or 'default',
                    source_path=source_path or None,
                    source_snapshot=source_snapshot or None,
                    destination_tenant=destination_tenant or None,
                    destination_path=destination_path or None,
                    refresh=refresh or False
                )
                return result
            except Exception as e:
                logging.error(f"Error creating clone: {e}")
                raise
        
        @mcp.tool(name="create_quota_vast", description="Create or update quota for a specific path and tenant")
        async def create_quota_mcp(
            cluster: str,
            tenant: str = 'default',
            path: str = '',
            hard_limit: Optional[str] = None,
            soft_limit: Optional[str] = None,
            files_hard_limit: Optional[int] = None,
            files_soft_limit: Optional[int] = None,
            grace_period: Optional[int] = None
        ) -> Dict[str, Any]:
            """
            Use this tool to create or update quota for a specific path and tenant on a VAST cluster. This operation requires read-write mode.

            Args:
                cluster: Cluster address or name. Required.
                tenant: Tenant name. Defaults to 'default' if not provided.
                path: View path to set quota for. Required.
                hard_limit (str): Hard quota limit (e.g., '10GB', '1TB'). If not specified, quota is unlimited.
                soft_limit (str): Soft quota limit (e.g., '8GB', '800GB'). If not specified, quota is unlimited.
                files_hard_limit (int): Hard limit for number of files. If not specified, unlimited.
                files_soft_limit (int): Soft limit for number of files. If not specified, unlimited.
                grace_period (int): Grace period in seconds for soft limit. If not specified, uses default.

            Returns:
                A dictionary containing the created/updated quota information including Cluster, Tenant, Path, Name, Hard Limit, Soft Limit, Files Hard Limit, Files Soft Limit, and Grace Period.
            """
            if not read_write:
                raise ValueError(readonly_error_msg)
            try:
                result = create_quota(
                    cluster=cluster,
                    tenant=tenant or 'default',
                    path=path or None,
                    hard_limit=hard_limit,
                    soft_limit=soft_limit,
                    files_hard_limit=files_hard_limit,
                    files_soft_limit=files_soft_limit,
                    grace_period=grace_period
                )

                return result
            except Exception as e:
                logging.error(f"Error creating/updating quota: {e}")
                raise
        
        logging.info("Registered create tools (available in read-write mode only)")

    mcp.run(transport="stdio")
    
    logging.info("MCP server started successfully.")

