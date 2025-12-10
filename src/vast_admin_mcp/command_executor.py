"""Command Executor - Executes templated commands
Handles:
- Multi-API calls
- Data joins
- Field transformations (jq, unit conversion)
- Ordering (via CLI arguments)
- Output formatting
"""

import json
import logging
import subprocess
import shutil
import fnmatch
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from .template_parser import TemplateParser
from .client import create_vast_client, call_vast_api
from .config import load_config, JQ_TIMEOUT_SECONDS
from .utils import (
    parse_filter_value, parse_capacity_value, parse_order_spec, apply_ordering, 
    format_time_delta, normalize_field_name, to_python_name, to_raw_field_name
)


def check_jq_available() -> bool:
    """Check if jq command is available in the system"""
    return shutil.which("jq") is not None


class CommandExecutor:
    """Executes templated commands with data transformation pipeline"""
    
    def __init__(self, template_parser: TemplateParser, cluster: Optional[str] = None, client=None):
        self.template_parser = template_parser
        self.cluster = cluster
        self.client = client
        if cluster and not self.client:
            self.client = create_vast_client(cluster)
    
    def execute(self, command_name: str, cli_args: Dict[str, Any]) -> List[Dict]:
        """Execute a command with given arguments"""
        template = self.template_parser.get_template(command_name)
        if not template:
            raise ValueError(f"Template not found for command: {command_name}")
        
        # Get cluster from executor's cluster (address) first, then args, then default
        # self.cluster is the resolved cluster address set when executor was created
        # cli_args.get('cluster') might be a cluster name (for $(cluster) field transformation)
        # We prioritize self.cluster (address) over cli_args (which might be a name)
        cluster = self.cluster or cli_args.get('cluster')
        if not cluster:
            config = load_config()
            cluster = config['clusters'][0]['cluster']
        
        # Create client if not already created or cluster changed
        # Only create new client if cluster address actually changed
        if not self.client or (self.cluster and self.cluster != cluster):
            self.client = create_vast_client(cluster)
            self.cluster = cluster
        
        # 1. Validate arguments
        self._validate_arguments(command_name, cli_args)
        
        # 2. Map CLI arguments to API parameters
        api_params = self._map_arguments_to_api(command_name, cli_args)
        
        # 3. Execute API calls
        api_data = self._execute_api_calls(command_name, api_params)
        
        # 3.5. Apply client-side filtering on raw API data (for API fields only)
        # This ensures correct results even if API filtering doesn't work as expected
        # Note: This only filters on fields that exist in the API response
        if self._client_filters:
            api_data = self._apply_client_filters(command_name, api_data)
        
        # 4. Join data from multiple APIs
        joined_data = self._join_data(command_name, api_data)
        
        # 4.5. Execute per-row endpoints if configured
        per_row_endpoints = self.template_parser.get_per_row_endpoints(command_name)
        if per_row_endpoints:
            joined_data = self._execute_per_row_endpoints(command_name, joined_data, cli_args)
        
        # 5. Apply ordering (BEFORE transformation to sort by raw numeric values)
        # Ordering now comes from CLI args, not YAML template
        ordered_data = self._apply_ordering(command_name, joined_data, cli_args)
        
        # 6. Transform fields
        # Capture original data before transformation if instance flag is set and output is JSON
        include_instance = cli_args.get('instance', False) and cli_args.get('_output_format') == 'json'
        if include_instance:
            # Deep copy original data to preserve it
            import copy
            original_data = [copy.deepcopy(row) for row in ordered_data]
        
        transformed_data = self._transform_fields(command_name, ordered_data, cli_args)
        
        # 6.5. Apply client-side filtering on transformed data (for computed fields)
        # This handles filtering on computed fields that don't exist in the API response
        if self._client_filters:
            transformed_data = self._apply_client_filters_on_transformed(command_name, transformed_data, cli_args)
        
        # 6.6. Add original instance data if requested
        if include_instance:
            for i, row in enumerate(transformed_data):
                if i < len(original_data):
                    row['instance'] = original_data[i]
        
        # 7. Top limit is now applied in list_dynamic() after combining all clusters
        # Skip top limit here if it's in cli_args (will be applied later)
        
        # 8. Ensure field order matches YAML definition (filter out hidden fields)
        final_data = self._ensure_field_order(command_name, transformed_data)
        
        return final_data
    
    def _validate_arguments(self, command_name: str, cli_args: Dict[str, Any]):
        """Validate CLI arguments against template"""
        args_config = self.template_parser.get_arguments(command_name)
        
        for arg_config in args_config:
            arg_name = arg_config.get('name')
            is_mandatory = arg_config.get('mandatory', False)
            arg_type = arg_config.get('type', 'str')
            
            # Check mandatory arguments
            if is_mandatory and arg_name not in cli_args:
                raise ValueError(f"Missing mandatory argument: {arg_name}")
            
            # Skip validation for list and other complex types - already validated at CLI level
            if arg_name in cli_args and arg_type not in ['list', 'choice', 'flag']:
                value = str(cli_args[arg_name])
                # Skip regex validation for wildcard patterns (they'll be handled client-side)
                if isinstance(cli_args[arg_name], str) and ('*' in cli_args[arg_name] or '?' in cli_args[arg_name] or '[' in cli_args[arg_name]):
                    continue  # Skip validation for wildcard patterns
                is_valid, error_msg = self.template_parser.validate_argument_value(command_name, arg_name, value)
                if not is_valid:
                    raise ValueError(f"{error_msg}\n   Provided value: '{value}'")
    
    def _map_arguments_to_api(self, command_name: str, cli_args: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Map CLI arguments to API parameters for each endpoint
        
        Returns:
            Dict of api_params for each endpoint
        """
        api_endpoints = self.template_parser.get_api_endpoints(command_name)
        api_params = {endpoint: {} for endpoint in api_endpoints}
        client_filters = {}  # Store filters that need client-side processing
        
        # Get all arguments configuration to check filter settings
        args_config = self.template_parser.get_arguments(command_name)
        # Create dict with both space and underscore versions as keys for lookup
        args_dict = {}
        for arg in args_config:
            arg_name = arg.get('name')
            if arg_name:
                args_dict[arg_name] = arg
                # Also add underscore version for CLI argument matching
                args_dict[to_python_name(arg_name)] = arg
        
        for arg_name, arg_value in cli_args.items():
            if arg_value is None or arg_value == '':
                continue
            
            # Skip order, top, and internal flags as they're handled separately
            if arg_name in ['order', 'top', '_output_format']:
                continue
                
            api_param_name = self.template_parser.get_api_mapping(command_name, arg_name)
            
            if api_param_name:  # Not False (non-API parameter)
                # Get argument configuration (try both underscore and space versions)
                arg_config = args_dict.get(arg_name) or args_dict.get(normalize_field_name(arg_name, 'to_space'))
                if not arg_config:
                    arg_config = {}
                is_filter = arg_config.get('filter', False)
                arg_type = arg_config.get('type', 'str')
                
                # Special case: capacity fields are typed as 'int' but should support capacity parsing
                # Detect capacity fields by checking field configuration for 'convert' property
                # or by field name patterns (capacity, size, limit, used)
                if arg_type == 'int' and is_filter:
                    # Get field configuration to check for capacity indicators
                    # Normalize arg_name for field lookup (handle both space and underscore)
                    field_lookup_name = normalize_field_name(arg_name, 'to_space')
                    fields = self.template_parser.get_fields(command_name)
                    for field_config in fields:
                        field_name = field_config.get('name')
                        if field_name == arg_name or field_name == field_lookup_name:
                            # Check if field has 'convert' property (indicates capacity field)
                            if 'convert' in field_config:
                                arg_type = 'capacity'
                            # Or check field name for capacity-related keywords
                            elif any(keyword in arg_name.lower() for keyword in ['capacity', 'size', 'limit', 'used', 'quota']):
                                arg_type = 'capacity'
                            break
                
                # If this is a filter argument, check if it's a computed field
                if is_filter and isinstance(arg_value, str):
                    # Check if this field is computed (has 'value' property)
                    # Computed fields don't exist in API response, so skip API filtering
                    field_lookup_name = arg_name.replace('_', ' ')
                    fields = self.template_parser.get_fields(command_name)
                    is_computed_field = False
                    for field_config in fields:
                        field_name = field_config.get('name')
                        if field_name == arg_name or field_name == field_lookup_name:
                            # Check if field has 'value' property (computed field)
                            if 'value' in field_config:
                                is_computed_field = True
                            break
                    
                    # Check if field has client_side_filter: true flag
                    client_side_only = False
                    for field_config in fields:
                        field_name = field_config.get('name')
                        if field_name == arg_name or field_name == field_lookup_name:
                            # Check if field has client_side_filter flag
                            arg_config_in_field = field_config.get('argument', {})
                            if arg_config_in_field.get('client_side_filter', False):
                                client_side_only = True
                            break
                    
                    if is_computed_field or client_side_only:
                        # Computed field or client-side-only field - skip API filtering, only use client-side filtering
                        client_filters[arg_name] = {
                            'field': api_param_name,
                            'pattern': arg_value,
                            'computed': is_computed_field  # Mark if it's computed for later filtering
                        }
                        reason = 'computed field' if is_computed_field else 'client_side_filter flag'
                        logging.debug(f"Skipping API filter for {reason} '{arg_name}' with value '{arg_value}', using client-side filtering only")
                        continue
                    
                    # Not a computed field - try API filtering
                    try:
                        # Try to parse as VAST API filter
                        filter_type, filter_value, api_suffix = parse_filter_value(arg_value, arg_type)
                        
                        # Build API parameter name with suffix
                        if api_suffix:
                            vast_api_param = f"{api_param_name}{api_suffix}"
                        else:
                            vast_api_param = api_param_name
                        
                        # Determine which endpoint(s) this filter should be applied to
                        # Check if this is a joined field (e.g., quotas.hard_limit)
                        target_endpoints = []
                        actual_api_param = vast_api_param
                        if '.' in api_param_name:
                            # Joined field - extract endpoint name and actual field name
                            # e.g., "quotas.hard_limit" -> endpoint="quotas", field="hard_limit"
                            endpoint_name = api_param_name.split('.')[0]
                            field_name = '.'.join(api_param_name.split('.')[1:])  # Get everything after first dot
                            if endpoint_name in api_endpoints:
                                target_endpoints = [endpoint_name]
                                # Rebuild API parameter without endpoint prefix
                                if api_suffix:
                                    actual_api_param = f"{field_name}{api_suffix}"
                                else:
                                    actual_api_param = field_name
                        else:
                            # Regular field - belongs to first endpoint (primary data source)
                            if api_endpoints:
                                target_endpoints = [api_endpoints[0]]
                        
                        # Add to API params only for relevant endpoints
                        for endpoint in target_endpoints:
                            api_params[endpoint][actual_api_param] = filter_value
                        
                        # ALWAYS also add to client-side filters for validation/backup
                        # This ensures filtering works correctly even if API filtering doesn't work as expected
                        client_filters[arg_name] = {
                            'field': api_param_name,
                            'pattern': arg_value
                        }
                        logging.debug(f"Added server-side API filter for '{arg_name}' with value '{arg_value}', also applying client-side validation")
                        continue
                    except (ValueError, Exception) as e:
                        # If parsing fails, use client-side filtering as fallback
                        # This ensures filtering works even if API doesn't support the filter parameter
                        # or if the filter format is not recognized
                        client_filters[arg_name] = {
                            'field': api_param_name,
                            'pattern': arg_value
                        }
                        # For non-wildcard patterns, we'll do exact match or substring match
                        logging.debug(f"Using client-side filtering for '{arg_name}' with value '{arg_value}': {e}")
                else:
                    # Not a filter argument or not a string, pass directly to API
                    for endpoint in api_endpoints:
                        api_params[endpoint][api_param_name] = arg_value
        
        # Store client filters for later use
        self._client_filters = client_filters
        return api_params
    
    def _execute_api_calls(self, command_name: str, api_params: Dict[str, Dict]) -> Dict[str, List[Dict]]:
        """Execute API calls for all endpoints using unified API call function"""
        # Get whitelist from template parser
        whitelist = self.template_parser.get_api_whitelist()
        
        api_data = {}
        
        for endpoint, params in api_params.items():
            try:
                # Create a copy of params to avoid modifying the original
                request_params = params.copy()
                
                # Handle tenant_id separately if present
                tenant_id = request_params.pop('tenant_id', None)
                
                # Use unified API call function
                all_results = call_vast_api(
                    client=self.client,
                    endpoint=endpoint,
                    method='get',
                    params=request_params,
                    tenant_id=tenant_id,
                    whitelist=whitelist
                )
                
                api_data[endpoint] = all_results
            except ValueError as e:
                # Whitelist validation error - log and skip this endpoint
                logging.error(f"Access denied for endpoint {endpoint}: {e}")
                api_data[endpoint] = []
            except Exception as e:
                logging.error(f"Error calling API endpoint {endpoint}: {e}")
                api_data[endpoint] = []
        
        return api_data
    
    def _apply_client_filters(self, command_name: str, api_data: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
        """Apply client-side filtering to validate and backup server-side filtering
        
        This function always applies client-side filtering as a validation/backup mechanism,
        even when server-side API filtering was attempted. This ensures correct results
        regardless of whether the API properly implements filtering.
        """
        if not self._client_filters:
            return api_data
        
        filtered_data = {}
        
        for endpoint, data_list in api_data.items():
            filtered_list = data_list.copy()
            
            # Apply each client filter
            for arg_name, filter_config in self._client_filters.items():
                # Skip computed fields - they'll be filtered after transformation
                if filter_config.get('computed', False):
                    logging.debug(f"Skipping client-side filter on raw API data for computed field '{arg_name}' (will filter after transformation)")
                    continue
                
                api_param_name = filter_config['field']  # This is the API parameter name (e.g., 'view')
                pattern = filter_config['pattern']
                
                # Check if this is a list field
                is_list_field = False
                fields = self.template_parser.get_fields(command_name)
                for field_config in fields:
                    field_name_check = field_config.get('name')
                    if field_name_check == arg_name or field_name_check.replace(' ', '_') == arg_name:
                        arg_config = field_config.get('argument', {})
                        if arg_config.get('type') == 'list':
                            is_list_field = True
                        break
                
                # Map API parameter name to actual field name in response
                # The API parameter might differ from the field name in the response
                # For example: API param 'view' maps to field 'path' in response
                # Try to find the field name from the template
                field_name = self._get_response_field_name(command_name, arg_name, api_param_name)
                
                # Check if field exists in the data (skip if it doesn't - might be computed)
                if filtered_list and field_name not in filtered_list[0]:
                    logging.debug(f"Skipping client-side filter on raw API data for field '{arg_name}' (field '{field_name}' not in API response, might be computed)")
                    continue
                
                # Filter rows that match the wildcard pattern
                filtered_list = [
                    row for row in filtered_list
                    if self._match_wildcard(row.get(field_name), pattern, is_list_field=is_list_field)
                ]
            
            filtered_data[endpoint] = filtered_list
        
        return filtered_data
    
    def _apply_client_filters_on_transformed(self, command_name: str, data: List[Dict], cli_args: Dict) -> List[Dict]:
        """Apply client-side filtering on transformed data (for computed fields)
        
        This handles filtering on computed fields that are created during transformation
        and don't exist in the raw API response.
        """
        if not self._client_filters:
            logging.debug("No client filters to apply on transformed data")
            return data
        
        if not data:
            logging.debug("No data to filter")
            return data
        
        logging.debug(f"Applying client-side filters on transformed data: {len(data)} rows, {len(self._client_filters)} filters")
        
        # Get the actual field names from the first row to see what's available
        sample_row = data[0] if data else {}
        available_fields = list(sample_row.keys())
        
        filtered_list = data.copy()
        
        # Apply each client filter
        for arg_name, filter_config in self._client_filters.items():
            api_param_name = filter_config['field']
            pattern = filter_config['pattern']
            logging.debug(f"Processing client filter: arg_name='{arg_name}', api_param_name='{api_param_name}', pattern='{pattern}'")
            
            # Resolve the actual field name in transformed data
            field_resolution = self._resolve_field_name(command_name, arg_name, sample_row, available_fields)
            if not field_resolution:
                # Field not found - skip filtering for this field
                continue
            
            field_to_use, is_list_field = field_resolution
            
            # Filter rows that match the wildcard pattern
            initial_count = len(filtered_list)
            filtered_list = [
                row for row in filtered_list
                if self._match_wildcard(row.get(field_to_use), pattern, is_list_field=is_list_field)
            ]
            
            logging.debug(f"Applied client-side filter '{arg_name}' with pattern '{pattern}' on field '{field_to_use}': {initial_count} -> {len(filtered_list)} rows")
            
            # Debug: show sample values if filtering removed all results
            if len(filtered_list) == 0 and initial_count > 0:
                sample_values = [str(row.get(field_to_use)) for row in data[:5] if field_to_use in row]
                logging.debug(f"Filter '{pattern}' removed all rows. Sample values from field '{field_to_use}': {sample_values}")
        
        return filtered_list
    
    def _resolve_field_name(self, command_name: str, arg_name: str, sample_row: Dict, available_fields: List[str]) -> Optional[Tuple[str, bool]]:
        """Resolve the actual field name in transformed data for a given argument name.
        
        Tries multiple strategies to find the matching field:
        1. Check template for exact field name
        2. Try normalized variations (space/underscore)
        3. Try case-insensitive matching
        4. Try fuzzy matching
        
        Args:
            command_name: Name of the command
            arg_name: Argument name to resolve
            sample_row: Sample row from transformed data
            available_fields: List of available field names
            
        Returns:
            Tuple of (field_name, is_list_field) if found, None otherwise
        """
        # Try multiple variations: space, underscore, normalized
        field_name_space = normalize_field_name(arg_name, 'to_space')
        field_name_underscore = to_python_name(arg_name)
        normalized_field_name = to_python_name(arg_name)
        
        # Check what the actual field name is in the YAML template
        fields = self.template_parser.get_fields(command_name)
        actual_field_name = None
        is_list_field = False
        for field_config in fields:
            field_name = field_config.get('name')
            # Match by exact name or normalized name
            if field_name:
                # Normalize both for comparison
                field_name_normalized = to_python_name(field_name)
                arg_name_normalized = to_python_name(arg_name)
                if (field_name == arg_name or 
                    field_name == field_name_space or
                    field_name_normalized == arg_name_normalized):
                    actual_field_name = field_name
                    # Check if this is a list field
                    arg_config = field_config.get('argument', {})
                    if arg_config.get('type') == 'list':
                        is_list_field = True
                    break
        
        # Try to find the field in the sample row
        # Check all possible variations
        field_to_use = None
        candidates = [actual_field_name, field_name_space, field_name_underscore, normalized_field_name]
        # Also try case-insensitive matching
        for candidate in candidates:
            if not candidate:
                continue
            # Try exact match
            if candidate in sample_row:
                field_to_use = candidate
                logging.debug(f"Found field '{field_to_use}' in transformed data for filter '{arg_name}'")
                break
            # Try case-insensitive match
            for key in sample_row.keys():
                if key.lower() == candidate.lower():
                    field_to_use = key
                    logging.debug(f"Found field '{field_to_use}' (case-insensitive match) in transformed data for filter '{arg_name}'")
                    break
            if field_to_use:
                break
        
        if not field_to_use:
            # Field not found - try to find it by checking all fields with similar names
            # This handles cases where field names might be slightly different
            for key in sample_row.keys():
                # Normalize both for comparison
                key_normalized = to_python_name(key).lower()
                arg_normalized = to_python_name(arg_name).lower()
                field_space_normalized = to_python_name(field_name_space).lower()
                
                if (key_normalized == arg_normalized or 
                    key_normalized == field_space_normalized or
                    'protection' in key.lower() and 'type' in key.lower()):
                    field_to_use = key
                    logging.debug(f"Found field '{field_to_use}' by fuzzy matching for filter '{arg_name}'")
                    break
        
        if not field_to_use:
            # Field still not found - log available fields for debugging
            logging.warning(f"Field '{arg_name}' not found in transformed data. Tried: {[actual_field_name, field_name_space, field_name_underscore, normalized_field_name]}. Available fields: {available_fields}")
            return None
        
        return (field_to_use, is_list_field)
    
    def _get_response_field_name(self, command_name: str, arg_name: str, api_param_name: str) -> str:
        """Get the actual field name in API response for a given argument
        
        The API parameter name might differ from the field name in the response.
        For example, API param 'view' corresponds to field 'path' in the response.
        """
        # Get fields from template to find the mapping
        fields = self.template_parser.get_fields(command_name)
        
        # Look for a field that uses the argument name or API parameter name
        for field_config in fields:
            # Check both 'name' and 'header' properties (YAML uses 'name')
            field_header = field_config.get('name') or field_config.get('header', '')
            field_source = field_config.get('field', field_header)
            
            # If the field header matches the argument name, use the field source
            if field_header.lower() == arg_name.lower():
                # If field source is just the header, that's the field name
                if field_source == field_header:
                    return field_source
                # Otherwise, the field source might be the actual field name
                # Remove any transformations (like $(cluster))
                if not field_source.startswith('$('):
                    return field_source
        
        # Fallback: try common mappings
        # API param 'view' -> response field 'path'
        if api_param_name == 'view':
            return 'path'
        # API param 'tenant_name' -> response field 'tenant_name'
        if api_param_name == 'tenant_name':
            return 'tenant_name'
        
        # Default: use API parameter name
        return api_param_name
    
    def _match_wildcard(self, value: Any, pattern: str, is_list_field: bool = False) -> bool:
        """Check if value matches pattern, supporting all filter types:
        - Wildcard patterns: *value*, value*, *value, !*value*
        - Numeric comparisons: >100, >=100, <100, <=100, 100
        - Capacity comparisons: >1TB, >=500GB, <1M
        - Boolean: true, false, 1, 0
        - String patterns: contains, starts_with, ends_with, equals, not_contains
        - List fields (comma-separated): Checks if pattern value exists in the comma-separated list (in operator)
        
        Args:
            value: The field value to check
            pattern: The filter pattern
            is_list_field: If True, treats value as comma-separated list and checks if pattern is in the list
        """
        if value is None:
            # Handle special case: non-empty filter (*) should not match None
            if pattern == '*':
                return False
            # For other filters, None values typically don't match
            return False
        
        # Handle list fields (comma-separated values) - check if pattern exists in the list
        if is_list_field:
            value_str = str(value).strip()
            if not value_str:
                return False
            
            # Support both "in:value" syntax and plain "value" syntax
            if pattern.startswith('in:'):
                search_value = pattern[3:].strip()
            else:
                search_value = pattern.strip()
            
            # Split the comma-separated list and check each entry
            list_items = [item.strip() for item in value_str.split(',')]
            
            # Check for exact match (case-insensitive)
            for item in list_items:
                if item.lower() == search_value.lower():
                    return True
            
            # Also support wildcard matching within list items
            # e.g., "*user*" should match if any item in the list contains "user"
            if '*' in search_value or '?' in search_value:
                for item in list_items:
                    if fnmatch.fnmatch(item, search_value):
                        return True
                return False
            
            # For plain strings, also check substring match (case-insensitive)
            for item in list_items:
                if search_value.lower() in item.lower():
                    return True
            
            return False
        
        # Try to parse the pattern using the same logic as parse_filter_value
        # We need to detect the filter type and apply appropriate matching
        try:
            # First, try to detect if it's a capacity filter (has units like TB, GB, etc.)
            import re
            capacity_pattern = r'^(>=|<=|>|<|=)?\s*([\d.]+)\s*([A-Za-z]+)$'
            if re.match(capacity_pattern, pattern):
                # It's a capacity filter - parse and compare
                from .utils import parse_capacity_value
                operator, value_in_bytes = parse_capacity_value(pattern)
                
                # Convert field value to bytes for comparison
                field_bytes = self._convert_to_bytes(value)
                if field_bytes is None:
                    return False
                
                # Apply comparison
                if operator == 'gt':
                    return field_bytes > value_in_bytes
                elif operator == 'gte':
                    return field_bytes >= value_in_bytes
                elif operator == 'lt':
                    return field_bytes < value_in_bytes
                elif operator == 'lte':
                    return field_bytes <= value_in_bytes
                elif operator == 'eq':
                    return field_bytes == value_in_bytes
                return False
            
            # Try to detect numeric filter (>100, >=100, etc.)
            numeric_pattern = r'^(>=|<=|>|<|=)?\s*([\d.]+)$'
            numeric_match = re.match(numeric_pattern, pattern)
            if numeric_match:
                operator_str = numeric_match.group(1) or '='
                number_str = numeric_match.group(2)
                
                try:
                    filter_number = float(number_str) if '.' in number_str else int(number_str)
                    field_number = self._convert_to_number(value)
                    
                    if field_number is None:
                        return False
                    
                    # Apply numeric comparison
                    if operator_str == '>':
                        return field_number > filter_number
                    elif operator_str == '>=':
                        return field_number >= filter_number
                    elif operator_str == '<':
                        return field_number < filter_number
                    elif operator_str == '<=':
                        return field_number <= filter_number
                    elif operator_str == '=':
                        return field_number == filter_number
                    else:
                        # No operator means equals
                        return field_number == filter_number
                except (ValueError, TypeError):
                    # Not a valid number, fall through to string matching
                    pass
            
                # Handle string filters
            value_str = str(value)
            
            # Non-empty: * (matches any non-empty string)
            if pattern == '*':
                return len(value_str) > 0
            
            # Not contains: !*value*
            if pattern.startswith('!*') and pattern.endswith('*'):
                search_value = pattern[2:-1].lower()
                return search_value not in value_str.lower()
            
            # Contains: *value*
            if pattern.startswith('*') and pattern.endswith('*'):
                search_value = pattern[1:-1].lower()
                return search_value in value_str.lower()
            
            # Starts with: value*
            if pattern.endswith('*') and not pattern.startswith('*'):
                search_value = pattern[:-1].lower()
                return value_str.lower().startswith(search_value)
            
            # Ends with: *value
            if pattern.startswith('*') and not pattern.endswith('*'):
                search_value = pattern[1:].lower()
                return value_str.lower().endswith(search_value)
            
            # Boolean filters (check before converting to string)
            pattern_lower = pattern.lower()
            if pattern_lower in ['true', '1']:
                bool_value = self._convert_to_bool(value)
                return bool_value is True
            elif pattern_lower in ['false', '0']:
                bool_value = self._convert_to_bool(value)
                return bool_value is False
            
            # Plain string match (case-insensitive substring for backward compatibility)
            # But also support exact match if no wildcards
            if '*' in pattern or '?' in pattern or '[' in pattern:
                # Use fnmatch for wildcard patterns
                return fnmatch.fnmatch(value_str, pattern)
            else:
                # For plain strings, do case-insensitive substring match
                # This allows filtering by partial matches (e.g., "loc1" matches "loc1", "loc1-policy", etc.)
                return pattern.lower() in value_str.lower()
                
        except Exception as e:
            # If parsing fails, fall back to simple substring match
            logging.debug(f"Filter pattern parsing failed for '{pattern}': {e}, using substring match")
            value_str = str(value)
            return pattern.lower() in value_str.lower()
    
    def _convert_to_bytes(self, value: Any) -> Optional[int]:
        """Convert a value to bytes for capacity comparison"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            # Assume it's already in bytes
            return int(value)
        
        if isinstance(value, str):
            # Try to parse capacity string (e.g., "1TB", "500GB")
            try:
                from .utils import parse_capacity_value
                _, bytes_value = parse_capacity_value(value)
                return bytes_value
            except (ValueError, Exception):
                # Not a capacity string, try to parse as number
                try:
                    return int(float(value))
                except (ValueError, TypeError):
                    return None
        
        return None
    
    def _convert_to_number(self, value: Any) -> Optional[float]:
        """Convert a value to a number for numeric comparison"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        return None
    
    def _convert_to_bool(self, value: Any) -> Optional[bool]:
        """Convert a value to boolean for boolean comparison"""
        if value is None:
            return None
        
        if isinstance(value, bool):
            return value
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        if isinstance(value, str):
            value_lower = value.lower()
            if value_lower in ('true', '1', 'yes', 'on'):
                return True
            elif value_lower in ('false', '0', 'no', 'off', ''):
                return False
            # Non-empty string is truthy
            return len(value) > 0
        
        return bool(value)
    
    def _join_data(self, command_name: str, api_data: Dict[str, List[Dict]]) -> List[Dict]:
        """Join data from multiple API calls"""
        api_endpoints = self.template_parser.get_api_endpoints(command_name)
        
        if len(api_endpoints) == 0:
            return []
        
        if len(api_endpoints) == 1:
            # No join needed
            return api_data.get(api_endpoints[0], [])
        
        # Multi-API join
        # Start with first API as base
        base_endpoint = api_endpoints[0]
        result = api_data.get(base_endpoint, [])
        
        # Join with other APIs
        fields = self.template_parser.get_fields(command_name)
        
        for field_config in fields:
            source_field = field_config.get('field', '')
            if source_field and '.' in source_field:
                # This is a join field (e.g., quotas.hard_limit)
                parts = source_field.split('.', 1)
                join_endpoint = parts[0]
                join_field = parts[1]
                join_on_config = field_config.get('join_on')
                
                if join_endpoint in api_data and join_on_config:
                    # Parse join_on configuration
                    # Support both old format (string) and new format (dict)
                    if isinstance(join_on_config, str):
                        # Old format: join_on: "field_name" (same field name in both endpoints)
                        base_field = join_on_config
                        joined_field = join_on_config
                    elif isinstance(join_on_config, dict):
                        # New format: join_on: {field: "base_field", on_field: "joined_field", act_on: "first|last|all"}
                        base_field = join_on_config.get('field')
                        joined_field = join_on_config.get('on_field')
                        act_on = join_on_config.get('act_on', 'first').lower()  # Default to 'first'
                        if not base_field or not joined_field:
                            logging.warning(f"Invalid join_on configuration for field '{source_field}': missing 'field' or 'on_field'")
                            continue
                    else:
                        logging.warning(f"Invalid join_on configuration for field '{source_field}': must be string or dict")
                        continue
                    
                    # Perform left join
                    join_data = api_data[join_endpoint]
                    
                    # Build index based on act_on behavior
                    if act_on == 'first':
                        # Keep first match only
                        join_index = {}
                        for item in join_data:
                            if joined_field in item:
                                key = item[joined_field]
                                if key not in join_index:
                                    join_index[key] = item
                    elif act_on == 'last':
                        # Keep last match (current behavior - dict overwrites)
                        join_index = {item[joined_field]: item for item in join_data if joined_field in item}
                    elif act_on == 'all':
                        # Keep all matches as a list
                        join_index = {}
                        for item in join_data:
                            if joined_field in item:
                                key = item[joined_field]
                                if key not in join_index:
                                    join_index[key] = []
                                join_index[key].append(item)
                    else:
                        logging.warning(f"Invalid act_on value '{act_on}' for field '{source_field}', defaulting to 'first'")
                        act_on = 'first'
                        join_index = {}
                        for item in join_data:
                            if joined_field in item:
                                key = item[joined_field]
                                if key not in join_index:
                                    join_index[key] = item
                    
                    for row in result:
                        # Match using base endpoint's field
                        if base_field in row:
                            joined_items = join_index.get(row[base_field])
                            if joined_items:
                                if act_on == 'all':
                                    # Store as list of dicts
                                    row[join_endpoint] = joined_items
                                else:
                                    # Store as single dict
                                    if join_endpoint not in row:
                                        row[join_endpoint] = {}
                                    row[join_endpoint].update(joined_items)
        
        return result
    
    def _execute_per_row_endpoints(self, command_name: str, base_data: List[Dict], cli_args: Dict) -> List[Dict]:
        """Execute per-row API endpoints for each row in base data
        
        For each row, makes individual API calls to per-row endpoints with query parameters
        derived from the row data. Query parameters can reference row fields using $field_name syntax.
        
        Args:
            command_name: Name of the command
            base_data: List of row dictionaries from base endpoint
            cli_args: CLI arguments (for potential future use)
        
        Returns:
            Modified base_data with per-row endpoint results merged into each row
        """
        per_row_endpoints = self.template_parser.get_per_row_endpoints(command_name)
        if not per_row_endpoints:
            return base_data
        
        from .config import REST_PAGE_SIZE
        import urllib.parse
        
        for row in base_data:
            for endpoint_config in per_row_endpoints:
                endpoint_name = endpoint_config.get('name')
                query_list = endpoint_config.get('query', [])
                
                if not endpoint_name:
                    logging.warning(f"Skipping per-row endpoint with missing name")
                    continue
                
                # Parse query parameters from row data
                api_params = {}
                for query_item in query_list:
                    if '=' not in query_item:
                        logging.warning(f"Invalid query format '{query_item}' for per-row endpoint '{endpoint_name}', skipping")
                        continue
                    
                    key, value = query_item.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    # Check if value references a row field (starts with $)
                    if value.startswith('$'):
                        # Extract field name (remove $ prefix)
                        field_name = value[1:]
                        if field_name in row:
                            api_params[key] = row[field_name]
                        else:
                            logging.warning(f"Field '{field_name}' not found in row for per-row endpoint '{endpoint_name}', skipping parameter '{key}'")
                            continue
                    else:
                        # Literal value
                        api_params[key] = value
                
                # Make API call for this row
                try:
                    # Get whitelist from template parser
                    whitelist = self.template_parser.get_api_whitelist()
                    
                    # Handle tenant_id separately if present
                    request_params = api_params.copy()
                    tenant_id = request_params.pop('tenant_id', None)
                    
                    # Use unified API call function
                    results = call_vast_api(
                        client=self.client,
                        endpoint=endpoint_name,
                        method='get',
                        params=request_params,
                        tenant_id=tenant_id,
                        whitelist=whitelist
                    )
                    
                    # Handle response structure (per-row calls typically return single objects)
                    if len(results) == 1:
                        row[endpoint_name] = results[0]
                    elif len(results) > 1:
                        # Multiple results - store as list
                        row[endpoint_name] = results
                    else:
                        # No results
                        row[endpoint_name] = None
                    
                except ValueError as e:
                    # Whitelist validation error - log and skip this endpoint for this row
                    logging.error(f"Access denied for per-row endpoint '{endpoint_name}': {e}")
                    row[endpoint_name] = None
                except Exception as e:
                    logging.error(f"Error calling per-row endpoint '{endpoint_name}' for row: {e}")
                    # Continue with next row even if this endpoint fails
                    row[endpoint_name] = None
        
        return base_data
    
    def _transform_fields(self, command_name: str, data: List[Dict], cli_args: Dict) -> List[Dict]:
        """Transform fields according to template
        
        Also stores raw field values (prefixed with _raw_) for later ordering
        when combining results from multiple clusters.
        """
        fields = self.template_parser.get_fields(command_name)
        transformed = []
        
        for row in data:
            new_row = {}
            
            # Track conditional fields: field_name -> list of (field_config, condition_result)
            conditional_fields = {}
            
            # First pass: process all fields (including hidden ones for value expressions)
            # Hidden fields will be processed but not added to new_row
            for field_config in fields:
                field_name = field_config['name']
                source_field = field_config.get('field', field_name)
                is_hidden = field_config.get('hide', False)
                
                # Check if field has a value expression (f-string)
                # If present, evaluate it instead of reading from source field
                if 'value' in field_config:
                    # Evaluate expression using row data
                    value = self._evaluate_field_expression(field_config['value'], row)
                    raw_value = value  # For expression-based fields, raw value is the evaluated result
                else:
                    # No expression, use existing logic to get value from source field
                    # This will be set below in the existing code
                    value = None
                    raw_value = None
                
                # Check condition before processing field
                if 'condition' in field_config:
                    condition_result = self._evaluate_condition(field_config['condition'], row, field_name)
                    if not condition_result:
                        # Condition evaluates to False, track it but skip processing for now
                        logging.debug(f"Conditional field '{field_name}': condition evaluated to False, skipping field")
                        if field_name not in conditional_fields:
                            conditional_fields[field_name] = []
                        conditional_fields[field_name].append((field_config, False))
                        continue
                    else:
                        # Condition evaluates to True, process normally
                        logging.debug(f"Conditional field '{field_name}': condition evaluated to True, including field")
                        conditional_fields[field_name] = [(field_config, True)]
                else:
                    # Non-conditional field, process normally
                    # If this field name was in conditional_fields, clear it (non-conditional takes precedence)
                    if field_name in conditional_fields:
                        del conditional_fields[field_name]
                
                # Only get value from source field if we don't have a value expression
                if value is None:
                    # Handle CLI parameter fields $(param)
                    if source_field.startswith('$(') and source_field.endswith(')'):
                        param_name = source_field[2:-1]
                        # Try both singular and plural forms (cluster/clusters)
                        value = cli_args.get(param_name) or cli_args.get(param_name + 's', '')
                        # If no value found and it's cluster, use the executor's cluster
                        if param_name == 'cluster' and not value:
                            value = self.cluster
                        raw_value = value  # CLI params don't have raw values
                    
                    # Handle joined fields (e.g., quotas.hard_limit)
                    elif '.' in source_field:
                        parts = source_field.split('.')
                        value = row
                        
                        # Check if first part is a list (act_on: all case)
                        if len(parts) > 0 and isinstance(value, dict):
                            first_part = parts[0]
                            first_value = value.get(first_part)
                            
                            if isinstance(first_value, list):
                                # act_on: all - aggregate values from all items
                                if len(parts) == 1:
                                    # Single part - just return the list (unlikely but handle it)
                                    value = first_value
                                else:
                                    # Multiple parts - extract field from each item and aggregate
                                    aggregated_values = []
                                    remaining_parts = parts[1:]
                                    for item in first_value:
                                        if isinstance(item, dict):
                                            nested_value = item
                                            for part in remaining_parts:
                                                if isinstance(nested_value, dict):
                                                    nested_value = nested_value.get(part)
                                                else:
                                                    nested_value = None
                                                    break
                                            if nested_value is not None:
                                                aggregated_values.append(str(nested_value))
                                    value = '\n'.join(aggregated_values) if aggregated_values else None
                            else:
                                # Normal dict traversal
                                for part in parts:
                                    if isinstance(value, dict):
                                        value = value.get(part, {})
                                    else:
                                        value = None
                                        break
                        else:
                            # Normal dict traversal
                            for part in parts:
                                if isinstance(value, dict):
                                    value = value.get(part, {})
                                else:
                                    value = None
                                    break
                        
                        # If we end up with an empty dict, treat it as None
                        if isinstance(value, dict) and len(value) == 0:
                            value = None
                        raw_value = value
                    
                    # Handle regular fields
                    # For argument fields, the 'field' property is the API parameter name,
                    # but the response field might be the field name. Try both.
                    else:
                        # First try the source_field (API parameter name)
                        value = row.get(source_field)
                        # If not found and source_field != field_name, try field_name (response field name)
                        if value is None and source_field != field_name:
                            value = row.get(field_name)
                        raw_value = value
                
                # Store raw value for later ordering (when combining multiple clusters)
                # Use field name with _raw_ prefix, normalized to underscores for consistency
                # Store even if None (for proper sorting of None values)
                # Only store raw value in new_row if not hidden
                if not is_hidden:
                    # Normalize field name to underscores for consistent lookup during sorting
                    normalized_field_name = normalize_field_name(field_name, 'to_underscore')
                    new_row[f'_raw_{normalized_field_name}'] = raw_value
                
                # Apply transformations
                if value is not None:
                    # jq transformation
                    if 'jq' in field_config:
                        value = self._apply_jq(value, field_config['jq'])
                    
                    # Unit conversion
                    if 'convert' in field_config:
                        value = self._convert_units(value, field_config['convert'])
                    
                    # Column width truncation (for display purposes, store original in metadata)
                    if 'limit_table_column_width' in field_config:
                        width = field_config['limit_table_column_width']
                        if isinstance(value, str) and len(value) > width:
                            # Store truncated version, but keep original for calculations
                            value = value[:width-3] + '...' if width > 3 else value[:width]
                
                # Store transformed value in row for use in value expressions (even if hidden)
                # This allows hidden fields to be referenced in value expressions after jq transformation
                row[field_name] = value
                
                # Only add to new_row if not hidden
                if not is_hidden:
                    new_row[field_name] = value
            
            # Second pass: handle conditional fields where all conditions failed
            # Add them with empty values so the field appears in output
            for field_name, condition_results in conditional_fields.items():
                # Check if all conditions evaluated to False (no True results)
                all_false = all(not result for _, result in condition_results)
                if all_false and field_name not in new_row:
                    # All conditions failed and field wasn't added, add it with empty value
                    logging.debug(f"Conditional field '{field_name}': all conditions evaluated to False, adding field with empty value")
                    new_row[field_name] = None
                    # Normalize field name to underscores for consistent lookup during sorting
                    normalized_field_name = normalize_field_name(field_name, 'to_underscore')
                    new_row[f'_raw_{normalized_field_name}'] = None
            
            transformed.append(new_row)
        
        return transformed
    
    def _apply_jq(self, value: Any, jq_expr: str) -> Any:
        """Apply jq expression to value using the system jq command"""
        # Check if jq is available
        if not check_jq_available():
            logging.error(
                "jq command not found. jq is required for field transformations. "
                "Please install jq: https://github.com/jqlang/jq/releases"
            )
            # Fall back to simple join for common case
            import re
            if isinstance(value, list) and 'join' in jq_expr:
                # Try to extract separator for join() as fallback
                match = re.search(r'join\s*\(\s*\\?["\']([^"\'\\]+)\\?["\']\s*\)', jq_expr)
                if not match:
                    match = re.search(r'join\s*\(\s*["\']([^"\']+)["\']\s*\)', jq_expr)
                if match:
                    separator = match.group(1)
                    return separator.join(str(v) for v in value)
            return value
        
        try:
            # Unescape the jq expression (YAML may have escaped quotes like join(\",\"))
            # Replace \" with " and \' with '
            jq_expr_unescaped = jq_expr.replace('\\"', '"').replace("\\'", "'")
            
            # Convert value to JSON string
            input_json = json.dumps(value)
            
            # Debug logging: log jq command details
            input_preview = input_json[:200] + "..." if len(input_json) > 200 else input_json
            logging.debug(f"Executing jq command: expression='{jq_expr_unescaped}', input_length={len(input_json)} bytes, input_preview={input_preview}")
            
            # Run jq command
            result = subprocess.run(
                ["jq", jq_expr_unescaped],
                input=input_json.encode(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=JQ_TIMEOUT_SECONDS
            )
            
            # Debug logging: log jq command result
            if result.returncode == 0:
                output = result.stdout.decode('utf-8', errors='ignore').strip()
                output_preview = output[:200] + "..." if len(output) > 200 else output
                logging.debug(f"jq command succeeded: output_length={len(output)} bytes, output_preview={output_preview}")
            else:
                error_msg = result.stderr.decode('utf-8', errors='ignore')
                logging.debug(f"jq command failed: returncode={result.returncode}, stderr={error_msg}")
            
            if result.returncode != 0:
                # jq failed, log error and return original value
                error_msg = result.stderr.decode('utf-8', errors='ignore')
                logging.warning(f"jq expression '{jq_expr}' failed: {error_msg}. Returning original value.")
                return value
            
            # Parse the output
            output = result.stdout.decode('utf-8', errors='ignore').strip()
            
            # Try to parse as JSON, fall back to string if not valid JSON
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                # If output is not valid JSON (e.g., a string), return as-is
                # Remove surrounding quotes if present
                if output.startswith('"') and output.endswith('"'):
                    return output[1:-1]
                return output
                
        except subprocess.TimeoutExpired:
            logging.warning(f"jq expression '{jq_expr}' timed out. Returning original value.")
            return value
        except FileNotFoundError:
            # jq command not found (shouldn't happen if check_jq_available passed)
            logging.error("jq command not found. Please install jq: https://github.com/jqlang/jq/releases")
            return value
        except Exception as e:
            logging.warning(f"Error applying jq expression '{jq_expr}': {e}. Returning original value.")
            return value
    
    def _evaluate_condition(self, condition_config: Dict, row: Dict, field_name_for_logging: str = None) -> bool:
        """Evaluate a condition against row data
        
        Args:
            condition_config: Dictionary with 'field', 'operator', and 'value' keys
            row: Row data dictionary to evaluate condition against
            field_name_for_logging: Optional field name for debug logging
        
        Returns:
            True if condition is met, False otherwise
        """
        condition_field = condition_config.get('field')
        operator = condition_config.get('operator')
        condition_value = condition_config.get('value')
        
        if not condition_field or not operator or 'value' not in condition_config:
            logging.warning(f"Invalid condition configuration: {condition_config}")
            return False
        
        # Get field value from row
        if condition_field not in row:
            # Field doesn't exist in row, condition evaluates to False
            log_msg = f"Condition evaluation: field '{condition_field}' not found in row"
            if field_name_for_logging:
                log_msg = f"Conditional field '{field_name_for_logging}': {log_msg}"
            logging.debug(log_msg)
            return False
        
        field_value = row[condition_field]
        
        # Handle None values
        if field_value is None:
            # Only equals with None value should match
            if operator == 'equals' and condition_value is None:
                log_msg = f"Condition evaluation: {condition_field} (None) {operator} {condition_value} -> True"
                if field_name_for_logging:
                    log_msg = f"Conditional field '{field_name_for_logging}': {log_msg}"
                logging.debug(log_msg)
                return True
            log_msg = f"Condition evaluation: {condition_field} (None) {operator} {condition_value} -> False"
            if field_name_for_logging:
                log_msg = f"Conditional field '{field_name_for_logging}': {log_msg}"
            logging.debug(log_msg)
            return False
        
        # Determine field type
        field_type = self._detect_field_type(field_value)
        
        # Convert condition value to match field type
        try:
            converted_value = self._convert_value(condition_value, field_type)
        except (ValueError, TypeError) as e:
            logging.warning(f"Could not convert condition value '{condition_value}' to {field_type}: {e}. Using string comparison.")
            converted_value = str(condition_value)
            field_type = 'str'
        
        # Apply operator
        result = self._apply_operator(operator, field_value, converted_value, field_type)
        
        # Debug logging
        log_msg = f"Condition evaluation: {condition_field} ({field_type}={field_value!r}) {operator} {condition_value!r} -> {result}"
        if field_name_for_logging:
            log_msg = f"Conditional field '{field_name_for_logging}': {log_msg}"
        logging.debug(log_msg)
        
        return result
    
    def _detect_field_type(self, value: Any) -> str:
        """Detect the type of a field value
        
        Returns:
            Type string: 'bool', 'int', 'datetime', or 'str'
        """
        if isinstance(value, bool):
            return 'bool'
        if isinstance(value, int):
            return 'int'
        if isinstance(value, float):
            return 'int'  # Treat float as int for comparison
        if isinstance(value, datetime):
            return 'datetime'
        if isinstance(value, str):
            # Try to detect if it's a boolean string
            if value.lower() in ('true', 'false'):
                return 'bool'
            # Try to detect if it's a number
            try:
                int(value)
                return 'int'
            except ValueError:
                pass
            # Try to detect if it's a datetime (ISO format)
            try:
                datetime.fromisoformat(value.replace('Z', '+00:00'))
                return 'datetime'
            except (ValueError, AttributeError):
                pass
        return 'str'
    
    def _convert_value(self, value: Any, target_type: str) -> Any:
        """Convert a value to the target type
        
        Args:
            value: Value to convert
            target_type: Target type ('bool', 'int', 'datetime', 'str')
        
        Returns:
            Converted value
        """
        if target_type == 'bool':
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        
        if target_type == 'int':
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str):
                return int(value)
            raise ValueError(f"Cannot convert {value} to int")
        
        if target_type == 'datetime':
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            raise ValueError(f"Cannot convert {value} to datetime")
        
        # Default to string
        return str(value)
    
    def _apply_operator(self, operator: str, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Apply an operator to compare field value with condition value
        
        Args:
            operator: Operator name (equals, not_equals, etc. or aliases like ==, !=, >, <)
            field_value: Value from the row
            condition_value: Value from condition config (already converted)
            field_type: Type of the field ('str', 'int', 'bool', 'datetime')
        
        Returns:
            True if condition is met, False otherwise
        """
        # Normalize operator aliases to canonical names
        operator_aliases = {
            'eq': 'equals',
            '==': 'equals',
            'ne': 'not_equals',
            '!=': 'not_equals',
            'gt': 'greater_than',
            '>': 'greater_than',
            'lt': 'less_than',
            '<': 'less_than',
            'gte': 'greater_equal',
            '>=': 'greater_equal',
            'lte': 'less_equal',
            '<=': 'less_equal',
        }
        
        # Convert alias to canonical name
        canonical_operator = operator_aliases.get(operator, operator)
        
        operator_map = {
            'equals': self._eval_equals,
            'not_equals': self._eval_not_equals,
            'greater_than': self._eval_greater_than,
            'less_than': self._eval_less_than,
            'greater_equal': self._eval_greater_equal,
            'less_equal': self._eval_less_equal,
            'contains': self._eval_contains,
            'starts_with': self._eval_starts_with,
            'ends_with': self._eval_ends_with,
            'in': self._eval_in,
            'regex': self._eval_regex,
        }
        
        if canonical_operator not in operator_map:
            logging.warning(f"Unknown operator '{operator}', treating as string equals")
            return str(field_value) == str(condition_value)
        
        eval_func = operator_map[canonical_operator]
        return eval_func(field_value, condition_value, field_type)
    
    def _eval_equals(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate equals operator"""
        return field_value == condition_value
    
    def _eval_not_equals(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate not_equals operator"""
        return field_value != condition_value
    
    def _eval_greater_than(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate greater_than operator (for numeric and datetime types)"""
        if field_type not in ('int', 'datetime'):
            logging.warning(f"greater_than operator not supported for type '{field_type}', treating as string comparison")
            return str(field_value) > str(condition_value)
        return field_value > condition_value
    
    def _eval_less_than(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate less_than operator (for numeric and datetime types)"""
        if field_type not in ('int', 'datetime'):
            logging.warning(f"less_than operator not supported for type '{field_type}', treating as string comparison")
            return str(field_value) < str(condition_value)
        return field_value < condition_value
    
    def _eval_greater_equal(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate greater_equal operator (for numeric and datetime types)"""
        if field_type not in ('int', 'datetime'):
            logging.warning(f"greater_equal operator not supported for type '{field_type}', treating as string comparison")
            return str(field_value) >= str(condition_value)
        return field_value >= condition_value
    
    def _eval_less_equal(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate less_equal operator (for numeric and datetime types)"""
        if field_type not in ('int', 'datetime'):
            logging.warning(f"less_equal operator not supported for type '{field_type}', treating as string comparison")
            return str(field_value) <= str(condition_value)
        return field_value <= condition_value
    
    def _eval_contains(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate contains operator (for string type)"""
        if field_type != 'str':
            logging.warning(f"contains operator not supported for type '{field_type}', converting to string")
        field_str = str(field_value)
        condition_str = str(condition_value)
        return condition_str in field_str
    
    def _eval_starts_with(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate starts_with operator (for string type)"""
        if field_type != 'str':
            logging.warning(f"starts_with operator not supported for type '{field_type}', converting to string")
        field_str = str(field_value)
        condition_str = str(condition_value)
        return field_str.startswith(condition_str)
    
    def _eval_ends_with(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate ends_with operator (for string type)"""
        if field_type != 'str':
            logging.warning(f"ends_with operator not supported for type '{field_type}', converting to string")
        field_str = str(field_value)
        condition_str = str(condition_value)
        return field_str.endswith(condition_str)
    
    def _eval_in(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate in operator
        
        If condition_value is a list, checks if field_value is in the list.
        If condition_value is a string, treats it as comma-separated values or checks substring.
        """
        if isinstance(condition_value, list):
            return field_value in condition_value
        
        if isinstance(condition_value, str):
            # Check if it's a comma-separated list
            if ',' in condition_value:
                values = [v.strip() for v in condition_value.split(',')]
                return field_value in values
            # Otherwise check substring
            return str(condition_value) in str(field_value)
        
        # For other types, do direct comparison
        return field_value == condition_value
    
    def _eval_regex(self, field_value: Any, condition_value: Any, field_type: str) -> bool:
        """Evaluate regex operator (for string type)"""
        if field_type != 'str':
            logging.warning(f"regex operator not supported for type '{field_type}', converting to string")
        field_str = str(field_value)
        condition_str = str(condition_value)
        try:
            return bool(re.match(condition_str, field_str))
        except re.error as e:
            logging.warning(f"Invalid regex pattern '{condition_str}': {e}")
            return False
    
    def _evaluate_field_expression(self, expression: str, row: Dict[str, Any]) -> Any:
        """Evaluate a field value expression (f-string) in a safe context
        
        Args:
            expression: F-string expression (e.g., 'f"async/{lower(role)}"')
            row: Row data dictionary with field values available as variables
            
        Returns:
            Evaluated expression result, or None on error
        """
        if not expression or not expression.strip():
            return None
        
        try:
            # Create safe evaluation context with helper functions
            # String utility functions
            def lower(s):
                """Convert string to lowercase"""
                return str(s).lower() if s is not None else ""
            
            def upper(s):
                """Convert string to uppercase"""
                return str(s).upper() if s is not None else ""
            
            def concat(*args):
                """Concatenate multiple strings"""
                return ''.join(str(arg) if arg is not None else '' for arg in args)
            
            def replace(s, old, new):
                """Replace occurrences of old with new in string"""
                return str(s).replace(str(old), str(new)) if s is not None else ""
            
            def substring(s, start, end=None):
                """Extract substring from start to end (or to end of string if end is None)"""
                if s is None:
                    return ""
                s_str = str(s)
                if end is None:
                    return s_str[start:]
                return s_str[start:end]
            
            def strip(s):
                """Remove leading and trailing whitespace"""
                return str(s).strip() if s is not None else ""
            
            def join(separator, *args):
                """Join strings with separator"""
                return str(separator).join(str(arg) if arg is not None else '' for arg in args)
            
            # Prepare row data as variables (normalize field names - handle spaces/underscores)
            # Make all field values available as variables
            eval_locals = {}
            for key, val in row.items():
                # Use both original key and normalized versions (with/without spaces/underscores)
                eval_locals[key] = val
                # Normalize: replace spaces with underscores and vice versa
                normalized_key = to_python_name(key)
                if normalized_key != key:
                    eval_locals[normalized_key] = val
                normalized_key2 = key.replace('_', ' ')
                if normalized_key2 != key and normalized_key2 not in eval_locals:
                    eval_locals[normalized_key2] = val
            
            # Add helper functions to evaluation context
            eval_locals.update({
                'lower': lower,
                'upper': upper,
                'concat': concat,
                'replace': replace,
                'substring': substring,
                'strip': strip,
                'join': join,
            })
            
            # Safe built-ins only
            safe_builtins = {
                'str': str,
                'int': int,
                'float': float,
                'len': len,
                'bool': bool,
                'None': None,
                'True': True,
                'False': False,
            }
            
            # Evaluate the f-string expression
            # The expression should already be an f-string (starts with f" or f')
            # We'll compile and evaluate it in the safe context
            try:
                # Compile the expression
                compiled = compile(expression, '<string>', 'eval')
                # Evaluate with restricted globals and locals
                result = eval(compiled, {'__builtins__': safe_builtins}, eval_locals)
                return result
            except SyntaxError as e:
                logging.warning(f"Syntax error in field expression '{expression}': {e}")
                return None
            except NameError as e:
                logging.warning(f"Name error in field expression '{expression}': {e}. Available fields: {list(row.keys())}")
                return None
            except Exception as e:
                logging.warning(f"Error evaluating field expression '{expression}': {e}")
                return None
                
        except Exception as e:
            logging.error(f"Unexpected error evaluating field expression '{expression}': {e}")
            return None
    
    def _convert_units(self, value: Any, unit: str) -> str:
        """Convert value to specified unit
        
        Supports:
        - Capacity units: B, KB, MB, GB, TB, PB, AUTO
        - Time units: time_delta (converts ISO timestamp to "Xd Xh Xm Xs ago" or "in Xd Xh Xm Xs")
        """
        # Handle time_delta conversion
        if unit == 'time_delta':
            if value is None:
                return None
            # Convert timestamp string to time delta format
            return format_time_delta(str(value))
        
        # Handle capacity conversions (bytes)
        try:
            bytes_value = int(value)
        except (ValueError, TypeError):
            return str(value)
        
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        
        if unit == 'AUTO':
            # Find best fit
            unit_index = 0
            size = float(bytes_value)
            while size >= 1024 and unit_index < len(units) - 1:
                size /= 1024
                unit_index += 1
            return f"{size:.2f} {units[unit_index]}"
        
        # Specific unit
        unit_map = {'B': 0, 'KB': 1, 'MB': 2, 'GB': 3, 'TB': 4, 'PB': 5}
        if unit in unit_map:
            divisor = 1024 ** unit_map[unit]
            converted = bytes_value / divisor
            return f"{converted:.2f} {unit}"
        
        return str(bytes_value)
    
    def _apply_ordering(self, command_name: str, data: List[Dict], cli_args: Dict[str, Any]) -> List[Dict]:
        """Apply ordering from CLI arguments
        
        Orders data BEFORE transformation, so it sorts by raw numeric values
        (e.g., bytes) rather than converted strings (e.g., "76.76 TB").
        
        Order format: "field:direction" or "field" (defaults to asc)
        Direction can be: asc, ascending, dec, descending (or prefixes)
        Can specify multiple orders: ["field1:dec", "field2:asc"]
        
        Field names can be either:
        - Raw API field names (e.g., "logical_capacity")
        - Transformed field names (e.g., "logical used") - will be mapped to raw field
        """
        order_arg = cli_args.get('order')
        if not order_arg:
            return data
        
        # Parse order argument - can be string or list
        if isinstance(order_arg, str):
            # Handle comma-separated or single order
            order_specs = [o.strip() for o in order_arg.split(',')]
        elif isinstance(order_arg, list):
            order_specs = order_arg
        else:
            return data
        
        # Get field mappings from template to resolve transformed field names
        fields = self.template_parser.get_fields(command_name)
        field_name_to_raw = {}
        for field_config in fields:
            field_name = field_config.get('name')
            raw_field = field_config.get('field', field_name)
            if field_name:
                field_name_to_raw[field_name] = raw_field
        
        # Parse order specifications using shared utility
        order_configs = []
        for order_spec in order_specs:
            order_config = parse_order_spec(order_spec, field_mappings=field_name_to_raw, use_raw_prefix=False)
            if order_config:
                order_configs.append(order_config)
        
        # Apply ordering using shared utility
        return apply_ordering(data, order_configs, remove_raw_fields=False)
    
    def _apply_top_limit(self, data: List[Dict], cli_args: Dict[str, Any]) -> List[Dict]:
        """Apply top limit to results"""
        top_arg = cli_args.get('top')
        if not top_arg:
            return data
        
        try:
            top = int(top_arg)
            if top > 0:
                return data[:top]
        except (ValueError, TypeError):
            # Invalid top value, return all data
            pass
        
        return data
    
    
    def _ensure_field_order(self, command_name: str, data: List[Dict]) -> List[Dict]:
        """Ensure field order matches YAML definition order and filter out undefined fields
        
        Preserves _raw_ fields for later ordering when combining multiple clusters.
        Normalizes field names to snake_case for consistent output keys.
        """
        if not data:
            return data
        
        fields = self.template_parser.get_fields(command_name)
        # Get field names in order, excluding hidden fields
        field_order = [f['name'] for f in fields if not f.get('hide', False)]
        
        # Build mapping from original field names to normalized names
        field_name_mapping = {}
        for field_name in field_order:
            normalized_name = normalize_field_name(field_name, 'to_underscore')
            if normalized_name != field_name:
                field_name_mapping[field_name] = normalized_name
        
        # Reorder each row's fields - only include fields defined in template
        # Also preserve _raw_ fields (used for ordering when combining clusters)
        # Normalize field names to snake_case
        reordered_data = []
        for row in data:
            new_row = {}
            # Add fields in YAML order (only fields defined in template)
            for field_name in field_order:
                if field_name in row:
                    # Use normalized name for output
                    normalized_name = field_name_mapping.get(field_name, field_name)
                    new_row[normalized_name] = row[field_name]
            # Preserve _raw_ fields for later ordering (also normalize their names)
            for key in row.keys():
                if key.startswith('_raw_'):
                    # Normalize the field name part (after _raw_ prefix)
                    original_field = key[5:]  # Remove '_raw_' prefix
                    normalized_field = normalize_field_name(original_field, 'to_underscore')
                    new_row[f'_raw_{normalized_field}'] = row[key]
            # Preserve 'instance' field if present (added by --instance flag)
            if 'instance' in row:
                new_row['instance'] = row['instance']
            reordered_data.append(new_row)
        
        return reordered_data

