"""Template Parser - Loads and validates list_template.yaml files
Parses the template structure for dynamic CLI commands with:
- Multi-API calls
- Field transformations (jq, unit conversion)
- Joins between APIs
- Argument validation and API mapping
"""

import yaml
import re
import logging
import os
from typing import Dict, List, Any, Optional
from pathlib import Path


class TemplateParser:
    """Parses and validates command templates"""
    
    def __init__(self, template_modifications_path: str, default_template_path: Optional[str] = None):
        self.template_modifications_path = template_modifications_path
        self.template_path = template_modifications_path  # Keep for backward compatibility
        self.default_template_path = default_template_path
        
        # Merge default template and user modifications
        self._merged_data = self._merge_template_files(default_template_path, template_modifications_path)
        
        # Initialize variables (replaces replacements)
        self.variables = {}
        # Initialize API whitelist (empty dict means deny all)
        self.api_whitelist = {}
        # Load templates (includes variables, field_anchors, and list_cmds)
        self.templates = self._load_templates()
        # Load API whitelist
        self.api_whitelist = self._load_api_whitelist()
        # Load merged commands (merged_list_cmds section)
        self.merged_commands = self._load_merged_commands()
        # Validate templates after loading
        self._validate_templates()
    
    def get_command_names(self) -> List[str]:
        """Get list of available command names (excludes YAML anchors starting with _)"""
        return [name for name in self.templates.keys() if not name.startswith('_')]
    
    def get_command_template(self, command_name: str) -> Optional[Dict]:
        """Get template for a specific command"""
        return self.templates.get(command_name)
    
    def get_merged_command_names(self) -> List[str]:
        """Get list of merged command names from merged_list_cmds section"""
        return list(self.merged_commands.keys())
    
    def get_merged_command_template(self, merged_name: str) -> Optional[Dict]:
        """Get merged command config (name, functions list, description)"""
        return self.merged_commands.get(merged_name)
    
    def get_merged_arguments(self, merged_name: str) -> List[Dict]:
        """Merge arguments from all source functions using union (unique arguments only)"""
        merged_template = self.get_merged_command_template(merged_name)
        if not merged_template:
            return []
        
        source_functions = merged_template.get('functions', [])
        if not source_functions:
            return []
        
        # Collect all arguments from all source functions
        all_args = {}  # name -> arg_config (first occurrence wins)
        seen_names = set()
        
        for func_name in source_functions:
            func_args = self.get_arguments(func_name)
            for arg in func_args:
                arg_name = arg.get('name')
                if not arg_name:
                    continue
                
                # Normalize name (handle spaces/underscores)
                normalized_name = arg_name.replace(' ', '_')
                
                if normalized_name not in seen_names:
                    seen_names.add(normalized_name)
                    all_args[normalized_name] = arg.copy()
                else:
                    # Argument already exists, log warning if configurations differ significantly
                    existing = all_args[normalized_name]
                    if (existing.get('type') != arg.get('type') or 
                        existing.get('mandatory') != arg.get('mandatory')):
                        logging.warning(
                            f"Merged command '{merged_name}': argument '{arg_name}' has conflicting "
                            f"configurations between source functions. Using first occurrence."
                        )
        
        # Return as list, preserving order: first function's arguments first, then unique from others
        result = []
        seen_in_result = set()
        
        for func_name in source_functions:
            func_args = self.get_arguments(func_name)
            for arg in func_args:
                arg_name = arg.get('name')
                if not arg_name:
                    continue
                normalized_name = arg_name.replace(' ', '_')
                if normalized_name not in seen_in_result:
                    seen_in_result.add(normalized_name)
                    result.append(all_args[normalized_name])
        
        return result
    
    def get_merged_fields(self, merged_name: str) -> List[str]:
        """Merge fields - all from first function, then unique fields from others"""
        merged_template = self.get_merged_command_template(merged_name)
        if not merged_template:
            return []
        
        source_functions = merged_template.get('functions', [])
        if not source_functions:
            return []
        
        # Get all fields from first function (in original order)
        first_func_fields = self.get_fields(source_functions[0])
        field_names = []
        seen_field_names = set()
        
        # Add all fields from first function
        for field in first_func_fields:
            field_name = field.get('name') or field.get('header')
            if field_name:
                normalized_name = field_name.replace(' ', '_')
                if normalized_name not in seen_field_names:
                    seen_field_names.add(normalized_name)
                    field_names.append(normalized_name)
        
        # Add unique fields from other functions
        for func_name in source_functions[1:]:
            func_fields = self.get_fields(func_name)
            for field in func_fields:
                field_name = field.get('name') or field.get('header')
                if field_name:
                    normalized_name = field_name.replace(' ', '_')
                    if normalized_name not in seen_field_names:
                        seen_field_names.add(normalized_name)
                        field_names.append(normalized_name)
        
        return field_names
    
    
    def _apply_replacements(self, text: str) -> str:
        """Apply string replacements to text containing {{placeholder}} syntax
        Note: Does NOT replace {{$arguments}} or {{$fields}} - those are special placeholders
        """
        if not isinstance(text, str):
            return text
        
        result = text
        for placeholder, replacement in self.variables.items():
            # Replace {{placeholder}} with the replacement text
            # Skip special placeholders that start with $ (like {{$arguments}}, {{$fields}})
            pattern = f'{{{{{placeholder}}}}}'
            # Only replace if it's not a special placeholder (doesn't start with $)
            if not placeholder.startswith('$'):
                result = result.replace(pattern, replacement)
        
        return result
    
    def _apply_replacements_recursive(self, obj: Any) -> Any:
        """Recursively apply string replacements to all string values in a data structure"""
        if isinstance(obj, dict):
            return {k: self._apply_replacements_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._apply_replacements_recursive(item) for item in obj]
        elif isinstance(obj, str):
            return self._apply_replacements(obj)
        else:
            return obj
    
    def _merge_template_files(self, default_path: Optional[str], user_path: str) -> Dict:
        """Merge default and user template files.
        
        Args:
            default_path: Path to default template file (project root), or None
            user_path: Path to user template file (~/.vast-admin-mcp/)
            
        Returns:
            Merged dictionary containing all sections
        """
        default_data = {}
        user_data = {}
        
        # Load default file if it exists
        if default_path and os.path.exists(default_path):
            try:
                with open(default_path, 'r') as f:
                    default_data = yaml.load(f, Loader=yaml.SafeLoader) or {}
            except (FileNotFoundError, yaml.YAMLError) as e:
                logging.warning(f"Error loading default template from {default_path}: {e}")
                default_data = {}
        
        # Load user file if it exists
        if os.path.exists(user_path):
            try:
                with open(user_path, 'r') as f:
                    user_data = yaml.load(f, Loader=yaml.SafeLoader) or {}
            except (FileNotFoundError, yaml.YAMLError) as e:
                logging.warning(f"Error loading user template from {user_path}: {e}")
                user_data = {}
        
        # If both files are missing, raise error
        if not default_data and not user_data:
            raise ValueError(f"Neither default template ({default_path}) nor user template ({user_path}) found")
        
        # Merge sections
        merged = {}
        
        # api_whitelist: Union merge (combine lists)
        default_whitelist = default_data.get('api_whitelist', [])
        user_whitelist = user_data.get('api_whitelist', [])
        if user_whitelist:
            # User whitelist takes precedence (union, user entries override duplicates)
            merged['api_whitelist'] = user_whitelist + [e for e in default_whitelist if e not in user_whitelist]
        else:
            merged['api_whitelist'] = default_whitelist
        
        # variables: Dictionary merge (user keys override/add to default)
        default_vars = default_data.get('variables', {})
        user_vars = user_data.get('variables', {})
        merged['variables'] = {**default_vars, **user_vars}
        
        # field_anchors: Dictionary merge (user anchors override/add to default)
        default_anchors = default_data.get('field_anchors', {})
        user_anchors = user_data.get('field_anchors', {})
        merged['field_anchors'] = {**default_anchors, **user_anchors}
        
        # list_cmds: Deep merge (user can override specific fields/properties)
        default_cmds = default_data.get('list_cmds', {})
        user_cmds = user_data.get('list_cmds', {})
        merged['list_cmds'] = self._deep_merge_commands(default_cmds, user_cmds)
        
        # merged_list_cmds: Deep merge (same as list_cmds)
        default_merged = default_data.get('merged_list_cmds', [])
        user_merged = user_data.get('merged_list_cmds', [])
        merged['merged_list_cmds'] = self._deep_merge_merged_commands(default_merged, user_merged)
        
        return merged
    
    def _deep_merge_commands(self, default_cmds: Dict, user_cmds: Dict) -> Dict:
        """Deep merge command dictionaries.
        
        For each command:
        - If user defines command: deep merge with default
        - If user doesn't define command: use default
        - Within commands, fields are matched by 'name' and merged
        """
        result = default_cmds.copy()
        
        for cmd_name, user_cmd in user_cmds.items():
            if cmd_name in result:
                # Command exists in both: deep merge
                result[cmd_name] = self._deep_merge_dict(result[cmd_name], user_cmd)
            else:
                # New command from user: add it
                result[cmd_name] = user_cmd
        
        return result
    
    def _deep_merge_merged_commands(self, default_merged: List, user_merged: List) -> List:
        """Deep merge merged_list_cmds lists.
        
        Matches merged commands by 'name' and merges them.
        """
        # Convert to dicts keyed by name for easier merging
        default_dict = {}
        for cmd in default_merged:
            if isinstance(cmd, dict) and 'name' in cmd:
                default_dict[cmd['name']] = cmd
        
        user_dict = {}
        for cmd in user_merged:
            if isinstance(cmd, dict) and 'name' in cmd:
                user_dict[cmd['name']] = cmd
        
        # Merge
        result_dict = default_dict.copy()
        for name, user_cmd in user_dict.items():
            if name in result_dict:
                result_dict[name] = self._deep_merge_dict(result_dict[name], user_cmd)
            else:
                result_dict[name] = user_cmd
        
        # Convert back to list
        return list(result_dict.values())
    
    def _deep_merge_dict(self, default: Dict, user: Dict) -> Dict:
        """Recursively deep merge two dictionaries.
        
        Special handling:
        - If key is 'fields' and both values are lists: merge field lists by matching 'name'
        - Otherwise: recursively merge dicts, user values override defaults
        """
        result = default.copy()
        
        for key, user_value in user.items():
            if key == 'fields' and isinstance(result.get(key), list) and isinstance(user_value, list):
                # Special handling for field lists: match by 'name' and merge
                result[key] = self._deep_merge_field_list(result[key], user_value)
            elif key in result and isinstance(result[key], dict) and isinstance(user_value, dict):
                # Both are dicts: recursively merge
                result[key] = self._deep_merge_dict(result[key], user_value)
            else:
                # User value replaces default
                result[key] = user_value
        
        return result
    
    def _deep_merge_field_list(self, default_fields: List, user_fields: List) -> List:
        """Merge field lists by matching fields by 'name' property.
        
        - Fields with matching 'name' are merged recursively
        - User-only fields are added
        - Default-only fields are kept
        """
        # Create dicts keyed by field name for easier matching
        default_dict = {}
        for field in default_fields:
            if isinstance(field, dict) and 'name' in field:
                default_dict[field['name']] = field
        
        user_dict = {}
        for field in user_fields:
            if isinstance(field, dict) and 'name' in field:
                user_dict[field['name']] = field
        
        # Merge fields
        result_dict = default_dict.copy()
        for name, user_field in user_dict.items():
            if name in result_dict:
                # Field exists in both: deep merge
                result_dict[name] = self._deep_merge_dict(result_dict[name], user_field)
            else:
                # New field from user: add it
                result_dict[name] = user_field
        
        # Convert back to list, preserving order (default fields first, then user-only fields)
        result = []
        seen_names = set()
        
        # Add default fields (merged if user overrode them)
        for field in default_fields:
            if isinstance(field, dict) and 'name' in field:
                name = field['name']
                if name not in seen_names:
                    result.append(result_dict[name])
                    seen_names.add(name)
        
        # Add user-only fields
        for field in user_fields:
            if isinstance(field, dict) and 'name' in field:
                name = field['name']
                if name not in seen_names:
                    result.append(result_dict[name])
                    seen_names.add(name)
        
        return result
    
    def _load_templates(self) -> Dict:
        """Load template YAML file with new structure: variables, field_anchors, list_cmds
        Supports YAML anchors/aliases and applies variable replacements
        Uses merged data from default and user template files
        """
        if not self._merged_data:
            return {}
        
        data = self._merged_data
        
        # Extract variables section (optional)
        if 'variables' in data:
            variables = data['variables']
            if isinstance(variables, dict):
                # Convert all values to strings
                self.variables = {k: str(v) for k, v in variables.items() if isinstance(v, (str, int, float, bool))}
            else:
                logging.warning("'variables' section must be a dictionary, ignoring")
        
        # Extract list_cmds section (optional - may not exist if only api_whitelist is needed)
        if 'list_cmds' not in data:
            return {}  # No list_cmds section - this is OK if only api_whitelist is needed
        
        templates = data['list_cmds']
        if not isinstance(templates, dict):
            logging.warning("'list_cmds' section must be a dictionary, ignoring")
            return {}
        
        # Apply variable replacements to all string values in the templates
        templates = self._apply_replacements_recursive(templates)
        return templates
    
    def _load_api_whitelist(self) -> Dict[str, List[str]]:
        """Load api_whitelist section from merged YAML data
        
        Returns:
            Dictionary mapping endpoint names to allowed HTTP methods.
            - Simple format (e.g., "- views"): defaults to ['get'] only
            - With methods (e.g., "- views: [post]"): includes 'get' + specified methods
            - Empty dict means deny all (restrictive default).
        """
        if not self._merged_data:
            return {}  # Empty = deny all
        
        # Extract api_whitelist section (optional)
        if 'api_whitelist' not in self._merged_data:
            return {}  # Missing = deny all (restrictive)
        
        whitelist_data = self._merged_data['api_whitelist']
        if not isinstance(whitelist_data, list):
            logging.warning("'api_whitelist' section must be a list, defaulting to deny all")
            return {}
        
        if len(whitelist_data) == 0:
            return {}  # Empty list = deny all
        
        # Parse whitelist entries
        whitelist = {}
        for entry in whitelist_data:
            if isinstance(entry, str):
                # Simple format: "- views" (defaults to GET only)
                whitelist[entry] = ['get']  # Default to GET only
            elif isinstance(entry, dict):
                # Complex format: "- views: [post]" or "- views: [get, post]"
                for endpoint, methods in entry.items():
                    if isinstance(methods, list):
                        # Normalize method names to lowercase
                        normalized_methods = [m.lower() if isinstance(m, str) else str(m).lower() for m in methods]
                        # Always include 'get' if not already present
                        if 'get' not in normalized_methods:
                            normalized_methods.insert(0, 'get')
                        whitelist[endpoint] = normalized_methods
                    else:
                        # Single method or invalid format, default to GET only
                        whitelist[endpoint] = ['get']
            else:
                logging.warning(f"Invalid api_whitelist entry format: {entry}, skipping")
        
        return whitelist
    
    def get_api_whitelist(self) -> Dict[str, List[str]]:
        """Get the API whitelist configuration
        
        Returns:
            Dictionary mapping endpoint names to allowed HTTP methods.
            - Simple format (e.g., "- views"): defaults to ['get'] only
            - With methods (e.g., "- views: [post]"): includes 'get' + specified methods
            - Empty dict means deny all (restrictive default).
        """
        return self.api_whitelist
    
    def _load_merged_commands(self) -> Dict:
        """Load merged_list_cmds section from merged YAML data"""
        if not self._merged_data:
            return {}
        
        # Extract merged_list_cmds section (optional)
        if 'merged_list_cmds' not in self._merged_data:
            return {}
        
        merged_cmds = self._merged_data['merged_list_cmds']
        if not isinstance(merged_cmds, list):
            logging.warning("'merged_list_cmds' section must be a list, ignoring")
            return {}
        
        # Convert list to dict keyed by name for easier lookup
        merged_dict = {}
        for cmd in merged_cmds:
            if isinstance(cmd, dict) and 'name' in cmd:
                name = cmd['name']
                # Apply variable replacements
                cmd = self._apply_replacements_recursive(cmd)
                merged_dict[name] = cmd
        
        return merged_dict
    
    def _validate_templates(self):
        """Validate template structure with improved error messages including field paths"""
        required_sections = ['api_endpoints', 'fields', 'description']
        valid_units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'AUTO', 'time_delta']
        valid_order_directions = ['asc', 'dec', 'desc']
        valid_arg_types = ['str', 'int', 'bool', 'list', 'capacity']
        valid_operators = [
            'equals', 'not_equals', 'greater_than', 'less_than', 
            'greater_equal', 'less_equal', 'contains', 'starts_with', 
            'ends_with', 'in', 'regex',
            # Common aliases
            'eq', '==', 'ne', '!=', 'gt', '>', 'lt', '<', 
            'gte', '>=', 'lte', '<='
        ]
        
        # Validate that templates exist (should be loaded from list_cmds)
        # Note: Allow empty templates if only api_whitelist is needed (e.g., for performance commands)
        # Only validate if we actually have template data to validate
        if not self.templates and not self._merged_data.get('api_whitelist'):
            raise ValueError("No commands found in 'list_cmds' section and no 'api_whitelist' section found")
        
        for command_name, template in self.templates.items():
            # Skip keys starting with underscore - these are YAML anchors, not commands
            if command_name.startswith('_'):
                continue
            
            base_path = f"list_cmds.{command_name}"
            
            if not isinstance(template, dict):
                raise ValueError(f"{base_path}: InvalidType - Expected dictionary, got {type(template).__name__}")
            
            # Check required sections
            for section in required_sections:
                if section not in template:
                    raise ValueError(f"{base_path}: MissingRequired - Missing required section '{section}'")
            
            # Validate api_endpoints
            api_endpoints_path = f"{base_path}.api_endpoints"
            if not isinstance(template['api_endpoints'], list):
                raise ValueError(f"{api_endpoints_path}: InvalidType - Expected list, got {type(template['api_endpoints']).__name__}")
            if not template['api_endpoints']:
                raise ValueError(f"{api_endpoints_path}: InvalidValue - Must be a non-empty list")
            
            # Validate fields
            fields_path = f"{base_path}.fields"
            if not isinstance(template['fields'], list):
                raise ValueError(f"{fields_path}: InvalidType - Expected list, got {type(template['fields']).__name__}")
            if not template['fields']:
                raise ValueError(f"{fields_path}: InvalidValue - Must be a non-empty list")
            
            for i, field in enumerate(template['fields']):
                field_path = f"{fields_path}[{i}]"
                if isinstance(field, dict):
                    # Field can have either 'header' or 'name' (for arguments)
                    if 'header' not in field and 'name' not in field:
                        raise ValueError(f"{field_path}: MissingRequired - Field must have 'header' or 'name' key")
                    
                    # Validate convert unit if present
                    if 'convert' in field:
                        convert_path = f"{field_path}.convert"
                        if field['convert'] not in valid_units:
                            field_name = field.get('header') or field.get('name', f'field_{i}')
                            raise ValueError(f"{convert_path}: InvalidValue - Expected one of {valid_units}, got '{field['convert']}'")
                    
                    # Validate argument property if present
                    if 'argument' in field:
                        arg_path = f"{field_path}.argument"
                        arg_config = field['argument']
                        if not isinstance(arg_config, dict):
                            raise ValueError(f"{arg_path}: InvalidType - Expected dictionary, got {type(arg_config).__name__}")
                        
                        # Validate argument type
                        if 'type' in arg_config:
                            type_path = f"{arg_path}.type"
                            if arg_config['type'] not in valid_arg_types:
                                field_name = field.get('name') or field.get('header', f'field_{i}')
                                raise ValueError(f"{type_path}: InvalidValue - Expected one of {valid_arg_types}, got '{arg_config['type']}'")
                        
                        # Validate aliases if present
                        if 'aliases' in arg_config:
                            aliases_path = f"{arg_path}.aliases"
                            if arg_config['aliases'] is None:
                                # Empty aliases: is valid, just remove it
                                del arg_config['aliases']
                            elif not isinstance(arg_config['aliases'], list):
                                field_name = field.get('name') or field.get('header', f'field_{i}')
                                raise ValueError(f"{aliases_path}: InvalidType - Expected list, got {type(arg_config['aliases']).__name__}")
                            elif len(arg_config['aliases']) == 0:
                                # Empty list is valid, but we can remove it for cleanliness
                                del arg_config['aliases']
                            else:
                                # Validate that aliases are strings
                                for alias_idx, alias in enumerate(arg_config['aliases']):
                                    if not isinstance(alias, str):
                                        alias_path = f"{aliases_path}[{alias_idx}]"
                                        raise ValueError(f"{alias_path}: InvalidType - Expected string, got {type(alias).__name__}")
                    
                    # Validate condition property if present
                    if 'condition' in field:
                        condition_path = f"{field_path}.condition"
                        condition = field['condition']
                        if not isinstance(condition, dict):
                            raise ValueError(f"{condition_path}: InvalidType - Expected dictionary, got {type(condition).__name__}")
                        
                        # Validate required keys
                        if 'field' not in condition:
                            raise ValueError(f"{condition_path}: MissingRequired - Missing required key 'field'")
                        if 'operator' not in condition:
                            raise ValueError(f"{condition_path}: MissingRequired - Missing required key 'operator'")
                        if 'value' not in condition:
                            raise ValueError(f"{condition_path}: MissingRequired - Missing required key 'value'")
                        
                        # Validate operator
                        operator_path = f"{condition_path}.operator"
                        operator = condition['operator']
                        if operator not in valid_operators:
                            raise ValueError(f"{operator_path}: InvalidValue - Expected one of {valid_operators}, got '{operator}'")
                        
                        # Validate field is a string
                        field_key_path = f"{condition_path}.field"
                        if not isinstance(condition['field'], str):
                            raise ValueError(f"{field_key_path}: InvalidType - Expected string, got {type(condition['field']).__name__}")
                    
                    # Validate value property if present (f-string expression)
                    if 'value' in field:
                        value_path = f"{field_path}.value"
                        value_expr = field['value']
                        if not isinstance(value_expr, str):
                            raise ValueError(f"{value_path}: InvalidType - Expected string, got {type(value_expr).__name__}")
                        if not value_expr.strip():
                            raise ValueError(f"{value_path}: InvalidValue - Value expression must be a non-empty string")
                        # Check if it looks like an f-string (starts with f" or f')
                        if not (value_expr.strip().startswith('f"') or value_expr.strip().startswith("f'")):
                            logging.warning(f"{value_path}: Value expression should start with f\" or f' for f-string syntax. Example: f\"{{field_name}}\"")
                    
                    # Validate join_on property if present
                    if 'join_on' in field:
                        join_on_path = f"{field_path}.join_on"
                        join_on_config = field['join_on']
                        valid_act_on_values = ['first', 'last', 'all']
                        
                        if isinstance(join_on_config, dict):
                            # Validate act_on if present
                            if 'act_on' in join_on_config:
                                act_on_path = f"{join_on_path}.act_on"
                                act_on_value = join_on_config['act_on']
                                if not isinstance(act_on_value, str):
                                    raise ValueError(f"{act_on_path}: InvalidType - Expected string, got {type(act_on_value).__name__}")
                                act_on_lower = act_on_value.lower()
                                if act_on_lower not in valid_act_on_values:
                                    raise ValueError(f"{act_on_path}: InvalidValue - Expected one of {valid_act_on_values}, got '{act_on_value}'")
                                # Normalize to lowercase
                                join_on_config['act_on'] = act_on_lower
            
            # Validate description
            desc_path = f"{base_path}.description"
            if not isinstance(template['description'], str):
                raise ValueError(f"{desc_path}: InvalidType - Expected string, got {type(template['description']).__name__}")
            if not template['description'].strip():
                raise ValueError(f"{desc_path}: InvalidValue - Description must be a non-empty string")
            
            # Validate per_row_endpoints if present (optional section)
            if 'per_row_endpoints' in template:
                per_row_path = f"{base_path}.per_row_endpoints"
                per_row_endpoints = template['per_row_endpoints']
                if not isinstance(per_row_endpoints, list):
                    raise ValueError(f"{per_row_path}: InvalidType - Expected list, got {type(per_row_endpoints).__name__}")
                
                for i, endpoint_config in enumerate(per_row_endpoints):
                    endpoint_path = f"{per_row_path}[{i}]"
                    if not isinstance(endpoint_config, dict):
                        raise ValueError(f"{endpoint_path}: InvalidType - Expected dictionary, got {type(endpoint_config).__name__}")
                    
                    if 'name' not in endpoint_config:
                        raise ValueError(f"{endpoint_path}: MissingRequired - Missing required key 'name'")
                    
                    name_path = f"{endpoint_path}.name"
                    if not isinstance(endpoint_config['name'], str) or not endpoint_config['name'].strip():
                        raise ValueError(f"{name_path}: InvalidValue - Must be a non-empty string")
                    
                    if 'query' not in endpoint_config:
                        raise ValueError(f"{endpoint_path}: MissingRequired - Missing required key 'query'")
                    
                    query_path = f"{endpoint_path}.query"
                    query = endpoint_config['query']
                    if not isinstance(query, list):
                        raise ValueError(f"{query_path}: InvalidType - Expected list, got {type(query).__name__}")
                    
                    for j, query_item in enumerate(query):
                        query_item_path = f"{query_path}[{j}]"
                        if not isinstance(query_item, str):
                            raise ValueError(f"{query_item_path}: InvalidType - Expected string, got {type(query_item).__name__}")
                        
                        # Validate query format: key=value or key=$field_name
                        if '=' not in query_item:
                            raise ValueError(f"{query_item_path}: InvalidValue - Must be in format 'key=value' or 'key=$field_name'")
        
        # Validate merged_list_cmds section if present
        if self.merged_commands:
            for merged_name, merged_template in self.merged_commands.items():
                base_path = f"merged_list_cmds.{merged_name}"
                
                if not isinstance(merged_template, dict):
                    raise ValueError(f"{base_path}: InvalidType - Expected dictionary, got {type(merged_template).__name__}")
                
                # Validate required keys
                if 'name' not in merged_template:
                    raise ValueError(f"{base_path}: MissingRequired - Missing required key 'name'")
                
                name_path = f"{base_path}.name"
                if not isinstance(merged_template['name'], str) or not merged_template['name'].strip():
                    raise ValueError(f"{name_path}: InvalidValue - Must be a non-empty string")
                
                if 'functions' not in merged_template:
                    raise ValueError(f"{base_path}: MissingRequired - Missing required key 'functions'")
                
                functions_path = f"{base_path}.functions"
                functions = merged_template['functions']
                if not isinstance(functions, list):
                    raise ValueError(f"{functions_path}: InvalidType - Expected list, got {type(functions).__name__}")
                
                if len(functions) < 2:
                    raise ValueError(f"{functions_path}: InvalidValue - Must contain at least 2 functions")
                
                # Validate all function names exist in list_cmds
                for i, func_name in enumerate(functions):
                    func_path = f"{functions_path}[{i}]"
                    if not isinstance(func_name, str):
                        raise ValueError(f"{func_path}: InvalidType - Expected string, got {type(func_name).__name__}")
                    
                    if func_name not in self.templates:
                        raise ValueError(f"{func_path}: InvalidValue - Function '{func_name}' not found in 'list_cmds' section")
                
                # Validate description
                if 'description' not in merged_template:
                    raise ValueError(f"{base_path}: MissingRequired - Missing required key 'description'")
                
                desc_path = f"{base_path}.description"
                if not isinstance(merged_template['description'], str):
                    raise ValueError(f"{desc_path}: InvalidType - Expected string, got {type(merged_template['description']).__name__}")
                
                if not merged_template['description'].strip():
                    raise ValueError(f"{desc_path}: InvalidValue - Description must be a non-empty string")
                
                # Validate description contains required placeholders
                description = merged_template['description']
                if '{{$arguments}}' not in description:
                    raise ValueError(f"{desc_path}: InvalidValue - Description must contain '{{$arguments}}' placeholder")
                
                if '{{$fields}}' not in description:
                    raise ValueError(f"{desc_path}: InvalidValue - Description must contain '{{$fields}}' placeholder")
    
    def get_template(self, command_name: str) -> Optional[Dict]:
        """Get template for a specific command"""
        return self.templates.get(command_name)
    
    def get_all_commands(self) -> List[str]:
        """Get list of all commands defined in templates"""
        return list(self.templates.keys())
    
    def get_api_endpoints(self, command_name: str) -> List[str]:
        """Get list of API endpoints for a command"""
        template = self.get_template(command_name)
        if not template:
            return []
        return template.get('api_endpoints', [])
    
    def get_per_row_endpoints(self, command_name: str) -> List[Dict]:
        """Get list of per-row endpoint configurations for a command
        
        Returns:
            List of endpoint configurations, each with 'name' and 'query' keys.
            'query' is a list of strings in format 'key=value' or 'key=$field_name'.
        """
        template = self.get_template(command_name)
        if not template:
            return []
        return template.get('per_row_endpoints', [])
    
    def get_fields(self, command_name: str) -> List[Dict]:
        """Get output fields configuration"""
        template = self.get_template(command_name)
        if not template:
            return []
        
        fields = template.get('fields', [])
        normalized_fields = []
        
        for field in fields:
            if isinstance(field, dict):
                # New structure: header-based (output fields)
                if 'header' in field:
                    field_config = field.copy()
                    field_config['name'] = field_config.pop('header')
                    # Set default source field if not specified
                    if 'field' not in field_config:
                        field_config['field'] = field_config['name']
                    normalized_fields.append(field_config)
                # New structure: name-based (argument fields that are also output)
                elif 'name' in field:
                    field_config = field.copy()
                    # Set default source field if not specified
                    if 'field' not in field_config:
                        # For argument fields, if no 'field' property, use the name
                        # But check if it's a special field like cluster that uses $(cluster)
                        if field_config['name'] in ['cluster', 'clusters']:
                            field_config['field'] = f"$({field_config['name']})"
                        else:
                            field_config['field'] = field_config['name']
                    normalized_fields.append(field_config)
                else:
                    # Old structure: first key is field name
                    field_name = list(field.keys())[0]
                    field_config = field[field_name]
                    
                    if isinstance(field_config, dict):
                        field_config['name'] = field_name
                        if 'field' not in field_config:
                            field_config['field'] = field_name
                        normalized_fields.append(field_config)
                    else:
                        normalized_fields.append({
                            'name': field_name,
                            'field': field_config,
                            'type': 'string'
                        })
            elif isinstance(field, str):
                # Simple field name
                normalized_fields.append({
                    'name': field,
                    'field': field,
                    'type': 'string'
                })
        
        return normalized_fields
    
    def get_arguments(self, command_name: str) -> List[Dict]:
        """Get CLI arguments configuration from fields with argument property"""
        template = self.get_template(command_name)
        if not template:
            return []
        
        fields = template.get('fields', [])
        normalized_args = []
        
        for field in fields:
            if isinstance(field, dict) and 'argument' in field:
                # Extract argument configuration from field
                arg_config = field['argument'].copy()
                
                # Use field's 'name' as the argument name (or 'header' if 'name' not present)
                field_name = field.get('name')
                if not field_name:
                    # If field has 'header', use that as name
                    field_name = field.get('header')
                
                if not field_name:
                    continue  # Skip if no name/header
                
                arg_config['name'] = field_name
                
                # Copy other field properties that might be relevant
                # The 'field' property maps to API parameter name
                if 'field' in field:
                    # Store the API field name for later use in get_api_mapping
                    arg_config['_api_field'] = field['field']
                
                # Auto-generate description if not provided
                if 'description' not in arg_config or not arg_config.get('description', '').strip():
                    arg_config['description'] = self._generate_argument_description(
                        command_name, field_name, arg_config, field
                    )
                else:
                    # Description was provided, but we should still add aliases if present
                    # (aliases are added in _generate_argument_description, so we need to add them here too)
                    aliases = arg_config.get('aliases', [])
                    if aliases:
                        aliases_str = ", ".join(aliases)
                        existing_desc = arg_config.get('description', '')
                        if 'Aliases:' not in existing_desc:
                            arg_config['description'] = f"{existing_desc}. Aliases: {aliases_str}"
                
                normalized_args.append(arg_config)
        
        return normalized_args
    
    def _generate_argument_description(self, command_name: str, field_name: str, arg_config: Dict, field_config: Dict) -> str:
        """Auto-generate argument description based on field information and command context"""
        # Humanize field name (replace underscores with spaces, capitalize)
        human_name = field_name.replace('_', ' ').replace('-', ' ')
        # Capitalize first letter of each word
        human_name = ' '.join(word.capitalize() for word in human_name.split())
        
        # Get argument properties
        arg_type = arg_config.get('type', 'str')
        is_mandatory = arg_config.get('mandatory', False)
        is_filter = arg_config.get('filter', False)
        is_list = arg_config.get('argument_list', False)
        aliases = arg_config.get('aliases', [])
        
        # Special case: capacity fields are typed as 'int' but should support capacity parsing
        # Detect capacity fields by checking field configuration for 'convert' property
        # or by field name patterns (capacity, size, limit, used)
        if arg_type == 'int' and is_filter:
            # Check if field has 'convert' property (indicates capacity field)
            if 'convert' in field_config:
                arg_type = 'capacity'
            # Or check field name for capacity-related keywords
            elif any(keyword in field_name.lower() for keyword in ['capacity', 'size', 'limit', 'used', 'quota']):
                arg_type = 'capacity'
        
        # Build description based on type and properties
        parts = []
        
        # Special handling for cluster/clusters
        if field_name in ['cluster', 'clusters']:
            if is_list:
                parts.append("Comma-separated list of cluster names")
            else:
                parts.append("Cluster name or address")
            if not is_mandatory:
                parts.append("Queries all clusters from configuration if not specified")
            return ". ".join(parts) + "."
        
        # Special handling for list type fields with filter:true - add "in" operator documentation
        if arg_type == 'list' and is_filter:
            parts.append("Filter by checking if value exists in comma-separated list")
            parts.append("Supports: exact match (e.g., 'user1'), 'in:value' syntax (e.g., 'in:user1'), wildcards (e.g., '*admin*'), or substring match (e.g., 'admin' matches 'admin1', 'admin2', etc.)")
            parts.append("All matching is case-insensitive")
            return ". ".join(parts) + "."
        
        # For filter arguments, use more specific language with filter syntax examples
        if is_filter:
            # Use command name to make it more contextual (e.g., "Filter views by tenant")
            command_plural = command_name if command_name.endswith('s') else f"{command_name}s"
            parts.append(f"Filter {command_plural} by {human_name.lower()}")
            
            # Add filter syntax examples based on type
            if arg_type == 'str':
                parts.append("Supports: * (non-empty), *value* (contains), !*value* (not contains), value* (starts with), *value (ends with), value (equals)")
            elif arg_type == 'int':
                parts.append("Supports: >value (greater than), >=value (greater or equal), <value (less than), <=value (less or equal), value (equals)")
            elif arg_type == 'capacity':
                parts.append("Supports: >1TB, >=500GB, <1M, <=100KB, 1TB (equals). Units: B, KB, MB, GB, TB, PB")
            elif arg_type == 'bool':
                parts.append("Supports: true/false, True/False, TRUE/FALSE, 1/0")
        elif is_list and arg_type == 'list':
            parts.append(f"Comma-separated list of {human_name.lower()}")
        else:
            parts.append(f"{human_name}")
        
        # Add default behavior
        if not is_mandatory:
            if is_filter:
                # Make it more specific based on field name
                if field_name in ['tenant', 'path', 'bucket', 'share']:
                    parts.append(f"Returns all {field_name}s if not specified")
                else:
                    parts.append("Returns all if not specified")
            elif is_list:
                parts.append("Uses default from configuration if not specified")
            else:
                parts.append("Optional")
        
        return ". ".join(parts) + "."
    
    def get_ordering(self, command_name: str) -> List[Dict]:
        """Get ordering configuration"""
        template = self.get_template(command_name)
        if not template:
            return []
        
        ordering = template.get('ordering', [])
        normalized_ordering = []
        
        # New structure: dict with direct key-value pairs
        if isinstance(ordering, dict):
            for field_name, direction in ordering.items():
                normalized_ordering.append({
                    'field': field_name,
                    'direction': direction
                })
        # Old structure: list of dicts
        elif isinstance(ordering, list):
            for order in ordering:
                if isinstance(order, dict):
                    field_name = list(order.keys())[0]
                    direction = order[field_name]
                    normalized_ordering.append({
                        'field': field_name,
                        'direction': direction
                    })
        
        return normalized_ordering
    
    def get_description(self, command_name: str) -> str:
        """Get command description with {{$arguments}} and {{$fields}} replaced by formatted lists
        Note: {{$arguments}} and {{$fields}} are special placeholders that are replaced AFTER string replacements
        Supports both regular commands and merged commands.
        """
        # Check if it's a merged command first
        merged_template = self.get_merged_command_template(command_name)
        if merged_template:
            description = merged_template.get('description', '').strip()
        else:
            # Regular command
            template = self.get_template(command_name)
            if not template:
                return ""
            description = template.get('description', '').strip()
        
        # Replace {{$arguments}} placeholder if present (note: $ prefix to distinguish from regular replacements)
        if '{{$arguments}}' in description:
            # Detect indentation level from the line containing {{$arguments}}
            lines = description.split('\n')
            base_indent = 0
            for line in lines:
                if '{{$arguments}}' in line:
                    # Count leading spaces before {{$arguments}}
                    # The placeholder itself will be replaced, so we use the full line indent
                    base_indent = len(line) - len(line.lstrip(' '))
                    break
            
            # Get formatted arguments - use the detected base indent level
            # For merged commands, use get_merged_arguments, otherwise use get_arguments
            if merged_template:
                formatted_args = self._format_merged_arguments_for_mcp(command_name, base_indent)
            else:
                formatted_args = self._format_arguments_for_mcp(command_name, base_indent)
            # Replace placeholder (including any whitespace around it on the same line)
            # Replace the entire line to preserve formatting
            for i, line in enumerate(lines):
                if '{{$arguments}}' in line:
                    # Replace the line with the formatted arguments
                    lines[i] = formatted_args
                    break
            description = '\n'.join(lines)
        
        # Replace {{$fields}} placeholder if present (replace all occurrences)
        # Note: $ prefix to distinguish from regular string replacements
        while '{{$fields}}' in description:
            # Detect indentation level from the line containing {{$fields}}
            lines = description.split('\n')
            base_indent = 0
            placeholder_line_idx = -1
            for i, line in enumerate(lines):
                if '{{$fields}}' in line:
                    # Count leading spaces before {{$fields}}
                    base_indent = len(line) - len(line.lstrip(' '))
                    placeholder_line_idx = i
                    break
            
            if placeholder_line_idx >= 0:
                # Get formatted fields - use the detected base indent level
                # For merged commands, use get_merged_fields, otherwise use get_fields
                if merged_template:
                    formatted_fields = self._format_merged_fields_for_mcp(command_name, base_indent)
                else:
                    formatted_fields = self._format_fields_for_mcp(command_name, base_indent)
                # Replace the placeholder line with the formatted fields
                # If the line only contains {{$fields}} (with optional whitespace), replace the whole line
                placeholder_line = lines[placeholder_line_idx]
                if placeholder_line.strip() == '{{$fields}}':
                    # Line only contains placeholder, replace entire line
                    lines[placeholder_line_idx] = formatted_fields
                else:
                    # Placeholder is part of a larger line, replace just the placeholder
                    lines[placeholder_line_idx] = placeholder_line.replace('{{$fields}}', formatted_fields)
                description = '\n'.join(lines)
            else:
                break  # Safety break if we can't find the placeholder
        
        return description
    
    def _format_arguments_for_mcp(self, command_name: str, indent_level: int = 2) -> str:
        """Format arguments list for MCP consumption
        
        Args:
            command_name: Name of the command
            indent_level: Number of spaces to indent each argument line (default: 2)
        
        Returns:
            Formatted arguments with consistent indentation
        """
        args_config = self.get_arguments(command_name)
        
        if not args_config:
            return " " * indent_level + "No arguments available."
        
        # Format each argument for MCP with consistent indentation
        indent = " " * indent_level
        formatted_args = []
        for arg in args_config:
            arg_name = arg.get('name', '')
            arg_type = arg.get('type', 'str')
            arg_desc = arg.get('description', '')
            arg_mandatory = arg.get('mandatory', False)
            aliases = arg.get('aliases', [])
            regex_validation = arg.get('regex_validation')
            
            # Build argument description with specified indentation
            mandatory_str = " (required)" if arg_mandatory else " (optional)"
            arg_line = f"{indent}{arg_name} ({arg_type}){mandatory_str}: {arg_desc}"
            
            # Add regex validation information if present
            if regex_validation:
                arg_line += f" [Regex validation: {regex_validation}]"
            
            # Add aliases information if present
            if aliases:
                aliases_str = ", ".join(aliases)
                arg_line += f" [Aliases: {aliases_str}]"
            
            formatted_args.append(arg_line)
        
        return "\n".join(formatted_args)
    
    def _format_merged_arguments_for_mcp(self, merged_name: str, indent_level: int = 2) -> str:
        """Format merged arguments list for MCP consumption
        
        Args:
            merged_name: Name of the merged command
            indent_level: Number of spaces to indent each argument line (default: 2)
        
        Returns:
            Formatted arguments with consistent indentation
        """
        args_config = self.get_merged_arguments(merged_name)
        
        if not args_config:
            return " " * indent_level + "No arguments available."
        
        # Format each argument for MCP with consistent indentation
        indent = " " * indent_level
        formatted_args = []
        for arg in args_config:
            arg_name = arg.get('name', '')
            arg_type = arg.get('type', 'str')
            arg_desc = arg.get('description', '')
            arg_mandatory = arg.get('mandatory', False)
            aliases = arg.get('aliases', [])
            regex_validation = arg.get('regex_validation')
            
            # Build argument description with specified indentation
            mandatory_str = " (required)" if arg_mandatory else " (optional)"
            arg_line = f"{indent}{arg_name} ({arg_type}){mandatory_str}: {arg_desc}"
            
            # Add regex validation information if present
            if regex_validation:
                arg_line += f" [Regex validation: {regex_validation}]"
            
            # Add aliases information if present
            if aliases:
                aliases_str = ", ".join(aliases)
                arg_line += f" [Aliases: {aliases_str}]"
            
            formatted_args.append(arg_line)
        
        return "\n".join(formatted_args)
    
    def _format_fields_for_mcp(self, command_name: str, indent_level: int = 2) -> str:
        """Format fields list for MCP consumption to describe returned object structure
        
        Args:
            command_name: Name of the command
            indent_level: Number of spaces to indent each field line (default: 2)
        
        Returns:
            Formatted fields with consistent indentation
        """
        fields_config = self.get_fields(command_name)
        
        if not fields_config:
            return " " * indent_level + "No fields available."
        
        # Format each field for MCP with consistent indentation
        indent = " " * indent_level
        formatted_fields = []
        for field in fields_config:
            # Skip hidden fields
            if field.get('hide', False):
                continue
            
            field_name = field.get('name', '')
            field_type = self._infer_field_type(field)
            field_desc = self._generate_field_description(field)
            
            # Build field description with specified indentation
            field_line = f"{indent}{field_name} ({field_type}): {field_desc}"
            formatted_fields.append(field_line)
        
        return "\n".join(formatted_fields)
    
    def _format_merged_fields_for_mcp(self, merged_name: str, indent_level: int = 2) -> str:
        """Format merged fields list for MCP consumption to describe returned object structure
        
        Args:
            merged_name: Name of the merged command
            indent_level: Number of spaces to indent each field line (default: 2)
        
        Returns:
            Formatted fields with consistent indentation
        """
        # Get merged field names
        merged_field_names = self.get_merged_fields(merged_name)
        
        if not merged_field_names:
            return " " * indent_level + "No fields available."
        
        # Get field configs from all source functions to build descriptions
        merged_template = self.get_merged_command_template(merged_name)
        source_functions = merged_template.get('functions', [])
        
        # Build a map of field_name -> field_config from all source functions
        field_config_map = {}
        for func_name in source_functions:
            func_fields = self.get_fields(func_name)
            for field in func_fields:
                field_name = field.get('name') or field.get('header')
                if field_name:
                    normalized_name = field_name.replace(' ', '_')
                    if normalized_name not in field_config_map:
                        field_config_map[normalized_name] = field
        
        # Format each field for MCP with consistent indentation
        indent = " " * indent_level
        formatted_fields = []
        for field_name in merged_field_names:
            field_config = field_config_map.get(field_name, {})
            
            # Skip hidden fields
            if field_config.get('hide', False):
                continue
            
            field_type = self._infer_field_type(field_config) if field_config else 'string'
            field_desc = self._generate_field_description(field_config) if field_config else "Field from merged command"
            
            # Build field description with specified indentation
            field_line = f"{indent}{field_name} ({field_type}): {field_desc}"
            formatted_fields.append(field_line)
        
        return "\n".join(formatted_fields)
    
    def _infer_field_type(self, field_config: Dict) -> str:
        """Infer the type of a field based on its configuration"""
        # Check for convert property (indicates capacity/size field)
        if 'convert' in field_config:
            return 'capacity'
        
        # Check for jq property (indicates transformation, likely string or list)
        if 'jq' in field_config:
            jq_expr = field_config['jq']
            # If jq expression contains 'join', it's likely converting a list to string
            if 'join' in jq_expr:
                return 'string (from list)'
            return 'transformed'
        
        # Check field name for common patterns
        field_name = field_config.get('name', '').lower()
        if any(keyword in field_name for keyword in ['capacity', 'size', 'limit', 'used', 'quota']):
            return 'capacity'
        if any(keyword in field_name for keyword in ['time', 'date', 'created', 'updated']):
            return 'datetime'
        if 'protocol' in field_name:
            return 'string (protocol list)'
        
        # Default to string
        return 'string'
    
    def _generate_field_description(self, field_config: Dict) -> str:
        """Generate a description for a field based on its configuration"""
        field_name = field_config.get('name', '')
        parts = []
        
        # Add information about transformations
        if 'convert' in field_config:
            convert_unit = field_config['convert']
            if convert_unit == 'AUTO':
                parts.append("human-readable capacity (auto-selected unit)")
            elif convert_unit == 'time_delta':
                parts.append("relative time delta (e.g., '3d 1h 45m 38s ago' or 'in 2h 30m 15s')")
            else:
                parts.append(f"capacity in {convert_unit}")
        
        if 'jq' in field_config:
            jq_expr = field_config['jq']
            if 'join' in jq_expr:
                # Extract separator if possible
                import re
                match = re.search(r'join\s*\([\\]?["\']([^"\'\\]+)[\\]?["\']\s*\)', jq_expr)
                if match:
                    separator = match.group(1)
                    parts.append(f"comma-separated list joined with '{separator}'")
                else:
                    parts.append("comma-separated list")
        
        # Add information about joined fields
        if 'join_on' in field_config:
            parts.append("from joined data")
        
        # Add information about CLI parameter fields
        source_field = field_config.get('field', '')
        if source_field.startswith('$('):
            parts.append("cluster name")
        
        # Generate more descriptive text based on field name patterns
        field_name_lower = field_name.lower()
        if not parts:
            # Provide more context based on field name (check more specific patterns first)
            if 'cluster' in field_name_lower:
                parts.append("name of the VAST cluster")
            elif 'tenant' in field_name_lower:
                parts.append("tenant name")
            elif 'policy' in field_name_lower:
                if 'qos' in field_name_lower:
                    parts.append("QoS policy name")
                else:
                    parts.append("view policy name")
            elif 'bucket' in field_name_lower:
                parts.append("S3 bucket name")
            elif 'share' in field_name_lower:
                parts.append("SMB share name")
            elif 'protocol' in field_name_lower:
                parts.append("supported protocols (NFS, S3, SMB, etc.)")
            elif 'logical' in field_name_lower and 'used' in field_name_lower:
                parts.append("logical capacity used by the view")
            elif 'physical' in field_name_lower and 'used' in field_name_lower:
                parts.append("physical capacity used by the view")
            elif 'quota' in field_name_lower:
                parts.append("hard quota limit for the view")
            elif 'qos' in field_name_lower:
                parts.append("QoS policy name")
            elif 'path' in field_name_lower:
                parts.append("view path")
            elif 'view' in field_name_lower:
                parts.append("view information")
            elif 'name' in field_name_lower and len(field_name_lower.split()) == 1:
                parts.append("view name")
            else:
                # Humanize the field name as fallback
                human_name = field_name.replace('_', ' ').replace('-', ' ')
                parts.append(f"{human_name} value")
        
        return ". ".join(parts) if parts else "field value"
    
    def validate_argument_value(self, command_name: str, arg_name: str, value: str) -> tuple[bool, Optional[str]]:
        """Validate argument value against regex in template
        
        Returns:
            tuple: (is_valid, error_message)
        """
        args = self.get_arguments(command_name)
        
        for arg in args:
            if arg.get('name') == arg_name:
                regex_pattern = arg.get('regex_validation')
                if regex_pattern:
                    try:
                        pattern = re.compile(regex_pattern)
                        if not pattern.match(value):
                            # Simple, concise error message
                            error_msg = f"Invalid format for '{arg_name}'"
                            return False, error_msg
                    except re.error as e:
                        return False, f"Invalid regex pattern in template for '{arg_name}': {e}"
                return True, None
        
        return True, None
    
    def get_api_mapping(self, command_name: str, arg_name: str) -> Optional[str]:
        """Get API parameter name for CLI argument
        
        Uses the field's 'field' property as the API parameter name.
        If field has no 'field' property, uses the field name.
        Special fields like 'cluster' that don't map to API parameters return None.
        
        Handles both space and underscore versions of field names (e.g., "logical used" and "logical_used").
        """
        template = self.get_template(command_name)
        if not template:
            return arg_name
        
        fields = template.get('fields', [])
        
        # Normalize arg_name: convert underscores to spaces for matching
        # This handles CLI arguments like "logical_used" matching YAML field "logical used"
        arg_name_normalized = arg_name.replace('_', ' ')
        
        for field in fields:
            if isinstance(field, dict) and 'argument' in field:
                # Get field name (either 'name' or 'header')
                field_name = field.get('name') or field.get('header')
                
                # Match both exact and normalized versions
                if field_name == arg_name or field_name == arg_name_normalized:
                    # Check if this is a special non-API argument (like cluster)
                    # If field has no 'field' property and is cluster/clusters, it's not an API parameter
                    if 'field' not in field:
                        if field_name in ['cluster', 'clusters']:
                            return None  # Not an API parameter
                        # Otherwise, use the field name as API parameter
                        return field_name
                    
                    # Use the 'field' property as the API parameter name
                    return field['field']
        
        # Fallback: return the argument name
        return arg_name

