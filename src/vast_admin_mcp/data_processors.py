"""Data processing classes extracted from CommandExecutor for better separation of concerns."""

import logging
import subprocess
import fnmatch
from typing import Dict, List, Any, Optional, Tuple

from .template_parser import TemplateParser
from .config import JQ_TIMEOUT_SECONDS
from .utils import (
    parse_filter_value, parse_capacity_value, normalize_field_name, to_python_name
)


class DataTransformer:
    """Handles field transformations (jq, unit conversion, expressions)."""
    
    def __init__(self, template_parser: TemplateParser):
        self.template_parser = template_parser
    
    def transform_fields(self, command_name: str, data: List[Dict], cli_args: Dict) -> List[Dict]:
        """Transform fields in data according to template configuration.
        
        This is a simplified interface that delegates to the full transformation logic.
        The actual implementation remains in CommandExecutor for now to maintain compatibility.
        """
        # This will be implemented by moving _transform_fields from CommandExecutor
        # For now, this is a placeholder to establish the interface
        raise NotImplementedError("This will be implemented by moving _transform_fields from CommandExecutor")
    
    def apply_jq(self, value: Any, jq_expr: str) -> Any:
        """Apply jq expression to a value."""
        import shutil
        import json
        
        if not shutil.which("jq"):
            logging.warning("jq not available, skipping jq transformation")
            return value
        
        try:
            # Convert value to JSON string
            if isinstance(value, (dict, list)):
                json_str = json.dumps(value)
            else:
                json_str = json.dumps(str(value))
            
            # Run jq command
            result = subprocess.run(
                ['jq', jq_expr],
                input=json_str,
                capture_output=True,
                text=True,
                timeout=JQ_TIMEOUT_SECONDS
            )
            
            if result.returncode == 0:
                # Parse result
                try:
                    return json.loads(result.stdout.strip())
                except json.JSONDecodeError:
                    # Return as string if not valid JSON
                    return result.stdout.strip()
            else:
                logging.warning(f"jq command failed: {result.stderr}")
                return value
        except subprocess.TimeoutExpired:
            logging.warning(f"jq command timed out after {JQ_TIMEOUT_SECONDS}s")
            return value
        except Exception as e:
            logging.warning(f"Error applying jq expression: {e}")
            return value


class DataFilter:
    """Handles data filtering logic."""
    
    def __init__(self, template_parser: TemplateParser):
        self.template_parser = template_parser
    
    def apply_client_filters(self, command_name: str, api_data: Dict[str, List[Dict]], client_filters: Dict) -> Dict[str, List[Dict]]:
        """Apply client-side filters to API data.
        
        This is a simplified interface that delegates to the full filtering logic.
        The actual implementation remains in CommandExecutor for now to maintain compatibility.
        """
        # This will be implemented by moving _apply_client_filters from CommandExecutor
        raise NotImplementedError("This will be implemented by moving _apply_client_filters from CommandExecutor")
    
    def apply_client_filters_on_transformed(self, command_name: str, data: List[Dict], client_filters: Dict, cli_args: Dict) -> List[Dict]:
        """Apply client-side filters on transformed data (for computed fields).
        
        This is a simplified interface that delegates to the full filtering logic.
        The actual implementation remains in CommandExecutor for now to maintain compatibility.
        """
        # This will be implemented by moving _apply_client_filters_on_transformed from CommandExecutor
        raise NotImplementedError("This will be implemented by moving _apply_client_filters_on_transformed from CommandExecutor")
    
    def resolve_field_name(self, command_name: str, arg_name: str, sample_row: Dict, available_fields: List[str]) -> Optional[Tuple[str, bool]]:
        """Resolve the actual field name in transformed data for a given argument name.
        
        This method was already extracted from CommandExecutor in a previous refactoring.
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
    
    def match_wildcard(self, value: Any, pattern: str, is_list_field: bool = False) -> bool:
        """Match a value against a wildcard pattern."""
        if value is None:
            return False
        
        # Convert value to string for matching
        if isinstance(value, list):
            if is_list_field:
                # For list fields, check if any item matches
                value_str = ','.join(str(v) for v in value)
            else:
                # For non-list fields that happen to be lists, join them
                value_str = ','.join(str(v) for v in value)
        else:
            value_str = str(value)
        
        # Case-insensitive matching
        value_str_lower = value_str.lower()
        pattern_lower = pattern.lower()
        
        # Check for special filter syntax
        if pattern.startswith('in:'):
            # 'in:value' syntax - check if value contains the substring
            search_value = pattern[3:].lower()
            return search_value in value_str_lower
        elif pattern.startswith('!*') and pattern.endswith('*'):
            # '!*value*' syntax - value should NOT contain the substring
            search_value = pattern[2:-1].lower()
            return search_value not in value_str_lower
        elif pattern.startswith('*') and pattern.endswith('*'):
            # '*value*' syntax - value should contain the substring
            search_value = pattern[1:-1].lower()
            return search_value in value_str_lower
        elif pattern.startswith('*'):
            # '*value' syntax - value should end with the substring
            search_value = pattern[1:].lower()
            return value_str_lower.endswith(search_value)
        elif pattern.endswith('*'):
            # 'value*' syntax - value should start with the substring
            search_value = pattern[:-1].lower()
            return value_str_lower.startswith(search_value)
        else:
            # Exact match (case-insensitive)
            return value_str_lower == pattern_lower


class DataJoiner:
    """Handles data joining operations."""
    
    def __init__(self, template_parser: TemplateParser):
        self.template_parser = template_parser
    
    def join_data(self, command_name: str, api_data: Dict[str, List[Dict]]) -> List[Dict]:
        """Join data from multiple API endpoints.
        
        This is a simplified interface that delegates to the full joining logic.
        The actual implementation remains in CommandExecutor for now to maintain compatibility.
        """
        # This will be implemented by moving _join_data from CommandExecutor
        raise NotImplementedError("This will be implemented by moving _join_data from CommandExecutor")

