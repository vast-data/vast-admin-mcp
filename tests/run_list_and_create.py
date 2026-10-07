#!/usr/bin/env python3
"""
Test script for list commands and create view from template.

This script tests:
1. Various list commands (views, tenants, viewpolicies, etc.)
2. Create view from template named "vmware"
3. Views list command:
   - Once without arguments
   - Once with all available arguments with random values
"""

import sys
import os
import random
import subprocess
import json
import yaml
from pathlib import Path

# Add parent directory to path to import vast_admin_mcp
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp.functions import list_dynamic
from vast_admin_mcp.create_functions import create_view_from_template
from vast_admin_mcp.config import load_config, VIEW_TEMPLATE_FILE, get_default_template_path, TEMPLATE_MODIFICATIONS_FILE
from vast_admin_mcp.template_parser import TemplateParser
from vast_admin_mcp.utils import to_python_name


def run_list_command(command_name: str, **kwargs):
    """Run a list command and return results."""
    print(f"\n{'='*60}")
    print(f"Testing list command: {command_name}")
    print(f"{'='*60}")
    print(f"Arguments: {kwargs if kwargs else 'None'}")
    
    try:
        results = list_dynamic(command_name, **kwargs)
        print(f"✓ Success: Retrieved {len(results)} results")
        if results and len(results) > 0:
            print(f"  First result keys: {list(results[0].keys())}")
        return results
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def get_commands_from_template():
    """Load commands from YAML template file."""
    # Try to find template file (same logic as config.py)
    template_file = TEMPLATE_MODIFICATIONS_FILE
    default_template_path = get_default_template_path()
    
    # Use default template if modifications file doesn't exist
    if not os.path.exists(template_file) and default_template_path:
        template_file = default_template_path
    
    if not os.path.exists(template_file):
        print(f"✗ Template file not found: {template_file}")
        if default_template_path:
            print(f"  Also checked: {default_template_path}")
        return []
    
    try:
        with open(template_file, 'r') as f:
            template_data = yaml.safe_load(f) or {}
        
        # Extract commands from list_cmds section
        list_cmds = template_data.get('list_cmds', {})
        commands = list(list_cmds.keys())
        
        # Filter out commands that have create_mcp_tool: false (like cnodes, dnodes)
        # These are typically merged commands only
        filtered_commands = []
        for cmd in commands:
            cmd_config = list_cmds.get(cmd, {})
            # Only include if create_mcp_tool is not False (default is True)
            if cmd_config.get('create_mcp_tool', True) is not False:
                filtered_commands.append(cmd)
        
        print(f"  Loaded {len(filtered_commands)} commands from {template_file}")
        return filtered_commands
    except Exception as e:
        print(f"✗ Error loading template file: {e}")
        import traceback
        traceback.print_exc()
        return []


def test_list_commands():
    """Test various list commands loaded from YAML template."""
    print("\n" + "="*60)
    print("TESTING LIST COMMANDS")
    print("="*60)
    
    # Load commands from YAML template file
    commands_to_test = get_commands_from_template()
    
    if not commands_to_test:
        print("✗ No commands found to test")
        return {}
    
    print(f"  Testing {len(commands_to_test)} commands: {', '.join(commands_to_test)}")
    
    # Load template parser and config for generating random args
    template_file = TEMPLATE_MODIFICATIONS_FILE
    default_template_path = get_default_template_path()
    if not os.path.exists(template_file) and default_template_path:
        template_file = default_template_path
    
    try:
        template_parser = TemplateParser(template_file, default_template_path=default_template_path)
    except Exception as e:
        print(f"✗ Error loading template parser: {e}")
        template_parser = None
    
    config = load_config()
    
    results = {}
    for cmd in commands_to_test:
        # Run without arguments
        print(f"\n--- Testing {cmd} without arguments ---")
        results[cmd] = {'without_args': run_list_command(cmd)}
        
        # Run with random arguments
        if template_parser:
            print(f"\n--- Testing {cmd} with random arguments ---")
            try:
                random_args = generate_random_args_for_command(cmd, template_parser, config)
                print(f"  Generated random arguments: {random_args}")
                results[cmd]['with_random_args'] = run_list_command(cmd, **random_args)
            except Exception as e:
                print(f"✗ Error generating random args for {cmd}: {e}")
                results[cmd]['with_random_args'] = None
        else:
            results[cmd]['with_random_args'] = None
    
    return results


def test_create_view_from_template():
    """Test creating a view from template named 'vmware'."""
    print("\n" + "="*60)
    print("TESTING CREATE VIEW FROM TEMPLATE: vmware")
    print("="*60)
    
    # Check if template file exists, try default location first, then example file
    template_file = VIEW_TEMPLATE_FILE
    if not os.path.exists(template_file):
        # Try example file in project root
        example_file = Path(__file__).parent.parent / 'view_templates_example.json'
        if os.path.exists(example_file):
            template_file = str(example_file)
            print(f"  Using example template file: {template_file}")
        else:
            print(f"✗ Template file not found: {VIEW_TEMPLATE_FILE}")
            print(f"  Also checked: {example_file}")
            print(f"  Please create {VIEW_TEMPLATE_FILE} or copy view_templates_example.json")
            return None
    else:
        print(f"  Using template file: {template_file}")
    
    # Load template file
    try:
        with open(template_file, 'r') as f:
            templates = json.load(f)
    except Exception as e:
        print(f"✗ Error loading template file: {e}")
        return None
    
    # Find vmware template
    vmware_template = None
    for template in templates:
        if template.get('name') == 'vmware':
            vmware_template = template
            break
    
    if not vmware_template:
        print(f"✗ Template 'vmware' not found in {template_file}")
        print(f"  Available templates: {[t.get('name') for t in templates]}")
        return None
    
    print(f"✓ Found template: {vmware_template}")
    
    # Get cluster from config
    config = load_config()
    if not config.get('clusters'):
        print("✗ No clusters configured")
        return None
    
    # Use the cluster from template or first available
    cluster = vmware_template.get('cluster')
    if not cluster:
        cluster = config['clusters'][0]['cluster']
    
    print(f"  Using cluster: {cluster}")
    print(f"  Using tenant: {vmware_template.get('tenant', 'default')}")
    
    try:
        # Create view from template (use the template file we found)
        result = create_view_from_template(template='vmware', count=1, view_template_file=template_file)
        print(f"✓ Success: Created view from template")
        print(f"  Result: {result}")
        return result
    except Exception as e:
        print(f"✗ Error creating view: {e}")
        import traceback
        traceback.print_exc()
        return None


def generate_random_filter_value(arg_config: dict) -> str:
    """Generate random filter values for testing based on argument configuration."""
    arg_type = arg_config.get('type', 'str')
    arg_name = arg_config.get('name', '')
    
    if arg_type == 'int':
        # Generate size filters like >20G, <4g, >=100MB, <=1TB
        operators = ['>', '>=', '<', '<=']
        sizes = ['B', 'KB', 'MB', 'GB', 'TB']
        operator = random.choice(operators)
        size_value = random.randint(1, 1000)
        size_unit = random.choice(sizes)
        return f"{operator}{size_value}{size_unit}"
    elif arg_type == 'str':
        # Generate string filters like *ff, *, *test*, test*
        patterns = [
            '*',  # Match all
            '*test*',  # Contains
            'test*',  # Starts with
            '*test',  # Ends with
            'test',  # Exact match
        ]
        return random.choice(patterns)
    elif arg_type == 'list':
        # For list types, return a single value or comma-separated values
        if random.choice([True, False]):
            return 'test'
        else:
            return 'test1,test2'
    elif arg_type == 'bool':
        return random.choice([True, False])
    else:
        return '*'


def generate_random_args_for_command(command_name: str, template_parser: TemplateParser, config: dict) -> dict:
    """Generate random arguments for a command based on its template configuration."""
    random_args = {}
    random_cluster = None
    
    # Get arguments for this command
    args_config = template_parser.get_arguments(command_name)
    
    # Get clusters from config
    if config.get('clusters'):
        random_cluster = random.choice(config['clusters'])
        random_args['cluster'] = random_cluster.get('cluster_name') or random_cluster['cluster']
    
    # Generate random values for each argument
    for arg_config in args_config:
        arg_name = arg_config.get('name', '')
        # Normalize field name to Python/CLI format (spaces to underscores)
        arg_name_normalized = to_python_name(arg_name)
        arg_mandatory = arg_config.get('mandatory', False)
        arg_type = arg_config.get('type', 'str')
        
        # Skip cluster as we already set it
        if arg_name_normalized == 'cluster' or arg_name_normalized in random_args:
            continue
        
        # Only generate values for filterable arguments (or if mandatory)
        if arg_config.get('filter', False) or arg_mandatory:
            # Special handling for tenant
            if arg_name_normalized == 'tenant':
                tenants = ['default', 'tenant1']
                if random_cluster.get('tenant'):
                    tenants.append(random_cluster['tenant'])
                random_args[arg_name_normalized] = random.choice(tenants)
            else:
                random_args[arg_name_normalized] = generate_random_filter_value(arg_config)
    
    # Add order and top for all commands (these are common CLI args)
    order_fields = ['name', 'path', 'tenant']
    # Try to get a field name from the command template
    template = template_parser.get_command_template(command_name)
    if template:
        fields = template.get('fields', [])
        for field in fields:
            if isinstance(field, dict) and field.get('name'):
                order_fields.append(field['name'].replace(' ', '_'))
    
    order_directions = ['asc', 'desc', 'a', 'd']
    random_args['order'] = f"{random.choice(order_fields)}:{random.choice(order_directions)}"
    random_args['top'] = random.randint(5, 20)
    
    return random_args




def main():
    """Main test function."""
    print("="*60)
    print("VAST ADMIN MCP - TEST SCRIPT")
    print("="*60)
    print("\nThis script tests:")
    print("  1. List commands (views, tenants, viewpolicies, etc.)")
    print("  2. Create view from template named 'vmware'")
    print("  3. Views command:")
    print("     - Once without arguments")
    print("     - Once with all available arguments (random values)")
    print("="*60)
    
    # Test 1: List commands
    list_results = test_list_commands()
    
    # Test 2: Create view from template
    create_result = test_create_view_from_template()
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    # Count successful tests
    commands_tested = 0
    commands_with_args = 0
    if list_results:
        for cmd, cmd_results in list_results.items():
            if isinstance(cmd_results, dict):
                if cmd_results.get('without_args'):
                    commands_tested += 1
                if cmd_results.get('with_random_args'):
                    commands_with_args += 1
            elif cmd_results:
                commands_tested += 1
    
    print(f"List commands tested (without args): {commands_tested}")
    print(f"List commands tested (with random args): {commands_with_args}")
    print(f"Create view from template: {'✓' if create_result else '✗'}")
    print("="*60)


if __name__ == '__main__':
    main()

