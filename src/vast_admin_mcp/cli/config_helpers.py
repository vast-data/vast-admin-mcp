"""Configuration helper functions for MCP setup."""

import os
import json
import platform
from typing import Dict, Tuple, List


def _get_claude_desktop_config_path() -> str:
    """Get Claude Desktop config path based on operating system."""
    system = platform.system()
    if system == 'Darwin':  # macOS
        return os.path.expanduser('~/Library/Application Support/Claude/claude_desktop_config.json')
    elif system == 'Windows':
        appdata = os.environ.get('APPDATA', '')
        return os.path.join(appdata, 'Claude', 'claude_desktop_config.json')
    else:  # Linux
        return os.path.expanduser('~/.config/Claude/claude_desktop_config.json')


def _get_mcp_tool_config(tool_name: str) -> Dict[str, str]:
    """Get MCP tool configuration (config path, section name, tool display name).
    
    Args:
        tool_name: Name of the tool ('cursor', 'claude-desktop', 'windsurf', 'vscode', 'gemini-cli')
        
    Returns:
        Dictionary with 'config_path', 'section_name', 'tool_display_name', and 'restart_instruction'
        
    Raises:
        ValueError: If tool_name is not recognized
    """
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


def _configure_mcp_tool(tool_name: str, command_base: str, args: List[str]) -> None:
    """Configure MCP server for a specific tool - shows instructions only.
    
    This unified function replaces the four separate _configure_* functions.
    
    Args:
        tool_name: Name of the tool ('cursor', 'claude-desktop', 'windsurf', 'vscode', 'gemini-cli')
        command_base: Base command to run the MCP server
        args: Arguments to pass to the MCP server command
    """
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
    return os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER') == 'true'

