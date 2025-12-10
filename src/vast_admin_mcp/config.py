"""Configuration, constants, and enums for vast-admin-mcp."""

import os
import json
import logging
from enum import Enum
from pathlib import Path
import sys

# Import version from single source of truth
from .__about__ import __version__

# Constants
VERSION = __version__  # Re-export for backward compatibility

# Config file location
# Cluster configuration file (only in user directory, no env var override)
CONFIG_FILE = os.path.join(os.path.expanduser("~"), '.vast-admin-mcp/config.json')

# YAML template file locations
# Default template in project root (can be overridden via env var)
# Try to find template file in multiple locations:
# 1. Environment variable override
# 2. Package data (when installed)
# 3. Project root (when running from source)
def _find_default_template():
    """Find the default template file in multiple locations."""
    # 1. Check environment variable override
    env_path = os.environ.get('VAST_ADMIN_MCP_DEFAULT_TEMPLATE_FILE')
    if env_path and os.path.exists(env_path):
        return env_path
    
    # 2. Try package root (for installed packages)
    # When installed, the template file is included in the package root
    try:
        # Get the package directory
        package_dir = Path(__file__).parent
        # Look for template file in package root (one level up from config.py's directory)
        # This works when installed because the file is included at the package level
        installed_template = package_dir / 'mcp_list_cmds_template.yaml'
        if installed_template.exists():
            return str(installed_template)
    except (OSError, AttributeError):
        # Package data not found, continue to next option
        pass
    
    # 3. Try project root (for development)
    project_root_template = Path(__file__).parent.parent.parent / 'mcp_list_cmds_template.yaml'
    if project_root_template.exists():
        return str(project_root_template)
    
    return None

_DEFAULT_TEMPLATE_FILE_DEFAULT = _find_default_template()
DEFAULT_TEMPLATE_FILE = _DEFAULT_TEMPLATE_FILE_DEFAULT

# User template modifications file (can be overridden via env var)
_TEMPLATE_MODIFICATIONS_FILE_DEFAULT = os.path.join(os.path.expanduser("~"), '.vast-admin-mcp/mcp_list_template_modifications.yaml')
TEMPLATE_MODIFICATIONS_FILE = os.environ.get('VAST_ADMIN_MCP_TEMPLATE_MODIFICATIONS_FILE', _TEMPLATE_MODIFICATIONS_FILE_DEFAULT)

# View templates file location (can be overridden via env var)
_VIEW_TEMPLATE_FILE_DEFAULT = os.path.join(os.path.expanduser("~"), '.vast-admin-mcp/view_templates.json')
VIEW_TEMPLATE_FILE = os.environ.get('VAST_ADMIN_MCP_VIEW_TEMPLATE_FILE', _VIEW_TEMPLATE_FILE_DEFAULT)

# Log file location
LOG_PATH = os.path.join(os.path.expanduser("~"), '.vast-admin-mcp/vast_admin_mcp.log')

# REST page size for API calls, maximum number of items to retrieve from API per page 
REST_PAGE_SIZE = 1000

# Query users default top limit
QUERY_USERS_DEFAULT_TOP = 20  # Default number of results for query_users function
QUERY_USERS_MAX_TOP = 50  # Maximum number of results allowed by API (hardcoded API limit)

# performance aggregation function in list_performance function
PERFORMANCE_AGGREGATION_FUNCTION = "max"  # when presenting performance metrics, use max aggregation

# Performance metrics constants
MAX_VIEW_TIMEFRAME_SECONDS = 28800  # 8 hours - maximum timeframe for view performance metrics
METRICS_API_LIMIT = 10000  # Maximum number of metrics to retrieve from API
GRANULARITY_THRESHOLD_SECONDS = 14400  # 4 hours - threshold for minutes granularity
GRANULARITY_THRESHOLD_HOURS = 172800  # 2 days - threshold for hours granularity
GRANULARITY_THRESHOLD_DAYS = 864000  # 10 days - threshold for days granularity

# Excluded metric patterns for view performance
EXCLUDED_VIEW_METRIC_PATTERNS = ['squares', 's3', 'rpc', 'time']

# jq command timeout
JQ_TIMEOUT_SECONDS = 5

# API request timeouts (in seconds)
API_CONNECT_TIMEOUT = 5  # Connection timeout for API requests
API_READ_TIMEOUT = 10  # Read timeout for API requests
API_MAX_RETRIES = 1  # Maximum number of retries for failed API requests

# Logging constants
LOG_FILE_MAX_BYTES = 548576  # 0.5 MB - maximum size of log file before rotation
LOG_FILE_BACKUP_COUNT = 5  # Number of backup log files to keep

# Security constants
PBKDF2_ITERATIONS = 100000  # Number of iterations for PBKDF2 key derivation
ENCRYPTION_KEY_FILE_PERMISSIONS = 0o600  # File permissions for encryption key file (read/write for owner only)

# Graph generation constants
GRAPH_TEMP_DIR = os.path.join(os.path.expanduser("~"), '.vast-admin-mcp/temp_graphs/')
GRAPH_CLEANUP_AGE_HOURS = 24  # Clean up graph files older than this many hours

# Enums for validation
class OutputFormat(str, Enum):
    table = "table"
    json = "json"
    csv = "csv"

class ProtectionType(str, Enum):
    async_repl = "a-sync"
    sync = "sync"
    local_snaps = "local_snaps"
    global_access = "global_access"

class ObjectType(str, Enum):
    cluster = "cluster"
    cnode = "cnode"
    host = "host"
    user = "user"
    vippool = "vippool"
    view = "view"
    tenant = "tenant"

from .cache import get_cache_manager

_cache_manager = get_cache_manager()


def load_config(force_reload: bool = False):
    """Load configuration from config file with caching.
    
    The configuration is cached in memory to avoid redundant file I/O operations.
    The cache is invalidated if the file modification time changes.
    
    Args:
        force_reload: If True, bypass cache and reload from file
        
    Returns:
        Configuration dictionary
        
    Raises:
        ValueError: If config file not found or cannot be loaded
    """
    # Check if file exists
    if not os.path.isfile(CONFIG_FILE):
        raise ValueError(f"config file:{CONFIG_FILE} not found. Please run setup command first.")
    
    # Get file modification time
    try:
        current_mtime = os.path.getmtime(CONFIG_FILE)
    except OSError:
        # File might have been deleted, clear cache
        _cache_manager.clear('config')
        raise ValueError(f"config file:{CONFIG_FILE} not found. Please run setup command first.")
    
    # Return cached config if valid and not forcing reload
    if not force_reload:
        cached_mtime = _cache_manager.get('config', '_file_mtime')
        cached_config = _cache_manager.get('config', '_data')
        if cached_config is not None and cached_mtime == current_mtime:
            return cached_config
    
    # Load config from file
    try:
        with open(CONFIG_FILE, 'r') as config_file:
            config = json.load(config_file)
        
        # Update cache
        _cache_manager.set('config', '_data', config)
        _cache_manager.set('config', '_file_mtime', current_mtime)
        
        return config
    except json.JSONDecodeError as e:
        raise ValueError(f"Error parsing config file:{CONFIG_FILE}. Error: {e}")
    except Exception as e:
        raise ValueError(f"Error loading config file:{CONFIG_FILE}. Error: {e}")


def clear_config_cache():
    """Clear the configuration cache.
    
    Useful for testing or when you know the config has been modified externally.
    """
    _cache_manager.clear('config')

def save_config(config: dict):
    """Save configuration to config file."""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        with open(CONFIG_FILE, 'w') as config_file:
            json.dump(config, config_file, indent=2)
    except Exception as e:
        raise ValueError(f"Error saving config file:{CONFIG_FILE}. Error: {e}")


def get_default_template_path():
    """Get the path to the default template file.
    
    Returns:
        Path string to default template file, or None if file doesn't exist
    """
    if os.path.exists(DEFAULT_TEMPLATE_FILE):
        return DEFAULT_TEMPLATE_FILE
    return None

