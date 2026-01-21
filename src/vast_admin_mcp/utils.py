"""Utility functions for vast-admin-mcp: password management, formatting, validation, and logging."""

import base64
import os
import sys
import json
import csv
import logging
import logging.handlers
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple

from .config import (
    CONFIG_FILE, LOG_PATH, TEMPLATE_MODIFICATIONS_FILE, get_default_template_path,
    LOG_FILE_MAX_BYTES, LOG_FILE_BACKUP_COUNT, PBKDF2_ITERATIONS, ENCRYPTION_KEY_FILE_PERMISSIONS
)

# Try to import keyring for secure password storage
try:
    import keyring
    # Check if we should force encrypted storage (e.g., in Docker)
    if os.environ.get('FORCE_ENCRYPTED_STORAGE', '').lower() in ('true', '1', 'yes'):
        KEYRING_AVAILABLE = False
        logging.info("Keyring disabled (FORCE_ENCRYPTED_STORAGE=true). Using encrypted file storage.")
    else:
        KEYRING_AVAILABLE = True
except ImportError:
    KEYRING_AVAILABLE = False
    logging.warning("keyring library not available. Passwords will be stored with encryption instead of OS keyring.")

# Try to import cryptography for password encryption
try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logging.warning("cryptography library not available. Passwords will be stored with base64 encoding (NOT secure).")

def _get_keyring_service_name():
    """Get the keyring service name for this application"""
    return "vast-admin-mcp"

def _get_encryption_key():
    """Generate or retrieve encryption key for password encryption"""
    key_file = os.path.join(os.path.expanduser("~"), '.vast-admin-mcp/encryption.key')
    
    if os.path.exists(key_file):
        with open(key_file, 'rb') as f:
            return f.read()
    else:
        # Generate new key
        if not os.path.exists(os.path.dirname(key_file)):
            os.makedirs(os.path.dirname(key_file))
        
        # Use a salt and derive key from machine-specific info
        salt = os.urandom(16)
        machine_info = (os.uname().nodename + os.environ.get('USER', 'default')).encode()
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=PBKDF2_ITERATIONS,
        )
        key = base64.urlsafe_b64encode(kdf.derive(machine_info))
        
        # Save key with salt
        with open(key_file, 'wb') as f:
            f.write(salt + key)
        
        # Set restrictive permissions
        os.chmod(key_file, ENCRYPTION_KEY_FILE_PERMISSIONS)
        
        return salt + key

def store_password_secure(cluster: str, username: str, password: str) -> str:
    """
    Store password securely using the best available method.
    Returns a reference/identifier for retrieving the password later.
    """
    if KEYRING_AVAILABLE:
        # Method 1: OS Keyring (most secure)
        service = _get_keyring_service_name()
        account = f"{cluster}:{username}"
        keyring.set_password(service, account, password)
        logging.debug(f"Password stored in OS keyring for {cluster}:{username}")
        return f"keyring:{account}"
    
    elif CRYPTO_AVAILABLE:
        # Method 2: Encrypted storage (good security)
        try:
            key_data = _get_encryption_key()
            salt = key_data[:16]
            key = key_data[16:]
            
            fernet = Fernet(key)
            encrypted_password = fernet.encrypt(password.encode())
            encoded = base64.urlsafe_b64encode(encrypted_password).decode()
            
            logging.debug(f"Password encrypted and stored for {cluster}:{username}")
            return f"encrypted:{encoded}"
        except Exception as e:
            logging.warning(f"Encryption failed, falling back to base64: {e}")
            # Fall through to base64
    
    # Method 3: Base64 encoding (backwards compatibility, not secure)
    logging.warning(f"Using base64 encoding for password storage (NOT secure). Install 'keyring' or 'cryptography' for better security.")
    encoded = base64.b64encode(password.encode()).decode()
    return f"base64:{encoded}"

def retrieve_password_secure(cluster: str, username: str, password_ref: str) -> str:
    """
    Retrieve password securely based on the storage method used.
    """
    if password_ref.startswith("keyring:"):
        if not KEYRING_AVAILABLE:
            raise ValueError("Password stored in keyring but keyring library not available")
        
        service = _get_keyring_service_name()
        account = password_ref[8:]  # Remove "keyring:" prefix
        try:
            password = keyring.get_password(service, account)
            if password is None:
                # Keyring entry not found - this can happen in Docker when host keyring is not accessible
                # Try to provide a helpful error message
                import os
                if os.path.exists('/.dockerenv') or os.environ.get('KEYRING_BACKEND'):
                    raise ValueError(
                        f"Password not found in keyring for {account}. "
                        f"In Docker, passwords stored in the host OS keyring are not accessible. "
                        f"Please re-run 'vast-admin-mcp setup' inside the Docker container to store passwords using the container's keyring."
                    )
                raise ValueError(f"Password not found in keyring for {account}")
            return password
        except Exception as e:
            # If keyring access fails (e.g., in Docker), provide helpful error
            import os
            if os.path.exists('/.dockerenv') or os.environ.get('KEYRING_BACKEND'):
                raise ValueError(
                    f"Failed to retrieve password from keyring for {account}: {e}. "
                    f"In Docker, passwords stored in the host OS keyring are not accessible. "
                    f"Please re-run 'vast-admin-mcp setup' inside the Docker container."
                )
            raise
    
    elif password_ref.startswith("encrypted:"):
        if not CRYPTO_AVAILABLE:
            raise ValueError("Password encrypted but cryptography library not available")
        
        try:
            key_data = _get_encryption_key()
            key = key_data[16:]  # Skip salt
            
            fernet = Fernet(key)
            encrypted_data = base64.urlsafe_b64decode(password_ref[10:].encode())
            password = fernet.decrypt(encrypted_data).decode()
            return password
        except Exception as e:
            raise ValueError(f"Failed to decrypt password: {e}")
    
    elif password_ref.startswith("base64:"):
        # Backwards compatibility
        try:
            return base64.b64decode(password_ref[7:]).decode()
        except Exception as e:
            raise ValueError(f"Failed to decode base64 password: {e}")
    
    else:
        # Legacy format - assume base64
        try:
            return base64.b64decode(password_ref).decode()
        except Exception as e:
            raise ValueError(f"Failed to decode legacy password format: {e}")

def delete_password_secure(cluster: str, username: str, password_ref: str):
    """
    Delete stored password securely.
    """
    if password_ref.startswith("keyring:"):
        if KEYRING_AVAILABLE:
            service = _get_keyring_service_name()
            account = password_ref[8:]
            try:
                keyring.delete_password(service, account)
                logging.debug(f"Password deleted from keyring for {account}")
            except Exception as e:
                logging.warning(f"Could not delete password from keyring: {e}")

def migrate_password_storage():
    """
    Migrate existing base64 passwords to more secure storage.
    Called automatically when config is loaded.
    """
    if not (KEYRING_AVAILABLE or CRYPTO_AVAILABLE):
        return  # No better storage available
    
    if not os.path.exists(CONFIG_FILE):
        return
    
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        
        modified = False
        for cluster_info in config.get('clusters', []):
            password_ref = cluster_info.get('password', '')
            
            # Check if password is in old base64 format (no prefix)
            if password_ref and not any(password_ref.startswith(prefix) for prefix in ['keyring:', 'encrypted:', 'base64:']):
                try:
                    # This is legacy base64 format
                    old_password = base64.b64decode(password_ref).decode()
                    new_password_ref = store_password_secure(
                        cluster_info['cluster'],
                        cluster_info['username'],
                        old_password
                    )
                    cluster_info['password'] = new_password_ref
                    modified = True
                    logging.info(f"Migrated password storage for cluster {cluster_info['cluster']} to more secure method")
                except Exception as e:
                    logging.warning(f"Could not migrate password for cluster {cluster_info['cluster']}: {e}")
        
        if modified:
            # Save updated config
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=2)
            logging.info("Password storage migration completed")
    
    except Exception as e:
        logging.error(f"Error during password migration: {e}")

# Initialize logging
def logging_main(debug: bool = False):
    """Initialize logging configuration."""
    # Get the root logger
    log = logging.getLogger()
    
    # Check if logging is already configured to avoid duplicate handlers
    # We'll use a custom attribute to track if we've already set up logging
    if hasattr(logging_main, '_configured'):
        # Just update the debug level if already configured
        for handler in log.handlers:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stderr:
                handler.setLevel(logging.DEBUG if debug else logging.INFO)
        return
    
    # Clear any existing handlers to avoid duplicate logging or stderr handlers
    # Remove handlers one by one and close them properly
    while log.handlers:
        handler = log.handlers[0]
        handler.close()
        log.removeHandler(handler)
    
    # Also clear handlers from any existing child loggers
    # Keep propagation enabled so child loggers send messages to root
    for logger_name in list(logging.root.manager.loggerDict.keys()):
        logger = logging.getLogger(logger_name)
        while logger.handlers:
            handler = logger.handlers[0]
            handler.close()
            logger.removeHandler(handler)
        logger.propagate = True  # Enable propagation to root logger
    
    # Disable the lastResort handler that writes to stderr
    logging.lastResort = None
    
    log.setLevel(logging.DEBUG)
    
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatterdebug = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
    
    # create file handler which logs even debug messages
    fh = logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=LOG_FILE_MAX_BYTES, backupCount=LOG_FILE_BACKUP_COUNT)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatterdebug)
    log.addHandler(fh)
    
    # log info to the console unless in debug mode (use stderr for MCP compatibility)
    # Only add one console handler
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(logging.INFO)
    if debug: 
        ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    log.addHandler(ch)
    
    # Mark as configured
    logging_main._configured = True
    
    logging.debug("starting " + os.path.basename(sys.argv[0]))

# Query user for yes/no question
def query_yes_no(question, default="no"):
    """Query user for a yes/no question."""
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

# Parse time duration string to seconds (e.g., 2d, 3w, 1d6h, 30m, 1h30m)
def parse_time_duration(duration_str: str) -> int:
    """
    Parse time duration string and return seconds.
    Supports formats like: 2d, 3w, 1d6h, 30m, 1h30m, etc.
    
    Supported units:
    - s: seconds
    - m: minutes
    - h: hours
    - d: days
    - w: weeks
    """
    if not duration_str or not duration_str.strip():
        raise ValueError("Duration string cannot be empty")
    
    # Convert to lowercase for easier parsing
    duration_str = duration_str.lower().strip()
    
    # Define unit multipliers in seconds
    units = {
        's': 1,          # seconds
        'm': 60,         # minutes
        'h': 3600,       # hours
        'd': 86400,      # days
        'w': 604800      # weeks
    }
    
    total_seconds = 0
    current_number = ""
    used_units = set()  # Track which units have been used to prevent duplicates
    
    for char in duration_str:
        if char.isdigit():
            current_number += char
        elif char in units:
            if current_number:
                if char in used_units:
                    raise ValueError(f"Invalid duration format: '{duration_str}' - unit '{char}' used multiple times")
                used_units.add(char)
                total_seconds += int(current_number) * units[char]
                current_number = ""
            else:
                raise ValueError(f"Invalid duration format: '{duration_str}' - number expected before unit '{char}'")
        else:
            raise ValueError(f"Invalid character '{char}' in duration: '{duration_str}'")
    
    if current_number:
        raise ValueError(f"Invalid duration format: '{duration_str}' - unit expected after number '{current_number}'")
    
    if total_seconds == 0:
        raise ValueError(f"Invalid duration: '{duration_str}' - no valid time units found")
    
    return total_seconds


# Convert size in bytes to "pretty" size (size in KB, MB, GB, or TB)
def pretty_size(size_in_bytes: str, num_decimal_points: int = 2) -> str:
    """Convert bytes to human-readable size string using logarithmic calculation.
    
    Args:
        size_in_bytes: Size in bytes (as string or number)
        num_decimal_points: Number of decimal points to display (default: 2)
        
    Returns:
        Human-readable size string (e.g., "1.23 GB")
    """
    import math
    
    try:
        size = float(size_in_bytes)
    except (ValueError, TypeError):
        return str(size_in_bytes)
    
    if size == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    # Calculate unit index using logarithm (base 1000)
    # Clamp to valid unit range
    unit_index = min(int(math.log(size, 1000)), len(units) - 1)
    unit_index = max(0, unit_index)  # Ensure non-negative
    
    # Convert to appropriate unit
    value = size / (1000 ** unit_index)
    
    # Format with specified decimal points
    return f"{value:.{num_decimal_points}f} {units[unit_index]}"

def format_time_delta(timestamp_str: str) -> str:
    """
    Convert ISO timestamp to human-readable time delta format.
    
    Converts timestamps to "Xd Xh Xm Xs ago" (past) or "in Xd Xh Xm Xs" (future) format.
    
    Supported formats:
    - "2025-12-10T17:40:53Z" (without microseconds)
    - "2025-11-26T21:18:36.547643Z" (with microseconds)
    - "2025-12-10T17:40:53+00:00" (with timezone offset)
    - "2025-12-10T17:40:53" (without timezone, assumes UTC)
    
    Args:
        timestamp_str: ISO format timestamp string
        
    Returns:
        Human-readable time delta string (e.g., "3d 1h 45m 38s ago" or "in 2h 30m 15s")
        Returns original string if parsing fails
    """
    if not timestamp_str or not timestamp_str.strip():
        return str(timestamp_str)
    
    try:
        # Parse ISO format timestamp
        # Handle both with and without microseconds, with and without timezone
        timestamp_str = timestamp_str.strip()
        
        # Handle 'Z' suffix (UTC timezone indicator)
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1]
            has_timezone = True
            tz_offset = '+00:00'
        elif '+' in timestamp_str or timestamp_str.count('-') > 2:
            # Has timezone info already
            has_timezone = True
            tz_offset = None
        else:
            # No timezone info, assume UTC
            has_timezone = False
            tz_offset = '+00:00'
        
        # Try parsing with different formats
        # First try with timezone if we have it or need to add it
        formats_with_tz = []
        formats_without_tz = []
        
        if has_timezone or tz_offset:
            # Formats with timezone
            if tz_offset:
                # We need to add timezone
                formats_with_tz = [
                    (timestamp_str + tz_offset, '%Y-%m-%dT%H:%M:%S.%f%z'),  # With microseconds
                    (timestamp_str + tz_offset, '%Y-%m-%dT%H:%M:%S%z'),      # Without microseconds
                ]
            else:
                # Timezone already in string
                formats_with_tz = [
                    (timestamp_str, '%Y-%m-%dT%H:%M:%S.%f%z'),  # With microseconds and timezone
                    (timestamp_str, '%Y-%m-%dT%H:%M:%S%z'),      # Without microseconds, with timezone
                ]
        
        # Formats without timezone (will add UTC later)
        formats_without_tz = [
            (timestamp_str, '%Y-%m-%dT%H:%M:%S.%f'),     # With microseconds, without timezone
            (timestamp_str, '%Y-%m-%dT%H:%M:%S'),        # Without microseconds, without timezone
        ]
        
        dt = None
        # Try formats with timezone first
        for ts_str, fmt in formats_with_tz:
            try:
                dt = datetime.strptime(ts_str, fmt)
                break
            except ValueError:
                continue
        
        # If that didn't work, try formats without timezone
        if dt is None:
            for ts_str, fmt in formats_without_tz:
                try:
                    dt = datetime.strptime(ts_str, fmt)
                    # Add UTC timezone if not present
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
        
        if dt is None:
            # If parsing failed, return original string
            return str(timestamp_str)
        
        # Ensure timezone-aware datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Calculate time difference from now
        now = datetime.now(timezone.utc)
        delta = now - dt
        
        # Handle future timestamps - show "in Xh Xm Xs" format
        is_future = delta.total_seconds() < 0
        if is_future:
            # For future timestamps, calculate the absolute difference
            total_seconds = int(abs(delta.total_seconds()))
            days = total_seconds // 86400
            hours = (total_seconds % 86400) // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            # Build "in X" string for future timestamps
            parts = []
            if days > 0:
                parts.append(f"{days}d")
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0:
                parts.append(f"{minutes}m")
            if seconds > 0 or not parts:  # Always show seconds if no other parts
                parts.append(f"{seconds}s")
            
            return "in " + " ".join(parts)
        
        # Extract components for past timestamps
        total_seconds = int(delta.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        # Build time ago string
        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds > 0 or not parts:  # Always show seconds if no other parts
            parts.append(f"{seconds}s")
        
        return " ".join(parts) + " ago"
        
    except (ValueError, AttributeError, TypeError) as e:
        # If parsing fails, return original string
        logging.debug(f"Failed to parse timestamp '{timestamp_str}': {e}")
        return str(timestamp_str)


def parse_capacity_value(capacity_str: str) -> tuple[str, int]:
    """
    Parse capacity string with units and convert to bytes.
    
    Supports formats like: ">1TB", ">=500GB", "<1M", "1TB", "=100GB"
    Units: B, KB, MB, GB, TB, PB (case-insensitive)
    Operators: >, >=, <, <=, = (or no operator for equals)
    
    Args:
        capacity_str: Capacity string with optional operator and unit
        
    Returns:
        Tuple of (operator, value_in_bytes)
        operator: One of 'gt', 'gte', 'lt', 'lte', 'eq'
        value_in_bytes: Integer value in bytes
    """
    import re
    
    if not capacity_str or not capacity_str.strip():
        raise ValueError("Capacity string cannot be empty")
    
    capacity_str = capacity_str.strip()
    
    # Unit multipliers (base 1024)
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4,
        'PB': 1024 ** 5
    }
    
    # Match operator and value with unit
    # Pattern: optional operator (>, >=, <, <=, =) followed by number and unit
    pattern = r'^(>=|<=|>|<|=)?\s*([\d.]+)\s*([A-Za-z]+)$'
    match = re.match(pattern, capacity_str)
    
    if not match:
        raise ValueError(f"Invalid capacity format: '{capacity_str}'. Expected format: '[operator]number[unit]' (e.g., '>1TB', '>=500GB')")
    
    operator_str = match.group(1) or '='
    number_str = match.group(2)
    unit_str = match.group(3).upper()
    
    # Handle shorthand units: M -> MB, K -> KB, G -> GB, T -> TB, P -> PB
    unit_aliases = {
        'K': 'KB',
        'M': 'MB',
        'G': 'GB',
        'T': 'TB',
        'P': 'PB'
    }
    if unit_str in unit_aliases:
        unit_str = unit_aliases[unit_str]
    
    # Normalize operator
    if operator_str == '>=':
        operator = 'gte'
    elif operator_str == '<=':
        operator = 'lte'
    elif operator_str == '>':
        operator = 'gt'
    elif operator_str == '<':
        operator = 'lt'
    elif operator_str == '=':
        operator = 'eq'
    else:
        operator = 'eq'
    
    # Validate unit
    if unit_str not in units:
        raise ValueError(f"Invalid unit: '{unit_str}'. Supported units: B, KB, MB, GB, TB, PB")
    
    # Convert to bytes
    try:
        number = float(number_str)
        value_in_bytes = int(number * units[unit_str])
    except ValueError:
        raise ValueError(f"Invalid number in capacity string: '{number_str}'")
    
    return (operator, value_in_bytes)


def parse_filter_value(filter_str: str, arg_type: str) -> tuple[str, Any, str]:
    """
    Parse user-friendly filter pattern and convert to VAST API format.
    
    Args:
        filter_str: User-friendly filter pattern
        arg_type: Argument type ('str', 'int', 'bool', 'capacity')
        
    Returns:
        Tuple of (filter_type, filter_value, api_suffix)
        filter_type: One of 'contains', 'not_contains', 'equals', 'starts_with', 
                     'ends_with', 'non_empty', 'lt', 'lte', 'gt', 'gte', 'bool'
        filter_value: Parsed and converted value
        api_suffix: API parameter suffix (e.g., '__icontains', '__lte', '__regex', '')
        
    Examples:
        String filters:
        - '*' → ('non_empty', '.+', '__regex') - matches any non-empty value
        - '*test*' → ('contains', 'test', '__icontains')
        - '!*test*' → ('not_contains', 'test', '__not_icontains')
        - 'test*' → ('starts_with', 'test', '__startswith')
        - '*test' → ('ends_with', 'test', '__endswith')
        - 'test' → ('equals', 'test', '')
        
        Numeric filters:
        - '>100' → ('gt', 100, '__gt')
        - '>=100' → ('gte', 100, '__gte')
        - '<100' → ('lt', 100, '__lt')
        - '<=100' → ('lte', 100, '__lte')
        - '100' → ('equals', 100, '')
        
        Boolean filters:
        - 'true', 'True', 'TRUE', '1' → ('bool', True, '')
        - 'false', 'False', 'FALSE', '0' → ('bool', False, '')
    """
    if not filter_str or not filter_str.strip():
        raise ValueError("Filter string cannot be empty")
    
    filter_str = filter_str.strip()
    
    # Handle boolean type
    if arg_type == 'bool':
        filter_lower = filter_str.lower()
        if filter_lower in ['true', '1']:
            return ('bool', True, '')
        elif filter_lower in ['false', '0']:
            return ('bool', False, '')
        else:
            raise ValueError(f"Invalid boolean value: '{filter_str}'. Use true/false, True/False, TRUE/FALSE, or 0/1")
    
    # Handle capacity type - parse with units
    if arg_type == 'capacity':
        operator, value_in_bytes = parse_capacity_value(filter_str)
        api_suffix_map = {
            'gt': '__gt',
            'gte': '__gte',
            'lt': '__lt',
            'lte': '__lte',
            'eq': ''
        }
        return (operator, value_in_bytes, api_suffix_map[operator])
    
    # Handle numeric type (int)
    if arg_type == 'int':
        # Check for comparison operators
        if filter_str.startswith('>='):
            try:
                value = int(filter_str[2:].strip())
                return ('gte', value, '__gte')
            except ValueError:
                raise ValueError(f"Invalid numeric value after '>=': '{filter_str[2:]}'")
        elif filter_str.startswith('<='):
            try:
                value = int(filter_str[2:].strip())
                return ('lte', value, '__lte')
            except ValueError:
                raise ValueError(f"Invalid numeric value after '<=': '{filter_str[2:]}'")
        elif filter_str.startswith('>'):
            try:
                value = int(filter_str[1:].strip())
                return ('gt', value, '__gt')
            except ValueError:
                raise ValueError(f"Invalid numeric value after '>': '{filter_str[1:]}'")
        elif filter_str.startswith('<'):
            try:
                value = int(filter_str[1:].strip())
                return ('lt', value, '__lt')
            except ValueError:
                raise ValueError(f"Invalid numeric value after '<': '{filter_str[1:]}'")
        else:
            # Plain equals
            try:
                value = int(filter_str)
                return ('equals', value, '')
            except ValueError:
                raise ValueError(f"Invalid numeric value: '{filter_str}'")
    
    # Handle string type
    if arg_type == 'str':
        # Non-empty: * (matches any non-empty string using regex)
        if filter_str == '*':
            return ('non_empty', '.+', '__regex')
        # Not contains: !*value*
        elif filter_str.startswith('!*') and filter_str.endswith('*'):
            value = filter_str[2:-1]
            return ('not_contains', value, '__not_icontains')
        # Contains: *value*
        elif filter_str.startswith('*') and filter_str.endswith('*'):
            value = filter_str[1:-1]
            return ('contains', value, '__icontains')
        # Starts with: value*
        elif filter_str.endswith('*') and not filter_str.startswith('*'):
            value = filter_str[:-1]
            return ('starts_with', value, '__startswith')
        # Ends with: *value
        elif filter_str.startswith('*') and not filter_str.endswith('*'):
            value = filter_str[1:]
            return ('ends_with', value, '__endswith')
        else:
            # Plain equals
            return ('equals', filter_str, '')
    
    # Default: treat as string
    return ('equals', filter_str, '')


def output_results(data: List[Dict], format: str = "table", output_file: str = None):
    """
    Output results in various formats (table, json, csv).
    
    Args:
        data: List of dictionaries to output
        format: Output format - 'table', 'json', or 'csv'
        output_file: Optional file path to write output to
    """
    from tabulate import tabulate
    import json
    
    if not data:
        if format == "table":
            print("No results found.")
        return
    
    if format == "table":
        # Extract column headers from first row
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            # Filter out metadata fields
            headers = [h for h in headers if not h.startswith('_')]
            
            # Build rows
            rows = []
            for row in data:
                # Filter out metadata fields
                filtered_row = {k: v for k, v in row.items() if not k.startswith('_')}
                rows.append([filtered_row.get(h, '') for h in headers])
            
            table = tabulate(rows, headers=headers, tablefmt="grid")
            output = table
        else:
            output = str(data)
    elif format == "json":
        output = json.dumps(data, indent=2, default=str)
    elif format == "csv":
        import csv
        import io
        if isinstance(data[0], dict):
            headers = [h for h in data[0].keys() if not h.startswith('_')]
            output_buffer = io.StringIO()
            writer = csv.DictWriter(output_buffer, fieldnames=headers)
            writer.writeheader()
            for row in data:
                filtered_row = {k: v for k, v in row.items() if not k.startswith('_')}
                writer.writerow(filtered_row)
            output = output_buffer.getvalue()
        else:
            output = str(data)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(output)
        print(f"Output written to {output_file}")
    else:
        print(output)


def parse_order_spec(order_spec: str, field_mappings: Optional[Dict[str, str]] = None, use_raw_prefix: bool = False) -> Optional[Dict[str, str]]:
    """Parse order specification into field and direction.
    
    Supports formats:
    - "field:direction" (colon separator)
    - "field direction" (space separator)
    - "field" (defaults to ascending)
    
    Direction supports prefixes:
    - Ascending: a, as, asc, ascending (or any valid prefix)
    - Descending: d, de, dec, desc, descending (or any valid prefix)
    
    Args:
        order_spec: Order specification string (e.g., "field:desc", "field asc")
        field_mappings: Optional dict mapping display field names to raw field names
        use_raw_prefix: If True, prefix field name with "_raw_" for accessing raw values
        
    Returns:
        Dict with 'field' and 'direction' keys, or None if invalid
    """
    if not order_spec or not order_spec.strip():
        return None
    
    field = None
    direction_str = None
    
    # Check for minus prefix (undocumented feature for descending sort)
    # Supports common API pattern like "-field_name" meaning descending
    order_spec_stripped = order_spec.strip()
    prefix_descending = False
    if order_spec_stripped.startswith('-'):
        prefix_descending = True
        order_spec_stripped = order_spec_stripped[1:].strip()  # Remove minus and re-strip
    
    # If minus prefix was used, treat entire remaining string as field name (no separators)
    # This avoids ambiguity with field names containing spaces
    if prefix_descending:
        field = order_spec_stripped
        direction_str = 'desc'
    # Try colon format first: "field:direction"
    elif ':' in order_spec_stripped:
        parts = order_spec_stripped.split(':', 1)
        field = parts[0].strip()
        direction_str = parts[1].strip().lower() if len(parts) > 1 else None
    # Try space format: "field direction"
    elif ' ' in order_spec_stripped:
        parts = order_spec_stripped.strip().split(None, 1)  # Split on first space only
        field = parts[0].strip()
        direction_str = parts[1].strip().lower() if len(parts) > 1 else None
    else:
        # No separator - treat entire string as field name
        field = order_spec_stripped.strip()
        direction_str = None
    
    if not field:
        return None
    
    # Normalize field name: accept both space and underscore versions
    field_normalized = field.replace('_', ' ')
    
    # Map to raw field name if mappings provided
    if field_mappings:
        if field_normalized in field_mappings:
            field = field_mappings[field_normalized]
        elif field in field_mappings:
            field = field_mappings[field]
        else:
            # Use normalized field name if it doesn't match
            field = field_normalized if field_normalized in field_mappings else field
    else:
        field = field_normalized
    
    # Add raw prefix if requested
    if use_raw_prefix:
        # Normalize to underscores for _raw_ fields to match how they're stored
        field = normalize_field_name(field, 'to_underscore')
        field = f'_raw_{field}'
    
    # Normalize direction - support prefixes
    direction = 'asc'  # default
    if direction_str:
        direction_lower = direction_str.lower()
        # Check for ascending prefixes (a, as, asc, ascen, ascendi, ascending)
        if direction_lower.startswith('a'):
            # Validate it's a valid prefix of "ascending"
            if 'ascending'.startswith(direction_lower):
                direction = 'asc'
            else:
                return None  # Invalid prefix
        # Check for descending prefixes (d, de, dec, desc, desce, descend, etc.)
        elif direction_lower.startswith('d'):
            # Validate it's a valid prefix of "descending" or matches common abbreviations
            if ('descending'.startswith(direction_lower) or 
                'desc'.startswith(direction_lower) or 
                direction_lower in ['dec', 'desc', 'dece'] or
                (len(direction_lower) <= len('descending') and direction_lower.startswith('de'))):
                direction = 'dec'
            else:
                return None  # Invalid prefix
        else:
            return None  # Invalid direction (must start with 'a' or 'd')
    
    return {'field': field, 'direction': direction}


def get_size_in_bytes(size_str: str) -> int:
    """Convert human-readable size string to bytes."""
    if not size_str:
        raise ValueError("size string cannot be empty")
    
    size_str = size_str.strip().upper()
    
    # Define size units:
    # Decimal (base 1000): K, M, G, T, P
    # Binary (base 1024): KB, MB, GB, TB, PB
    # IEC Binary (base 1024): KiB, MiB, GiB, TiB, PiB (explicit base 2)
    units = {
        # IEC binary units (longest strings first for proper matching)
        'PIB': 1024**5, 'PB': 1024**5,
        'TIB': 1024**4, 'TB': 1024**4,
        'GIB': 1024**3, 'GB': 1024**3,
        'MIB': 1024**2, 'MB': 1024**2,
        'KIB': 1024,    'KB': 1024,
        # Decimal units (base 1000)
        'P': 1000**5,
        'T': 1000**4,
        'G': 1000**3,
        'M': 1000**2,
        'K': 1000,
        'B': 1
    }
    
    # Try to match the pattern: number + unit
    # Check longest units first (e.g., TiB before TB, TB before T)
    for unit, multiplier in sorted(units.items(), key=lambda x: len(x[0]), reverse=True):
        if size_str.endswith(unit):
            try:
                number_part = size_str[:-len(unit)].strip()
                if not number_part:
                    raise ValueError(f"No numeric value before unit '{unit}'")
                
                # Parse the numeric part (supports decimals)
                value = float(number_part)
                if value < 0:
                    raise ValueError("Size cannot be negative")
                
                # Calculate bytes
                bytes_value = int(value * multiplier)
                return bytes_value
            except ValueError as e:
                if "could not convert" in str(e):
                    raise ValueError(f"Invalid number format: '{number_part}'")
                raise
    
    # If no unit found, try to parse as plain number (bytes)
    try:
        value = float(size_str)
        if value < 0:
            raise ValueError("Size cannot be negative")
        return int(value)
    except ValueError:
        raise ValueError(f"Invalid size format: '{size_str}'. Expected formats: 10T, 1GB, 17.5K, 5TiB, etc.")


def validate_path(path: str) -> None:
    """Validate that path is absolute and normalized."""
    if not path:
        raise ValueError("path is required")
    
    # Check if path is absolute (starts with /)
    if not os.path.isabs(path):
        raise ValueError(f"path must be an absolute path starting with '/' (got: {path})")
    
    # Normalize and check if path changed (detects .., ., //, etc.)
    normalized = os.path.normpath(path)
    if normalized != path or (len(path) > 1 and path.endswith('/')):
        raise ValueError(f"path must be normalized without '.', '..', '//' or trailing slash (got: {path})")


def format_simple_datetime(timestamp_str: str) -> str:
    """
    Convert ISO timestamp to simple date and time format.
    
    Args:
        timestamp_str: ISO timestamp (e.g., '2025-11-01T22:03:22.995564Z')
    
    Returns:
        Simple formatted string (e.g., '2025-11-01 22:03:22')
    """
    if not timestamp_str or timestamp_str == 'N/A' or timestamp_str == '':
        return 'N/A'
    
    try:
        # Parse ISO timestamp
        if timestamp_str.endswith('Z'):
            # UTC timestamp - remove Z and add UTC timezone
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '')).replace(tzinfo=timezone.utc)
        elif '+' in timestamp_str or '-' in timestamp_str[-6:]:  # Check for timezone offset
            # Already has timezone info
            timestamp = datetime.fromisoformat(timestamp_str)
        else:
            # No timezone info, assume UTC
            timestamp = datetime.fromisoformat(timestamp_str).replace(tzinfo=timezone.utc)
        
        # Format as simple date and time (YYYY-MM-DD HH:MM:SS)
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse timestamp '{timestamp_str}': {e}")
        return timestamp_str  # Return original if parsing fails


def apply_ordering(data: List[Dict], order_configs: List[Dict[str, str]], remove_raw_fields: bool = False) -> List[Dict]:
    """Apply ordering to data using order configurations.
    
    Args:
        data: List of dictionaries to sort
        order_configs: List of order config dicts with 'field' and 'direction' keys
        remove_raw_fields: If True, remove fields starting with '_raw_' after sorting
        
    Returns:
        Sorted list of dictionaries
    """
    if not order_configs or not data:
        return data
    
    # Apply sorting in reverse order (last sort is primary)
    sorted_data = data
    for order_config in reversed(order_configs):
        field = order_config['field']
        direction = order_config['direction']
        reverse = (direction == 'dec')
        
        # Sort key function that handles None and numeric values properly
        def sort_key(x):
            value = x.get(field)
            # Handle None values - treat as 0 for numeric fields, empty string for others
            if value is None:
                # Check if this is a numeric field based on field name
                if isinstance(field, str) and any(keyword in field.lower() for keyword in ['capacity', 'size', 'limit', 'used', 'quota']):
                    return 0
                return ''
            # Return numeric value for proper numeric sorting
            if isinstance(value, (int, float)):
                return value
            # For strings, return as-is
            return value
        
        sorted_data = sorted(sorted_data, key=sort_key, reverse=reverse)
    
    # Remove raw field values if requested
    if remove_raw_fields:
        for row in sorted_data:
            keys_to_remove = [k for k in row.keys() if k.startswith('_raw_')]
            for k in keys_to_remove:
                del row[k]
    
    return sorted_data


def normalize_field_name(field_name: str, direction: str = 'to_underscore') -> str:
    """Normalize field name between space-separated and underscore-separated formats.
    
    Args:
        field_name: Field name to normalize
        direction: 'to_underscore' (default) converts "logical used" -> "logical_used",
                   'to_space' converts "logical_used" -> "logical used"
    
    Returns:
        Normalized field name
    """
    if direction == 'to_underscore':
        # Replace spaces and hyphens with underscores
        normalized = field_name.replace(' ', '_').replace('-', '_')
        # Remove multiple consecutive underscores
        while '__' in normalized:
            normalized = normalized.replace('__', '_')
        # Remove leading/trailing underscores
        normalized = normalized.strip('_')
        return normalized
    elif direction == 'to_space':
        # Replace underscores with spaces
        normalized = field_name.replace('_', ' ').replace('-', ' ')
        # Remove multiple consecutive spaces
        while '  ' in normalized:
            normalized = normalized.replace('  ', ' ')
        # Remove leading/trailing spaces
        normalized = normalized.strip()
        return normalized
    else:
        raise ValueError(f"Invalid direction: {direction}. Must be 'to_underscore' or 'to_space'")


def to_cli_name(field_name: str) -> str:
    """Convert field name to CLI argument format.
    
    For field names with spaces, replaces spaces with underscores and keeps underscores.
    For field names without spaces, converts underscores to dashes (standard CLI format).
    
    Examples:
        "logical used" -> "--logical_used"
        "logical_used" -> "--logical-used"
        "cluster_name" -> "--cluster-name"
    
    Args:
        field_name: Field name to convert
        
    Returns:
        CLI argument name (without -- prefix)
    """
    if ' ' in field_name:
        # Field name has spaces: "logical used" -> "logical_used"
        return field_name.replace(' ', '_')
    else:
        # Field name has no spaces: "cluster_name" -> "cluster-name"
        return field_name.replace('_', '-')


def to_raw_field_name(field_name: str) -> str:
    """Convert field name to _raw_ prefixed field name for sorting.
    
    Normalizes the field name to underscores and adds _raw_ prefix.
    
    Examples:
        "logical used" -> "_raw_logical_used"
        "logical_used" -> "_raw_logical_used"
    
    Args:
        field_name: Field name to convert
        
    Returns:
        Raw field name with _raw_ prefix
    """
    normalized = normalize_field_name(field_name, 'to_underscore')
    return f'_raw_{normalized}'


def to_python_name(field_name: str) -> str:
    """Convert field name to Python variable name format.
    
    Normalizes to underscores (replaces spaces and hyphens).
    
    Examples:
        "logical used" -> "logical_used"
        "cluster-name" -> "cluster_name"
        "logical_used" -> "logical_used"
    
    Args:
        field_name: Field name to convert
        
    Returns:
        Python variable name
    """
    return normalize_field_name(field_name, 'to_underscore')


def convert_docker_path_to_host(container_path: str) -> str:
    """Convert Docker container path to host path for file access.
    
    When running in Docker, files are stored in container paths like /root/.vast-admin-mcp/,
    but users need host paths like ~/.vast-admin-mcp/ to access them.
    
    Args:
        container_path: File path from inside the container
        
    Returns:
        Host path that users can access, or original path if not in Docker
        
    Examples:
        In Docker: "/root/.vast-admin-mcp/temp_graphs/file.png" -> "~/.vast-admin-mcp/temp_graphs/file.png"
        Not in Docker: "/root/.vast-admin-mcp/temp_graphs/file.png" -> unchanged
    """
    # Check if we're running in Docker
    if not os.environ.get('DOCKER_CONTAINER'):
        return container_path
    
    # Get host config directory from environment (set by docker-compose or docker run)
    host_config_dir = os.environ.get('HOST_CONFIG_DIR', '~/.vast-admin-mcp')
    
    # Container config directory is always /root/.vast-admin-mcp
    container_config_dir = '/root/.vast-admin-mcp'
    
    # If the path starts with the container config directory, replace it with host path
    if container_path.startswith(container_config_dir):
        # Replace container path with host path
        relative_path = container_path[len(container_config_dir):].lstrip('/')
        host_path = os.path.join(os.path.expanduser(host_config_dir), relative_path)
        logging.debug(f"Converted Docker path: {container_path} -> {host_path}")
        return host_path
    
    return container_path


def handle_errors(debug: bool = False, command_name: str = "command"):
    """Decorator to handle errors consistently across command handlers.
    
    Replaces the repeated pattern of:
    ```python
    try:
        # operation
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    ```
    
    Args:
        debug: Whether to show full traceback on error
        command_name: Name of the command for error messages
        
    Usage:
        @handle_errors(debug=args.debug, command_name="list")
        def handle_list_command(args):
            # command logic
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(f"Error executing {command_name} command: {e}", file=sys.stderr)
                if debug:
                    import traceback
                    traceback.print_exc()
                sys.exit(1)
        return wrapper
    return decorator


from .cache import get_cache_manager

_cache_manager = get_cache_manager()


def is_vast_version_legacy(version_string: str, threshold: tuple = (5, 3)) -> bool:
    """Check if VAST version is legacy (< 5.3 by default).
    
    Args:
        version_string: Version string like "5.2.0" or "5.2.0-123"
        threshold: Version threshold as tuple (major, minor). Default is (5, 3)
        
    Returns:
        True if version < threshold, False otherwise
        
    Examples:
        is_vast_version_legacy("5.2.0")  # True (< 5.3)
        is_vast_version_legacy("5.3.0")  # False (>= 5.3)
        is_vast_version_legacy("5.1.0")  # True (< 5.3)
        is_vast_version_legacy("6.0.0")  # False (>= 5.3)
        is_vast_version_legacy("")       # False (unknown version, assume modern)
    """
    if not version_string:
        # If version is unknown, assume modern version (safer default)
        return False
    
    try:
        # Use parse_vast_version from setup module
        # Import here to avoid circular dependency
        from .setup import parse_vast_version
        
        version_tuple = parse_vast_version(version_string)
        
        # If parsing failed (returns (0, 0)), assume modern
        if version_tuple == (0, 0):
            return False
        
        # Compare with threshold
        return version_tuple < threshold
    except Exception as e:
        logging.debug(f"Failed to check if version is legacy: {e}")
        # On error, assume modern version (safer default)
        return False


def get_api_whitelist() -> Dict[str, List[str]]:
    """Get API whitelist from template parser (cached).
    
    This function is shared across modules to avoid duplication.
    The whitelist is cached in memory to avoid redundant parsing.
    
    Returns:
        Dictionary mapping API endpoint names to lists of allowed field names.
        Empty dict means deny all (default if loading fails).
    """
    def _load_whitelist():
        try:
            from .template_parser import TemplateParser
            default_template_path = get_default_template_path()
            parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
            return parser.get_api_whitelist()
        except Exception as e:
            logging.warning(f"Could not load API whitelist: {e}, defaulting to empty (deny all)")
            return {}
    
    return _cache_manager.get_or_set('whitelist', 'api_whitelist', _load_whitelist)

