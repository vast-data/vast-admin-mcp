"""Setup functionality for cluster inventory management."""

import os
import json
import logging
from typing import Optional, Dict, Any
from urllib.parse import urlparse

from vastpy import VASTClient

from .config import CONFIG_FILE
from .utils import (
    query_yes_no, retrieve_password_secure, store_password_secure,
    logging_main
)
from .client import create_vast_client


def parse_cluster_address(address: str) -> str:
    """Parse cluster address, supporting both URL format (https://host:port) and plain hostname:port format.
    
    Args:
        address: Cluster address in URL format (https://host:port) or plain format (host:port or host)
        
    Returns:
        Parsed address in format host:port (or just host if no port specified)
    """
    # If it looks like a URL (starts with http:// or https://), parse it
    if address.startswith('http://') or address.startswith('https://'):
        parsed = urlparse(address)
        hostname = parsed.hostname
        port = parsed.port
        
        if not hostname:
            # Fallback: try to extract from netloc if hostname is None
            netloc = parsed.netloc
            if ':' in netloc:
                hostname, port_str = netloc.rsplit(':', 1)
                try:
                    port = int(port_str)
                except ValueError:
                    port = None
            else:
                hostname = netloc
                port = None
        
        if hostname:
            if port:
                return f"{hostname}:{port}"
            else:
                return hostname
        else:
            # If parsing failed, return original (might be valid hostname:port)
            return address
    
    # Not a URL, return as-is (could be hostname:port or just hostname)
    return address


def parse_vast_version(build_string: str) -> tuple:
    """Parse VAST version from build string or sw_version.
    
    Args:
        build_string: Version string like "5.2.0-123", "5.2.0", "5.2.3.45", or "vrelease-5-2-3-2169905"
        
    Returns:
        Tuple of (major, minor) version numbers, or (0, 0) if parsing fails
        
    Examples:
        "5.2.0-123" -> (5, 2)
        "5.2.0" -> (5, 2)
        "5.2.3.45" -> (5, 2)
        "5.2" -> (5, 2)
        "vrelease-5-2-3-2169905" -> (5, 2)
        "invalid" -> (0, 0)
    """
    if not build_string:
        return (0, 0)
    
    try:
        # Handle "vrelease-5-2-3-2169905" format (build format)
        if build_string.startswith('vrelease-') or build_string.startswith('v-'):
            # Strip "vrelease-" or "v-" prefix
            if build_string.startswith('vrelease-'):
                version_part = build_string[9:]  # Remove "vrelease-"
            else:
                version_part = build_string[2:]  # Remove "v-"
            
            # Version numbers are separated by hyphens: "5-2-3-2169905"
            parts = version_part.split('-')
            if len(parts) >= 2:
                major = int(parts[0])
                minor = int(parts[1])
                return (major, minor)
        
        # Handle standard dot-separated formats: "5.2.0-123", "5.2.0", "5.2.3.45"
        # Remove any build suffix after hyphen (e.g., "5.2.0-123" -> "5.2.0")
        version_part = build_string.split('-')[0]
        
        # Split by dots and get major.minor
        parts = version_part.split('.')
        if len(parts) >= 2:
            major = int(parts[0])
            minor = int(parts[1])
            return (major, minor)
        elif len(parts) == 1:
            # Only major version
            major = int(parts[0])
            return (major, 0)
        else:
            return (0, 0)
    except (ValueError, AttributeError, IndexError):
        logging.debug(f"Failed to parse version from build string: {build_string}")
        return (0, 0)


def validate_cluster(cluster: str, tenant: str, username: str, password: str, user_type: str = None) -> Dict[str, Any]:
    """Validate cluster connectivity and return cluster info."""
    try:
        # Parse cluster address to handle URL format
        cluster_address = parse_cluster_address(cluster)
        
        # For SUPER_ADMIN users, don't pass tenant parameter (even if tenant is set in config)
        # For tenant admins, always pass the tenant parameter
        if user_type == 'SUPER_ADMIN':
            client = VASTClient(address=cluster_address, user=username, password=password)
        elif tenant != '':
            client = VASTClient(address=cluster_address, user=username, password=password, tenant=tenant)
        else:
            client = VASTClient(address=cluster_address, user=username, password=password)
        status = client.login.get()
        
        # Get cluster info from clusters endpoint (same as list_clusters function)
        # This provides sw_version which is the proper version field
        cluster_name = None
        sw_version = None
        build_number = None
        
        try:
            clusters_response = client.clusters.get(page_size=1)
            if clusters_response and 'results' in clusters_response and len(clusters_response['results']) > 0:
                cluster_data = clusters_response['results'][0]
                cluster_name = cluster_data.get('name')
                sw_version = cluster_data.get('sw_version')
        except Exception as e:
            logging.debug(f"Could not get cluster info from clusters endpoint: {e}")
        
        # Fallback to dashboard.status if clusters endpoint failed
        if not sw_version:
            try:
                cluster_info = client.dashboard.status.get()['clusters'][0]
                build_number = cluster_info.get('build', '')
                if not cluster_name:
                    cluster_name = cluster_info.get('name')
                # Use build as version fallback
                sw_version = build_number
            except Exception as e:
                logging.debug(f"Could not get cluster info from dashboard.status: {e}")
                sw_version = ''
        
        # Format version like list_clusters does: take first 4 parts if dot-separated
        if isinstance(sw_version, str) and "." in sw_version and len(sw_version.split(".")) >= 4:
            vast_version = ".".join(sw_version.split(".")[:4])
        else:
            vast_version = sw_version
        
        # Parse version to determine if legacy (< 5.3)
        version_tuple = parse_vast_version(vast_version)
        
        # Check if this is a legacy version (< 5.3)
        from .utils import is_vast_version_legacy
        is_legacy = is_vast_version_legacy(vast_version)
        
        # Get user_type from status (might not be present in versions < 5.3)
        user_type_from_api = status.get('user_type')
        
        if is_legacy:
            # For legacy versions (< 5.3), force SUPER_ADMIN and ignore tenant
            user_type_from_api = 'SUPER_ADMIN'
            tenant = ''  # Clear tenant for legacy versions
            logging.info(f"Successfully connected to VAST cluster at {cluster_address}. "
                        f"Legacy version {vast_version} detected - treating as SUPER_ADMIN")
        else:
            # For modern versions, use API user_type or default to TENANT_ADMIN
            if not user_type_from_api:
                user_type_from_api = 'TENANT_ADMIN'
            logging.info(f"Successfully connected to VAST cluster at {cluster_address}. "
                        f"Version: {vast_version}, User role: {user_type_from_api}")

        # Store password securely (use parsed address for storage)
        secure_password_ref = store_password_secure(cluster_address, username, password)
        result = {
            "cluster": cluster_address,  # Store parsed address (hostname:port format)
            "username": username,
            "password": secure_password_ref,  # Now stores secure reference instead of base64
            "tenant": tenant,
            "user_type": user_type_from_api,
            "vast_version": vast_version,  # Store version permanently in config
        }
        
        # Add cluster_name if we got it from the API
        if cluster_name:
            result["cluster_name"] = cluster_name
            logging.debug(f"Retrieved cluster name: {cluster_name}")
        
        # Store build/version separately for display only (not saved to config)
        result['_build'] = build_number if build_number else vast_version
        return result
    except Exception as e:
        logging.error(f"Failed to connect to VAST cluster at {cluster}. Error: {e}")
        return {}


def setup_config(config_file: str = CONFIG_FILE):
    """
    Enhanced setup function that supports adding, editing, and removing clusters.
    Provides a menu-driven interface for managing cluster configurations.
    """
    if not os.path.exists(os.path.dirname(config_file)):
        os.makedirs(os.path.dirname(config_file))
    
    # Load existing config or create new one
    config = {}
    if os.path.isfile(config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                logging.info(f"Loaded existing configuration with {len(config.get('clusters', []))} clusters")
        except Exception as e:
            logging.error(f"Error loading existing config: {e}")
            config = {}
    
    if 'clusters' not in config:
        config['clusters'] = []
    
    while True:
        print("\n" + "="*60)
        print("VAST Admin MCP - Configuration Management")
        print("="*60)
        
        # Display current clusters
        if config['clusters']:
            print(f"\nCurrent configured clusters ({len(config['clusters'])}):")
            print("-" * 50)
            for i, cluster in enumerate(config['clusters'], 1):
                version_info = f" (v{cluster['vast_version']})" if cluster.get('vast_version') else ""
                print(f"  {i}. {cluster['cluster']} - {cluster.get('user_type', 'N/A')}{version_info} - Tenant: {cluster.get('tenant', 'N/A')}")
        else:
            print("\nNo clusters currently configured.")
        
        # Display HTTP server status
        print(f"\nHTTP Server: {get_http_server_status(config)}")
        
        print("\nAvailable options:")
        print("  1. Add new cluster")
        if config['clusters']:
            print("  2. Edit existing cluster")
            print("  3. Remove cluster")
            print("  4. Test cluster connectivity")
        print("  5. Configure HTTP server settings")
        print("  6. Manage authentication token")
        print("  9. Save and exit")
        print("  0. Exit without saving")
        
        try:
            choice = input("\nSelect option [1-9, 0]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled.")
            return config
        
        if choice == '1':
            # Add new cluster
            new_cluster = _add_new_cluster()
            if new_cluster:
                # Remove internal fields (like _build) before saving
                cluster_to_save = {k: v for k, v in new_cluster.items() if not k.startswith('_')}
                # Check if cluster already exists
                existing = [c for c in config['clusters'] if c['cluster'] == cluster_to_save['cluster']]
                if existing:
                    update = query_yes_no(f"Cluster {cluster_to_save['cluster']} already exists. Update it?", default="yes")
                    if update:
                        # Remove existing and add new
                        config['clusters'] = [c for c in config['clusters'] if c['cluster'] != cluster_to_save['cluster']]
                        config['clusters'].append(cluster_to_save)
                        logging.info(f"Updated cluster {cluster_to_save['cluster']}")
                    else:
                        logging.info("Cluster addition cancelled.")
                else:
                    config['clusters'].append(cluster_to_save)
                    logging.info(f"Added new cluster {cluster_to_save['cluster']}")
        
        elif choice == '2' and config['clusters']:
            # Edit existing cluster
            cluster_to_edit = _select_cluster(config['clusters'], "Select cluster to edit")
            if cluster_to_edit:
                updated_cluster = _edit_cluster(cluster_to_edit)
                if updated_cluster:
                    # Remove internal fields (like _build) before saving
                    cluster_to_save = {k: v for k, v in updated_cluster.items() if not k.startswith('_')}
                    # Replace the cluster in the list
                    for i, cluster in enumerate(config['clusters']):
                        if cluster['cluster'] == cluster_to_edit['cluster']:
                            config['clusters'][i] = cluster_to_save
                            break
                    logging.info(f"Updated cluster {cluster_to_save['cluster']}")
        
        elif choice == '3' and config['clusters']:
            # Remove cluster
            cluster_to_remove = _select_cluster(config['clusters'], "Select cluster to remove")
            if cluster_to_remove:
                confirm = query_yes_no(f"Are you sure you want to remove cluster {cluster_to_remove['cluster']}?", default="no")
                if confirm:
                    config['clusters'] = [c for c in config['clusters'] if c['cluster'] != cluster_to_remove['cluster']]
                    logging.info(f"Removed cluster {cluster_to_remove['cluster']}")
        
        elif choice == '4' and config['clusters']:
            # Test connectivity
            cluster_to_test = _select_cluster(config['clusters'], "Select cluster to test")
            if cluster_to_test:
                _test_cluster_connectivity(cluster_to_test)
        
        elif choice == '5':
            # Configure HTTP server
            config = _configure_http_server(config)
        
        elif choice == '6':
            # Manage authentication token
            config = _manage_auth_token(config)
        
        elif choice == '9':
            # Save and exit
            if config['clusters'] or config.get('http_server', {}).get('enabled'):
                try:
                    with open(config_file, 'w') as f:
                        json.dump(config, f, indent=2)
                    logging.info(f"Configuration saved to: {config_file}")
                    return config
                except Exception as e:
                    logging.error(f"Error saving configuration: {e}")
                    continue
            else:
                logging.error("Cannot save empty configuration. Please add at least one cluster or configure HTTP server.")
        
        elif choice == '0':
            # Exit without saving
            if config['clusters']:
                confirm = query_yes_no("Exit without saving changes?", default="no")
                if confirm:
                    logging.info("Exited without saving changes.")
                    return config
            else:
                logging.info("Exited without saving.")
                return config
        
        else:
            print("Invalid option. Please try again.")


def _add_new_cluster():
    """Add a new cluster configuration"""
    print("\n" + "-" * 40)
    print("Adding new cluster")
    print("-" * 40)
    
    try:
        cluster = input("VAST cluster address (IP, FQDN, or URL like https://host:port): ").strip()
        if not cluster:
            logging.error("Cluster address is required.")
            return None
        
        # Parse the address to extract hostname:port (handles URL format)
        cluster_address = parse_cluster_address(cluster)
        logging.debug(f"Parsed cluster address: {cluster} -> {cluster_address}")
        
        tenant = input("For TENANT ADMINS only, enter tenant name [leave empty for super admin]: ").strip()
        
        # Get credentials
        if os.environ.get('VMS_USER'):
            username = os.environ.get('VMS_USER')    
            logging.info("Using VAST API username from environment variable VMS_USER")
        else:
            username = input("VAST API username: ").strip()
            if not username:
                logging.error("Username is required.")
                return None

        if os.environ.get('VMS_PASSWORD'):
            password = os.environ.get('VMS_PASSWORD')
            logging.info("Using VAST API password from environment variable VMS_PASSWORD")
        else:
            from getpass import getpass
            password = getpass("VAST password: ")
            if not password:
                logging.error("Password is required.")
                return None

        # Validate cluster connectivity (use parsed address)
        logging.info(f"Testing connectivity to VAST cluster: {cluster_address}")
        cluster_info = validate_cluster(cluster=cluster, tenant=tenant, username=username, password=password)
        
        if not cluster_info:
            logging.error(f"Could not connect to cluster: {cluster} with the provided credentials.")
            return None
        
        # For super admin users, ask for tenant
        if cluster_info.get('user_type') == 'SUPER_ADMIN':
            tenant = input(f"Cluster admin user detected. Which tenant do you want to use [default]: ").strip() or 'default'
            cluster_info['tenant'] = tenant
        
        logging.info(f"Successfully validated cluster {cluster}")
        return cluster_info
        
    except (EOFError, KeyboardInterrupt):
        print("\nOperation cancelled.")
        return None
    except Exception as e:
        logging.error(f"Error adding cluster: {e}")
        return None


def _edit_cluster(cluster_info):
    """Edit an existing cluster configuration"""
    print(f"\n" + "-" * 40)
    print(f"Editing cluster: {cluster_info['cluster']}")
    print("-" * 40)
    
    try:
        print(f"Current cluster: {cluster_info['cluster']}")
        print(f"Current tenant: {cluster_info.get('tenant', 'N/A')}")
        print(f"Current user: {cluster_info['username']}")
        version_info = f" (v{cluster_info['vast_version']})" if cluster_info.get('vast_version') else ""
        print(f"User type: {cluster_info.get('user_type', 'N/A')}{version_info}")
        
        print("\nWhat would you like to update?")
        print("  1. Username/Password")
        print("  2. Tenant (super admin only)")
        print("  3. Test connectivity (no changes)")
        print("  0. Cancel")
        
        choice = input("Select option [1-3, 0]: ").strip()
        
        if choice == '0':
            return None
        elif choice == '1':
            # Update credentials
            new_username = input(f"Username [{cluster_info['username']}]: ").strip()
            if not new_username:
                new_username = cluster_info['username']
            
            from getpass import getpass
            new_password = getpass("New password (leave empty to keep current): ")
            if not new_password:
                # Keep current password - retrieve it securely
                new_password = retrieve_password_secure(cluster_info['cluster'], cluster_info['username'], cluster_info['password'])
            
            # Test new credentials
            logging.info("Testing new credentials...")
            updated_info = validate_cluster(
                cluster=cluster_info['cluster'], 
                tenant=cluster_info.get('tenant', ''), 
                username=new_username, 
                password=new_password,
                user_type=cluster_info.get('user_type')
            )
            
            if not updated_info:
                logging.error("Failed to validate new credentials.")
                return None
                
            logging.info("Credentials validated successfully.")
            return updated_info
            
        elif choice == '2':
            # Update tenant (super admin only)
            if cluster_info.get('user_type') != 'SUPER_ADMIN':
                logging.error("Tenant can only be changed for super admin users.")
                return None
            
            current_tenant = cluster_info.get('tenant', 'default')
            new_tenant = input(f"Tenant [{current_tenant}]: ").strip()
            if not new_tenant:
                new_tenant = current_tenant
            
            # Create updated cluster info
            updated_info = cluster_info.copy()
            updated_info['tenant'] = new_tenant
            
            logging.info(f"Updated tenant to: {new_tenant}")
            return updated_info
            
        elif choice == '3':
            # Test connectivity
            _test_cluster_connectivity(cluster_info)
            return None
        
        else:
            print("Invalid option.")
            return None
            
    except (EOFError, KeyboardInterrupt):
        print("\nOperation cancelled.")
        return None
    except Exception as e:
        logging.error(f"Error editing cluster: {e}")
        return None


def _select_cluster(clusters, prompt):
    """Select a cluster from the list"""
    if not clusters:
        return None
    
    print(f"\n{prompt}:")
    for i, cluster in enumerate(clusters, 1):
        version_info = f" (v{cluster['vast_version']})" if cluster.get('vast_version') else ""
        print(f"  {i}. {cluster['cluster']} - {cluster.get('user_type', 'N/A')}{version_info} - Tenant: {cluster.get('tenant', 'N/A')}")
    
    try:
        choice = input(f"Select cluster [1-{len(clusters)}, 0 to cancel]: ").strip()
        if choice == '0':
            return None
        
        index = int(choice) - 1
        if 0 <= index < len(clusters):
            return clusters[index]
        else:
            print("Invalid selection.")
            return None
    except (ValueError, EOFError, KeyboardInterrupt):
        return None


def _test_cluster_connectivity(cluster_info):
    """Test connectivity to a cluster"""
    try:
        print(f"\nTesting connectivity to {cluster_info['cluster']}...")
        password = retrieve_password_secure(cluster_info['cluster'], cluster_info['username'], cluster_info['password'])
        
        result = validate_cluster(
            cluster=cluster_info['cluster'],
            tenant=cluster_info.get('tenant', ''),
            username=cluster_info['username'],
            password=password,
            user_type=cluster_info.get('user_type')
        )
        
        if result:
            print(f"✅ Successfully connected to {cluster_info['cluster']}")
            version_info = f" (v{result['vast_version']})" if result.get('vast_version') else ""
            print(f"   User: {result['username']} ({result.get('user_type', 'N/A')}{version_info})")
            if '_build' in result:
                print(f"   Build: {result['_build']}")
        else:
            print(f"❌ Failed to connect to {cluster_info['cluster']}")
            
    except Exception as e:
        print(f"❌ Error testing connectivity: {e}")


# ============================================================================
# HTTP Server Configuration Functions
# ============================================================================

import secrets

TOKEN_PREFIX = "vamt_"  # VAST Admin MCP Token


def generate_auth_token(length: int = 32) -> str:
    """Generate a secure random token with prefix."""
    random_part = secrets.token_urlsafe(length)
    return f"{TOKEN_PREFIX}{random_part}"


def get_http_server_status(config: dict) -> str:
    """Get HTTP server configuration status string."""
    http_config = config.get('http_server', {})
    if not http_config.get('enabled', False):
        return "Disabled"
    
    parts = [f"port {http_config.get('port', 8000)}"]
    
    auth_config = http_config.get('auth', {})
    auth_type = auth_config.get('type', 'none')
    if auth_type == 'bearer':
        parts.append("auth: token")
    elif auth_type == 'oauth':
        provider = auth_config.get('provider', 'unknown')
        parts.append(f"auth: {provider}")
    else:
        parts.append("auth: none")
    
    ssl_config = http_config.get('ssl', {})
    if ssl_config.get('enabled', False):
        parts.append("SSL: enabled")
    
    return "Enabled (" + ", ".join(parts) + ")"


def _configure_http_server(config: dict) -> dict:
    """Configure HTTP server settings interactively."""
    print("\n" + "-" * 50)
    print("HTTP Server Configuration")
    print("-" * 50)
    
    http_config = config.get('http_server', {})
    
    # Show current status
    print(f"\nCurrent status: {get_http_server_status(config)}")
    
    # Enable/disable
    current_enabled = http_config.get('enabled', False)
    enable_prompt = "Enable HTTP server?" if not current_enabled else "Keep HTTP server enabled?"
    enable = query_yes_no(enable_prompt, default="yes" if current_enabled else "no")
    
    if not enable:
        http_config['enabled'] = False
        config['http_server'] = http_config
        print("HTTP server disabled.")
        return config
    
    http_config['enabled'] = True
    
    # Host/Port configuration
    current_host = http_config.get('host', '0.0.0.0')
    current_port = http_config.get('port', 8000)
    current_path = http_config.get('path', '/mcp/')
    
    host = input(f"Bind address [{current_host}]: ").strip() or current_host
    port_str = input(f"Port [{current_port}]: ").strip()
    port = int(port_str) if port_str else current_port
    path = input(f"URL path [{current_path}]: ").strip() or current_path
    
    http_config['host'] = host
    http_config['port'] = port
    http_config['path'] = path
    
    # Authentication configuration
    print("\nAuthentication:")
    print("  1. Bearer token (simple shared secret)")
    print("  2. OAuth provider (GitHub, Google, Azure)")
    print("  3. Generic OAuth/OIDC (any provider)")
    print("  4. None (localhost only - not recommended)")
    
    auth_choice = input("Select [1]: ").strip() or "1"
    
    if auth_choice == '1':
        http_config['auth'] = _configure_bearer_auth(http_config.get('auth', {}))
    elif auth_choice == '2':
        http_config['auth'] = _configure_oauth_provider(http_config.get('auth', {}))
    elif auth_choice == '3':
        http_config['auth'] = _configure_generic_oauth(http_config.get('auth', {}))
    else:
        http_config['auth'] = {'type': 'none'}
        print("⚠️  Warning: Running without authentication. Only use on localhost!")
    
    # SSL configuration
    print("\nSSL/TLS:")
    print("  1. No SSL (use reverse proxy for HTTPS)")
    print("  2. Enable SSL with certificate files")
    print("  3. Generate self-signed certificate (development)")
    
    ssl_choice = input("Select [1]: ").strip() or "1"
    
    if ssl_choice == '2':
        http_config['ssl'] = _configure_ssl_files(http_config.get('ssl', {}))
    elif ssl_choice == '3':
        http_config['ssl'] = _generate_ssl_cert()
    else:
        http_config['ssl'] = {'enabled': False}
    
    config['http_server'] = http_config
    print("\n✅ HTTP server configured successfully!")
    return config


def _configure_bearer_auth(current_auth: dict) -> dict:
    """Configure bearer token authentication."""
    print("\nBearer token configuration:")
    print("  1. Generate new token (stored encrypted)")
    print("  2. Enter token manually (stored encrypted)")
    print("  3. Use environment variable only")
    
    choice = input("Select [1]: ").strip() or "1"
    
    if choice == '1':
        token = generate_auth_token()
        print(f"\nGenerated token: {token}")
        
        # Store encrypted
        secure_ref = store_password_secure("http_server", "auth_token", token)
        print("Token stored encrypted in config.")
        print(f"\nTo use with environment variable:")
        print(f'  export VAST_ADMIN_MCP_AUTH_TOKEN="{token}"')
        
        return {
            'type': 'bearer',
            'token': secure_ref
        }
    elif choice == '2':
        from getpass import getpass
        token = getpass("Enter token: ").strip()
        if token:
            secure_ref = store_password_secure("http_server", "auth_token", token)
            print("Token stored encrypted in config.")
            return {
                'type': 'bearer',
                'token': secure_ref
            }
    
    # Use environment variable only
    print("Using environment variable VAST_ADMIN_MCP_AUTH_TOKEN")
    return {
        'type': 'bearer',
        'token': 'env:VAST_ADMIN_MCP_AUTH_TOKEN'
    }


def _configure_oauth_provider(current_auth: dict) -> dict:
    """Configure OAuth provider authentication."""
    print("\nSelect provider:")
    print("  1. GitHub")
    print("  2. Google")
    print("  3. Azure AD")
    
    choice = input("Select [1]: ").strip() or "1"
    
    providers = {'1': 'github', '2': 'google', '3': 'azure'}
    provider = providers.get(choice, 'github')
    
    print(f"\n{provider.title()} OAuth Setup:")
    print("  1. Create an OAuth App in your provider's settings")
    print("  2. Set the callback URL to: https://your-server.com/auth/callback")
    
    client_id = input("\nClient ID: ").strip()
    if not client_id:
        print("Client ID is required.")
        return current_auth
    
    from getpass import getpass
    client_secret = getpass("Client Secret: ").strip()
    if not client_secret:
        print("Client Secret is required.")
        return current_auth
    
    base_url = input("Base URL (your server's public URL): ").strip()
    if not base_url:
        print("Base URL is required.")
        return current_auth
    
    # Store client secret encrypted
    secret_ref = store_password_secure("http_server", "oauth_client_secret", client_secret)
    
    auth_config = {
        'type': 'oauth',
        'provider': provider,
        'client_id': client_id,
        'client_secret': secret_ref,
        'base_url': base_url
    }
    
    if provider == 'azure':
        tenant_id = input("Tenant ID: ").strip()
        if tenant_id:
            auth_config['tenant_id'] = tenant_id
    
    print(f"\n✅ {provider.title()} OAuth configured!")
    return auth_config


def _configure_generic_oauth(current_auth: dict) -> dict:
    """Configure generic OAuth/OIDC provider."""
    print("\nGeneric OAuth/OIDC Configuration")
    
    use_oidc = query_yes_no("Does your provider support OIDC discovery?", default="yes")
    
    if use_oidc:
        oidc_issuer = input("OIDC Issuer URL (e.g., https://your-domain.okta.com): ").strip()
        if not oidc_issuer:
            print("OIDC Issuer URL is required.")
            return current_auth
        
        print("\nAuto-discovering endpoints...")
    else:
        oidc_issuer = None
        print("\nManual OAuth endpoint configuration:")
    
    client_id = input("Client ID: ").strip()
    if not client_id:
        print("Client ID is required.")
        return current_auth
    
    from getpass import getpass
    client_secret = getpass("Client Secret: ").strip()
    if not client_secret:
        print("Client Secret is required.")
        return current_auth
    
    base_url = input("Base URL (your server's public URL): ").strip()
    if not base_url:
        print("Base URL is required.")
        return current_auth
    
    # Store client secret encrypted
    secret_ref = store_password_secure("http_server", "oauth_client_secret", client_secret)
    
    auth_config = {
        'type': 'oauth',
        'provider': 'generic',
        'client_id': client_id,
        'client_secret': secret_ref,
        'base_url': base_url
    }
    
    if oidc_issuer:
        auth_config['oidc_issuer'] = oidc_issuer
    else:
        # Manual endpoint configuration
        auth_url = input("Authorization URL: ").strip()
        token_url = input("Token URL: ").strip()
        jwks_url = input("JWKS URL: ").strip()
        issuer = input("Issuer: ").strip()
        audience = input("Audience (optional): ").strip()
        
        auth_config['authorization_url'] = auth_url
        auth_config['token_url'] = token_url
        auth_config['jwks_url'] = jwks_url
        auth_config['issuer'] = issuer
        if audience:
            auth_config['audience'] = audience
    
    print(f"\nCallback URL to register with your provider:")
    print(f"  {base_url}/auth/callback")
    print("\n✅ Generic OAuth configured!")
    return auth_config


def _configure_ssl_files(current_ssl: dict) -> dict:
    """Configure SSL with existing certificate files."""
    cert_file = input("Certificate file path: ").strip()
    key_file = input("Private key file path: ").strip()
    
    if not cert_file or not key_file:
        print("Both certificate and key files are required.")
        return {'enabled': False}
    
    if not os.path.exists(cert_file):
        print(f"⚠️  Warning: Certificate file not found: {cert_file}")
    if not os.path.exists(key_file):
        print(f"⚠️  Warning: Key file not found: {key_file}")
    
    return {
        'enabled': True,
        'cert_file': cert_file,
        'key_file': key_file
    }


def _generate_ssl_cert() -> dict:
    """Generate self-signed SSL certificate."""
    import datetime
    import ipaddress
    
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError:
        print("❌ Error: cryptography library is required for certificate generation.")
        return {'enabled': False}
    
    output_dir = os.path.expanduser('~/.vast-admin-mcp/ssl')
    cn = input("Common Name (CN) [vast-admin-mcp.local]: ").strip() or "vast-admin-mcp.local"
    days_str = input("Valid for days [365]: ").strip()
    days = int(days_str) if days_str else 365
    
    print(f"\nGenerating self-signed certificate...")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate private key
    key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
    
    # Build Subject Alternative Names
    san_entries = [
        x509.DNSName(cn),
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
    ]
    
    # Generate certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, cn),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "VAST Admin MCP"),
    ])
    
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.utcnow())
        .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=days))
        .add_extension(x509.SubjectAlternativeName(san_entries), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    
    # Write files
    cert_path = os.path.join(output_dir, "cert.pem")
    key_path = os.path.join(output_dir, "key.pem")
    
    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()
        ))
    os.chmod(key_path, 0o600)
    
    print(f"  Cert: {cert_path}")
    print(f"  Key:  {key_path}")
    
    return {
        'enabled': True,
        'cert_file': cert_path,
        'key_file': key_path
    }


def _manage_auth_token(config: dict) -> dict:
    """Manage authentication token submenu."""
    while True:
        http_config = config.get('http_server', {})
        auth_config = http_config.get('auth', {})
        
        # Try to get current token
        token = None
        token_ref = auth_config.get('token', '')
        
        if token_ref.startswith('env:'):
            token_status = "from environment variable"
        elif token_ref:
            try:
                token = retrieve_password_secure("http_server", "auth_token", token_ref)
                token_status = f"{token[:12]}...{token[-4:]}" if len(token) > 20 else token
            except Exception:
                token_status = "configured (encrypted)"
        else:
            token_status = "not configured"
        
        print("\n" + "-" * 40)
        print("Authentication Token Management")
        print("-" * 40)
        print(f"Current token: {token_status}")
        print("\nOptions:")
        print("  1. Show current token")
        print("  2. Generate new token (rotate)")
        print("  3. Enter token manually")
        print("  4. Export as environment variable")
        print("  5. Remove token (disable auth)")
        print("  0. Back to main menu")
        
        choice = input("\nSelect [0]: ").strip() or "0"
        
        if choice == "0":
            return config
        elif choice == "1":
            if token_ref.startswith('env:'):
                env_var = token_ref[4:]
                env_token = os.environ.get(env_var, '')
                if env_token:
                    print(f"\nToken from {env_var}: {env_token}")
                else:
                    print(f"\nEnvironment variable {env_var} is not set")
            elif token:
                print(f"\nCurrent token: {token}")
            else:
                print("\nNo token configured")
        elif choice == "2":
            new_token = generate_auth_token()
            secure_ref = store_password_secure("http_server", "auth_token", new_token)
            if 'http_server' not in config:
                config['http_server'] = {}
            if 'auth' not in config['http_server']:
                config['http_server']['auth'] = {'type': 'bearer'}
            config['http_server']['auth']['token'] = secure_ref
            print(f"\nGenerated token: {new_token}")
            print("Token saved (encrypted)")
        elif choice == "3":
            from getpass import getpass
            manual_token = getpass("Enter token: ").strip()
            if manual_token:
                secure_ref = store_password_secure("http_server", "auth_token", manual_token)
                if 'http_server' not in config:
                    config['http_server'] = {}
                if 'auth' not in config['http_server']:
                    config['http_server']['auth'] = {'type': 'bearer'}
                config['http_server']['auth']['token'] = secure_ref
                print("Token saved (encrypted)")
        elif choice == "4":
            if token:
                print(f'\nexport VAST_ADMIN_MCP_AUTH_TOKEN="{token}"')
            elif token_ref.startswith('env:'):
                env_var = token_ref[4:]
                env_token = os.environ.get(env_var, '')
                if env_token:
                    print(f'\nexport VAST_ADMIN_MCP_AUTH_TOKEN="{env_token}"')
                else:
                    print(f"\nEnvironment variable {env_var} is not set")
            else:
                print("\nNo token configured")
        elif choice == "5":
            if 'http_server' in config and 'auth' in config['http_server']:
                if 'token' in config['http_server']['auth']:
                    del config['http_server']['auth']['token']
                    print("Token removed")
    
    return config

