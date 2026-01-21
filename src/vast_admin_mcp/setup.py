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
        print("VAST Admin MCP - Cluster configurations Management")
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
        
        print("\nAvailable options:")
        print("  1. Add new cluster")
        if config['clusters']:
            print("  2. Edit existing cluster")
            print("  3. Remove cluster")
            print("  4. Test cluster connectivity")
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
        
        elif choice == '9':
            # Save and exit
            if config['clusters']:
                try:
                    with open(config_file, 'w') as f:
                        json.dump(config, f, indent=2)
                    logging.info(f"Configuration saved to: {config_file}")
                    return config
                except Exception as e:
                    logging.error(f"Error saving configuration: {e}")
                    continue
            else:
                logging.error("Cannot save empty configuration. Please add at least one cluster.")
        
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

