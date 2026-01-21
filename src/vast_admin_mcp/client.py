"""VAST client creation and utilities."""

import time
import functools
import logging
from typing import Dict, Any, Optional, List
import urllib3

from vastpy import VASTClient

from .config import load_config, REST_PAGE_SIZE, API_CONNECT_TIMEOUT, API_READ_TIMEOUT, API_MAX_RETRIES

# Monkey-patch VASTClient.request() to add timeout and retry configuration
# VASTClient creates a new PoolManager for each request, so we patch the request method
# to pass our timeout and retry settings to the PoolManager
_original_vast_client_request = None

def _patch_vast_client_request():
    """Patch VASTClient.request() to add timeout and retry configuration."""
    global _original_vast_client_request
    
    if _original_vast_client_request is not None:
        return  # Already patched
    
    _original_vast_client_request = VASTClient.request
    
    def patched_request(self, method, fields=None, data=None):
        """Patched request method that adds timeout and retry configuration."""
        # Create retry configuration
        retry_config = urllib3.util.retry.Retry(
            total=API_MAX_RETRIES,
            connect=API_MAX_RETRIES,
            read=API_MAX_RETRIES,
            redirect=API_MAX_RETRIES,
            status=API_MAX_RETRIES
        )
        
        # Create timeout configuration
        timeout_config = urllib3.util.timeout.Timeout(
            connect=API_CONNECT_TIMEOUT,
            read=API_READ_TIMEOUT
        )
        
        # Create PoolManager with our configuration
        if self._cert_file:
            pm = urllib3.PoolManager(
                ca_certs=self._cert_file,
                server_hostname=self._cert_server_name,
                retries=retry_config,
                timeout=timeout_config
            )
        else:
            pm = urllib3.PoolManager(
                cert_reqs='CERT_NONE',
                retries=retry_config,
                timeout=timeout_config
            )
            urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)
        
        # Rest of the request logic (copied from VASTClient.request)
        if self._token:
            headers = {'authorization': f"'Api-Token {self._token}"}
        else:
            headers = urllib3.make_headers(basic_auth=self._user + ':' + self._password)
        if self._tenant:
            headers['X-Tenant-Name'] = self._tenant
        if data:
            headers['Content-Type'] = 'application/json'
            import json
            data = json.dumps(data).encode('utf-8')
        if fields:
            result = []
            for k, v in fields.items():
                if isinstance(v, list):
                    result.extend((k, i) for i in v)
                else:
                    result.append((k, v))
            fields = result
        version_path = f'/{self._version}' if self._version else ''
        r = pm.request(method, f'https://{self._address}/{self._url}{version_path}/', headers=headers, fields=fields, body=data)
        
        # Check status codes (from VASTClient)
        import http
        SUCCESS_CODES = {http.HTTPStatus.OK,
                         http.HTTPStatus.CREATED,
                         http.HTTPStatus.ACCEPTED,
                         http.HTTPStatus.NON_AUTHORITATIVE_INFORMATION,
                         http.HTTPStatus.NO_CONTENT,
                         http.HTTPStatus.RESET_CONTENT,
                         http.HTTPStatus.PARTIAL_CONTENT}
        
        if r.status not in SUCCESS_CODES:
            from vastpy import RESTFailure
            raise RESTFailure(method, self._url, fields, r.status, r.data)
        data = r.data
        if 'application/json' in r.headers.get('Content-Type', '') and data:
            import json
            return json.loads(data.decode('utf-8'))
        return data
    
    VASTClient.request = patched_request

# Apply the patch when this module is imported
_patch_vast_client_request()
from .utils import retrieve_password_secure
from .cache import get_cache_manager

# Endpoints that do not support pagination (return single dict or non-paginated list)
NON_PAGINATED_ENDPOINTS = ['monitors.ad_hoc_query']
# Monitor query endpoints follow pattern monitors.{id}.query - handle dynamically

# Valid VAST API object types for security validation
VALID_OBJECT_TYPES = {
    'views', 'tenants', 'snapshots', 'volumes', 'quotas', 'vippools',
    'clusters', 'cnodes', 'host', 'monitoredusers', 'policies', 'qospolicies'
}

# Wrapper to log VAST API calls
def vast_api_wrapper(func):
    """Wrapper function to log VAST API calls with timing."""
    # Check if function is already wrapped to avoid double-wrapping
    if hasattr(func, '_vast_wrapped'):
        return func
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start
            logging.debug(f"{args} succeeded in {elapsed:.2f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start
            logging.error(f"✗ {func.__qualname__} failed in {elapsed:.2f}s: {e!r}")
            raise
    
    # Mark as wrapped to prevent double-wrapping
    wrapper._vast_wrapped = True
    return wrapper

# Cache for cluster name-to-address mappings to avoid redundant API calls
_cluster_name_to_address_cache = {}
_cluster_address_to_name_cache = {}

_cache_manager = get_cache_manager()


def resolve_cluster_identifier(identifier: str, config: dict, client_cache: Optional[Dict[str, Any]] = None) -> tuple[str, dict, Optional[str]]:
    """Resolve cluster identifier (name or address) to address, config, and name.
    
    This function handles the common pattern of resolving a cluster identifier
    (which could be either a cluster name or address) to the actual cluster
    address and configuration. It uses caching to avoid redundant API calls.
    
    Args:
        identifier: Cluster identifier (name or address/IP/FQDN)
        config: Configuration dictionary with 'clusters' list
        client_cache: Optional dict to cache clients (key: address, value: client)
                     If provided, will reuse clients from cache instead of creating new ones
        
    Returns:
        Tuple of (cluster_address, cluster_config_dict, cluster_name)
        - cluster_address: The resolved cluster address (IP/FQDN)
        - cluster_config_dict: The cluster configuration dictionary
        - cluster_name: The cluster name (if resolved, otherwise same as address)
        
    Raises:
        ValueError: If cluster identifier cannot be resolved
    """
    # First, try to find by address or cluster_name
    cluster_config = [c for c in config['clusters'] 
                     if c['cluster'] == identifier or c.get('cluster_name') == identifier]
    cluster_address = identifier
    cluster_name = identifier  # Default to identifier
    
    # If found, extract the address and name
    if cluster_config:
        cluster_config_entry = cluster_config[0]
        cluster_address = cluster_config_entry['cluster']
        cluster_name = cluster_config_entry.get('cluster_name', cluster_address)
    # If not found by address or cluster_name, try to find by querying API
    else:
        # Check cache first
        if identifier in _cluster_name_to_address_cache:
            cluster_address = _cluster_name_to_address_cache[identifier]
            cluster_config = [c for c in config['clusters'] if c['cluster'] == cluster_address]
            if cluster_config:
                cluster_name = _cluster_address_to_name_cache.get(cluster_address, cluster_address)
        else:
            # Try to find by cluster name - but only check clusters that might match
            # First, check if any cluster in config has a cached name that matches
            for c in config['clusters']:
                cached_name = _cluster_address_to_name_cache.get(c['cluster'])
                if cached_name == identifier:
                    cluster_address = c['cluster']
                    cluster_name = cached_name
                    cluster_config = [c]
                    _cluster_name_to_address_cache[identifier] = cluster_address
                    break
            
            # If still not found and identifier looks like it might be a cluster name (not an address),
            # only query clusters that are likely matches (e.g., if identifier contains the cluster address substring)
            # Otherwise, skip querying all clusters to avoid unnecessary API calls
            if not cluster_config:
                # Only query clusters if the identifier doesn't look like an IP/FQDN
                # This is a heuristic: if it contains dots or looks like an IP, don't query
                looks_like_address = '.' in identifier or ':' in identifier or identifier.replace('-', '').replace('_', '').isdigit()
                
                if not looks_like_address:
                    # Identifier might be a cluster name, but we should only query if we have a reasonable match
                    # Check both cluster address and cluster_name for potential matches
                    # This prevents querying ALL clusters when a specific one is requested
                    potential_matches = [
                        c for c in config['clusters'] 
                        if (identifier.lower() in c['cluster'].lower() or 
                            c['cluster'].lower() in identifier.lower() or
                            (c.get('cluster_name') and (identifier.lower() in c['cluster_name'].lower() or 
                             c['cluster_name'].lower() in identifier.lower())))
                    ]
                    
                    # If no potential matches, don't query any clusters - just raise an error
                    if not potential_matches:
                        raise ValueError(f"Cluster {identifier} not found in config and no potential matches to query.")
                    
                    # Only query the potential matches, not all clusters
                    for c in potential_matches:
                        try:
                            # Reuse client from cache if available
                            client = None
                            if client_cache and c['cluster'] in client_cache:
                                client = client_cache[c['cluster']]
                            else:
                                # Create temporary client to query cluster name
                                username = c['username']
                                password = retrieve_password_secure(c['cluster'], username, c['password'])
                                if c.get('user_type') == 'SUPER_ADMIN':
                                    client = VASTClient(address=c['cluster'], user=username, password=password, version='latest')
                                else:
                                    client = VASTClient(address=c['cluster'], user=username, password=password, tenant=c.get('tenant', ''), version='latest')
                                
                                # Cache the client if cache dict provided
                                if client_cache is not None:
                                    client_cache[c['cluster']] = client
                            
                            all_clusters = client.clusters.get(page_size=REST_PAGE_SIZE)
                            for cl in all_clusters.get('results', []):
                                cl_name = cl.get('name', c['cluster'])
                                # Cache the address-to-name mapping
                                _cluster_address_to_name_cache[c['cluster']] = cl_name
                                # If this matches what we're looking for, cache it
                                if cl_name == identifier:
                                    cluster_address = c['cluster']
                                    cluster_name = cl_name
                                    cluster_config = [c]
                                    _cluster_name_to_address_cache[identifier] = cluster_address
                                    break
                            
                            if cluster_config:
                                break
                        except Exception as e:
                            logging.debug(f"Could not query cluster {c['cluster']} for name matching: {e}")
                            continue
                else:
                    # Identifier looks like an address but wasn't found - don't query all clusters
                    raise ValueError(f"Cluster {identifier} not found in config.")
    
    # If we found the cluster by address but cluster_name is missing, try to get it from cache
    # (We don't query here to avoid unnecessary API calls - list_clusters will update it)
    if cluster_config and cluster_name == cluster_address:
        cluster_config_entry = cluster_config[0]
        # Check if cluster_name is in config
        if 'cluster_name' in cluster_config_entry:
            cluster_name = cluster_config_entry['cluster_name']
        # Otherwise check cache
        elif cluster_address in _cluster_address_to_name_cache:
            cluster_name = _cluster_address_to_name_cache[cluster_address]
    
    if not cluster_config:
        raise ValueError(f"Cluster {identifier} not found in config.")
    
    return cluster_address, cluster_config[0], cluster_name


def get_or_create_client(cluster: str) -> VASTClient:
    """Get cached VAST client or create new one.
    
    This function provides centralized client caching to avoid redundant
    client creation for the same cluster address.
    
    Args:
        cluster: Cluster identifier (name or address)
        
    Returns:
        VAST client instance (cached or newly created)
    """
    # Resolve cluster identifier to address
    cfg = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(cluster, cfg)
    
    # Return cached client if available
    # Check cache first
    cached_client = _cache_manager.get('client', cluster_address)
    if cached_client is not None:
        return cached_client
    
    # Create new client and cache it
    client = create_vast_client(cluster_address)
    _cache_manager.set('client', cluster_address, client)
    return client


def clear_client_cache():
    """Clear the VAST client cache.
    
    Useful for testing or when you need to force client recreation.
    """
    _cache_manager.clear('client')


# Create VAST client instance for cluster
def create_vast_client(cluster: str, use_cache: bool = True):
    """Create and return a VAST client instance for the specified cluster.
    
    Supports both cluster address (IP/FQDN) and cluster name matching.
    Uses caching to avoid redundant API calls for cluster name resolution.
    
    Args:
        cluster: Cluster identifier (name or address)
        use_cache: If True, check cache before creating new client (default: True)
        
    Returns:
        VAST client instance
    """
    cfg = load_config()
    
    # Check cache first if enabled
    if use_cache:
        # Resolve to address for cache lookup
        try:
            cluster_address, _, _ = resolve_cluster_identifier(cluster, cfg)
            cached_client = _cache_manager.get('client', cluster_address)
            if cached_client is not None:
                return cached_client
        except ValueError:
            # If resolution fails, continue to create new client
            pass
    
    # Use shared resolution function
    cluster_address, cluster_info, _ = resolve_cluster_identifier(cluster, cfg)
    username = cluster_info['username']
    
    # Retrieve password securely
    try:
        password = retrieve_password_secure(cluster_address, username, cluster_info['password'])
    except Exception as e:
        raise ValueError(f"Failed to retrieve password for cluster {cluster_address}: {e}")
    
    # Check if this is a legacy version (< 5.3) - these don't support tenant parameter
    from .utils import is_vast_version_legacy
    vast_version = cluster_info.get('vast_version', '')
    is_legacy = is_vast_version_legacy(vast_version)
    
    # For legacy versions or SUPER_ADMIN, connect without tenant
    if is_legacy or cluster_info.get('user_type') == 'SUPER_ADMIN':
        client = VASTClient(address=cluster_address, user=username, password=password, version='latest')
    else:
        # Modern version with tenant admin - use tenant parameter
        client = VASTClient(address=cluster_address, user=username, password=password, tenant=cluster_info.get('tenant', ''), version='latest')

    try:
        # Track which methods we've already wrapped to avoid double-wrapping
        wrapped_methods = set()
        
        for attr_name in dir(client):
            attr = getattr(client, attr_name)
            # Example: client.views, client.snapshots, etc.
            if hasattr(attr, "__dict__"):
                for method_name, method in attr.__dict__.items():
                    # Only wrap high-level API methods, skip internal methods like 'request'
                    # Also skip if already wrapped (check by method id to avoid wrapping the wrapper)
                    method_id = id(method)
                    if (callable(method) and 
                        not method_name.startswith("_") and 
                        method_name not in ['request', 'login', 'logout'] and
                        method_id not in wrapped_methods):
                        wrapped = vast_api_wrapper(method)
                        setattr(attr, method_name, wrapped)
                        wrapped_methods.add(method_id)
        
        # Cache the client if caching is enabled
        if use_cache:
            _cache_manager.set('client', cluster_address, client)
        
        return client
    except Exception as e:
        logging.error(f"Failed to create VAST client for cluster {cluster_address}. Error: {e}")
        raise

def _build_query_string(params: Dict[str, Any], tenant_id: Optional[str] = None) -> str:
    """Build query string from parameters for logging purposes.
    
    Args:
        params: Dictionary of query parameters
        tenant_id: Optional tenant ID to include in query string
        
    Returns:
        URL-encoded query string (e.g., "key1=value1&key2=value2")
    """
    import urllib.parse
    all_params = params.copy()
    if tenant_id:
        all_params['tenant_id'] = tenant_id
    
    query_parts = []
    for key, value in sorted(all_params.items()):
        if value is not None:
            query_parts.append(f"{key}={urllib.parse.quote(str(value))}")
    return "&".join(query_parts)


def call_vast_api(
    client: VASTClient,
    endpoint: str,
    method: str = 'get',
    params: Optional[Dict[str, Any]] = None,
    tenant_id: Optional[str] = None,
    whitelist: Optional[Dict[str, List[str]]] = None
) -> List[Dict[str, Any]]:
    """Unified function to call VAST API endpoints with whitelist validation.
    
    This function provides a single entry point for all VAST API calls with:
    - Whitelist validation (restrictive by default)
    - Sub-endpoint support (if parent is whitelisted, sub-endpoints are allowed)
    - HTTP method validation
    - Automatic pagination handling
    - Consistent error messages
    - Standardized response format
    
    Args:
        client: VAST client instance
        endpoint: API endpoint name (e.g., 'views', 'monitors.ad_hoc_query')
        method: HTTP method (default: 'get')
        params: Query parameters for the API call
        tenant_id: Optional tenant ID for tenant-scoped queries
        whitelist: Optional whitelist dict. If None, no validation is performed.
                   Format: {endpoint: [allowed_methods]}, defaults to ['get'] if endpoint listed without methods
                   Empty dict = deny all (restrictive default)
    
    Returns:
        List of result dictionaries (normalized from paginated responses)
    
    Raises:
        ValueError: If endpoint is not whitelisted or method is not allowed
    """
    from .config import REST_PAGE_SIZE
    import urllib.parse
    
    if params is None:
        params = {}
    
    method = method.lower()
    
    # Whitelist validation (if whitelist is provided)
    if whitelist is not None:
        # Check if endpoint is whitelisted
        endpoint_allowed = False
        allowed_methods = []
        parent_endpoint = None
        
        # Direct match
        if endpoint in whitelist:
            endpoint_allowed = True
            allowed_methods = whitelist[endpoint]
        # Check if it's a sub-endpoint and parent is whitelisted
        elif '.' in endpoint:
            # For endpoints like monitors.1.query, check if 'monitors' is whitelisted
            # Split and take first part as parent
            parent_endpoint = endpoint.split('.')[0]
            if parent_endpoint in whitelist:
                endpoint_allowed = True
                allowed_methods = whitelist[parent_endpoint]
        # Also handle endpoints with numeric IDs like monitors.{id}.query
        # The pattern is: monitors.{number}.query
        elif endpoint.startswith('monitors.') and '.query' in endpoint:
            if 'monitors' in whitelist:
                endpoint_allowed = True
                allowed_methods = whitelist['monitors']
        
        if not endpoint_allowed:
            error_msg = (
                f"Access denied: API endpoint '{endpoint}' is not whitelisted. "
                f"Please contact your administrator to add it to the api_whitelist section "
                f"in the YAML configuration file."
            )
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        # Check HTTP method if restrictions exist
        # Note: allowed_methods is never empty now (defaults to ['get']), so we always check
        if method not in allowed_methods:
            methods_str = ', '.join(allowed_methods)
            error_msg = (
                f"Access denied: HTTP method '{method.upper()}' is not allowed for endpoint '{endpoint}'. "
                f"Allowed methods: [{methods_str}]"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)
    
    # Get endpoint object from client
    # Handle sub-endpoints (e.g., 'monitors.ad_hoc_query' -> client.monitors.ad_hoc_query)
    # Handle monitor query endpoints (e.g., 'monitors.1.query' -> client.monitors(1).query or client.monitors[1].query)
    endpoint_parts = endpoint.split('.')
    endpoint_obj = client
    
    # Special handling for monitors.{id}.query pattern
    if len(endpoint_parts) == 3 and endpoint_parts[0] == 'monitors' and endpoint_parts[2] == 'query':
        try:
            monitor_id = int(endpoint_parts[1])
            # Access as monitors(id).query or monitors[id].query
            monitors_obj = getattr(client, 'monitors', None)
            if monitors_obj is None:
                raise ValueError(f"Endpoint 'monitors' not found on VAST client")
            
            # Try callable first (monitors(id))
            if callable(monitors_obj):
                monitor_obj = monitors_obj(monitor_id)
                endpoint_obj = getattr(monitor_obj, 'query', None)
                if endpoint_obj is not None:
                    # Successfully accessed via callable
                    pass
                else:
                    raise ValueError(f"Endpoint 'monitors.{monitor_id}.query' not found on VAST client")
            # Try subscriptable (monitors[id])
            elif hasattr(monitors_obj, '__getitem__'):
                monitor_obj = monitors_obj[monitor_id]
                endpoint_obj = getattr(monitor_obj, 'query', None)
                if endpoint_obj is not None:
                    # Successfully accessed via subscript
                    pass
                else:
                    raise ValueError(f"Endpoint 'monitors.{monitor_id}.query' not found on VAST client")
            else:
                # Not callable or subscriptable, use normal access
                raise ValueError("Monitors object is not callable or subscriptable")
        except (ValueError, TypeError, AttributeError, KeyError) as e:
            # If special handling fails, fall through to normal endpoint access
            # Don't log warning - normal access should work for monitors.{id}.query
            endpoint_obj = None
    
    # Normal endpoint access (works for monitors.1.query as monitors -> 1 -> query)
    if endpoint_obj is None or endpoint_obj == client:
        endpoint_obj = client
        for part in endpoint_parts:
            endpoint_obj = getattr(endpoint_obj, part, None)
            if endpoint_obj is None:
                error_msg = f"Endpoint '{endpoint}' not found on VAST client"
                logging.error(error_msg)
                raise ValueError(error_msg)
    
    # Prepare parameters
    request_params = params.copy()
    
    # Remove internal parameters that shouldn't be sent to API
    request_params.pop('_output_format', None)
    
    # Handle pagination for GET requests
    # Special case: non-paginated endpoints don't support pagination - return single dict or non-paginated list
    # Also handle monitor query endpoints (monitors.{id}.query)
    is_non_paginated = endpoint in NON_PAGINATED_ENDPOINTS or (endpoint.startswith('monitors.') and '.query' in endpoint and endpoint.count('.') >= 2)
    if method == 'get' and is_non_paginated:
        # Don't add page_size or page parameters for monitors.ad_hoc_query
        # Build query string for logging
        query_string = _build_query_string(request_params, tenant_id)
        logging.debug(f"API Request: {method.upper()} /api/{endpoint}/?{query_string}")
        
        # Make API call (no pagination, no page parameter)
        try:
            if tenant_id:
                result = endpoint_obj.get(tenant_id=tenant_id, **request_params)
            else:
                result = endpoint_obj.get(**request_params)
        except Exception as e:
            logging.error(f"API call failed for endpoint '{endpoint}': {e}")
            raise
        
        # monitors.ad_hoc_query returns a single dict, not a list
        # Return as list for consistency with other endpoints
        if isinstance(result, dict):
            return [result]
        elif isinstance(result, list):
            return result
        else:
            return [result] if result is not None else []
    elif method == 'get':
        # Regular GET requests with pagination support
        # Ensure page_size is set (unless explicitly provided)
        if 'page_size' not in request_params:
            request_params['page_size'] = REST_PAGE_SIZE
        
        all_results = []
        page = 1
        
        while True:
            # Prepare parameters for this page
            current_params = request_params.copy()
            current_params['page'] = page
            
            # Build query string for logging
            query_string = _build_query_string(current_params, tenant_id)
            logging.debug(f"API Request: {method.upper()} /api/{endpoint}/?{query_string}")
            
            # Make API call
            try:
                if tenant_id:
                    result = endpoint_obj.get(tenant_id=tenant_id, **current_params)
                else:
                    result = endpoint_obj.get(**current_params)
            except Exception as e:
                logging.error(f"API call failed for endpoint '{endpoint}': {e}")
                raise
            
            # Handle response format
            if isinstance(result, dict):
                if 'results' in result:
                    # Paginated response
                    page_results = result.get('results', [])
                    all_results.extend(page_results)
                    
                    # Check if there are more pages
                    total = result.get('total', len(page_results))
                    if len(all_results) >= total or len(page_results) == 0:
                        break
                    page += 1
                else:
                    # Single object response
                    all_results.append(result)
                    break
            elif isinstance(result, list):
                # Direct list response
                all_results.extend(result)
                break
            else:
                # Unexpected format
                logging.warning(f"Unexpected response format from endpoint '{endpoint}': {type(result)}")
                break
        
        return all_results
    # Note: The elif block for non-paginated endpoints was removed as it's a duplicate
    # of the first if block. This code path should never be reached.
    else:
        # Non-GET methods (post, patch, delete, put)
        # Build query string for logging
        query_string = _build_query_string(request_params, tenant_id)
        logging.debug(f"API Request: {method.upper()} /api/{endpoint}/?{query_string}")
        
        # Get the appropriate method
        method_func = getattr(endpoint_obj, method, None)
        if method_func is None:
            error_msg = f"HTTP method '{method.upper()}' not supported for endpoint '{endpoint}'"
            logging.error(error_msg)
            raise ValueError(error_msg)
        
        # Make API call
        try:
            if tenant_id:
                result = method_func(tenant_id=tenant_id, **request_params)
            else:
                result = method_func(**request_params)
        except Exception as e:
            logging.error(f"API call failed for endpoint '{endpoint}' with method '{method}': {e}")
            raise
        
        # Normalize response format
        if isinstance(result, dict):
            if 'results' in result:
                return result.get('results', [])
            else:
                return [result]
        elif isinstance(result, list):
            return result
        else:
            return [result] if result is not None else []


# Get object id by name
def get_id_by_name(client: VASTClient, object_type: str, name: str, field: str='name', tenant_id: str = None, whitelist: Optional[Dict[str, List[str]]] = None) -> Optional[str]:
    """Get object ID by name.
    
    Args:
        client: VAST client instance
        object_type: Type of object (e.g., 'views', 'tenants', 'snapshots')
        name: Name of the object to find
        field: Field name to search by (default: 'name')
        tenant_id: Optional tenant ID for tenant-scoped queries
        whitelist: Optional API whitelist for validation
        
    Returns:
        Object ID if found, None otherwise
        
    Raises:
        ValueError: If object_type is not whitelisted or access denied
    """
    try:
        # Build parameters dict safely
        params = {field: name}
        
        # Use unified API call function
        result = call_vast_api(
            client=client,
            endpoint=object_type,
            method='get',
            params=params,
            tenant_id=tenant_id,
            whitelist=whitelist
        )
        
        if not result or not len(result):
            return None
        return result[0]['id']
    except ValueError:
        # Re-raise ValueError (whitelist validation errors)
        raise
    except Exception as e:
        logging.error(f"Error retrieving {object_type} by name {name}. Error: {e}")
        return None

# Get object name by id
def get_name_by_id(client: VASTClient, object_type: str, object_id: str, field: str='name', tenant_id: str = None, whitelist: Optional[Dict[str, List[str]]] = None) -> Optional[str]:
    """Get object name by ID.
    
    Args:
        client: VAST client instance
        object_type: Type of object (e.g., 'views', 'tenants', 'snapshots')
        object_id: ID of the object to find
        field: Field name to return (default: 'name')
        tenant_id: Optional tenant ID for tenant-scoped queries
        whitelist: Optional API whitelist for validation
        
    Returns:
        Object name if found, None otherwise
        
    Raises:
        ValueError: If object_type is not whitelisted or access denied
    """
    try:
        # Build parameters dict safely
        params = {'id': object_id}
        
        # Use unified API call function
        result = call_vast_api(
            client=client,
            endpoint=object_type,
            method='get',
            params=params,
            tenant_id=tenant_id,
            whitelist=whitelist
        )
        
        if not result or not len(result):
            return None
        return result[0].get(field)
    except ValueError:
        # Re-raise ValueError (whitelist validation errors)
        raise
    except Exception as e:
        logging.error(f"Error retrieving {object_type} by id {object_id}. Error: {e}")
        return None

# Get object by name (returns full object)
def get_object_by_name(client: VASTClient, object_type: str, name: str, tenant_id: str = None, whitelist: Optional[Dict[str, List[str]]] = None) -> Optional[Dict[str, Any]]:
    """Get full object by name.
    
    Args:
        client: VAST client instance
        object_type: Type of object (e.g., 'views', 'tenants', 'snapshots')
        name: Name of the object to find
        tenant_id: Optional tenant ID for tenant-scoped queries
        whitelist: Optional API whitelist for validation
        
    Returns:
        Full object dictionary if found, None otherwise
        
    Raises:
        ValueError: If object_type is not whitelisted or access denied
    """
    try:
        # Build parameters dict safely
        params = {'name': name}
        
        # Use unified API call function
        result = call_vast_api(
            client=client,
            endpoint=object_type,
            method='get',
            params=params,
            tenant_id=tenant_id,
            whitelist=whitelist
        )
        
        if not result or not len(result):
            return None
        return result[0]
    except ValueError:
        # Re-raise ValueError (whitelist validation errors)
        raise
    except Exception as e:
        logging.error(f"Error retrieving {object_type} by name {name}. Error: {e}")
        return None

