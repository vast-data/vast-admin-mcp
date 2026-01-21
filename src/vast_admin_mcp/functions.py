"""Core business logic functions for vast-admin-mcp: list operations."""

import logging
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
import os
import tempfile
from datetime import datetime, timezone
import time

from .config import (
    load_config, REST_PAGE_SIZE, PERFORMANCE_AGGREGATION_FUNCTION, TEMPLATE_MODIFICATIONS_FILE, get_default_template_path,
    MAX_VIEW_TIMEFRAME_SECONDS, METRICS_API_LIMIT,
    GRANULARITY_THRESHOLD_SECONDS, GRANULARITY_THRESHOLD_HOURS, GRANULARITY_THRESHOLD_DAYS,
    EXCLUDED_VIEW_METRIC_PATTERNS, QUERY_USERS_DEFAULT_TOP, QUERY_USERS_MAX_TOP,
    GRAPH_TEMP_DIR, GRAPH_CLEANUP_AGE_HOURS
)
from .utils import (
    pretty_size, parse_time_duration, parse_order_spec, apply_ordering, normalize_field_name, get_api_whitelist,
    convert_docker_path_to_host
)
from .client import (
    create_vast_client, get_id_by_name, get_name_by_id, resolve_cluster_identifier, get_or_create_client, call_vast_api
)
from .template_parser import TemplateParser
from .command_executor import CommandExecutor
from vastpy import VASTClient


def _get_metrics (client: VASTClient) -> List[Dict]:
    """Get all metrics from the API.
    
    Args:
        client: VAST client instance
        
    Returns:
        List of metric dictionaries
    """
    whitelist = get_api_whitelist()
    all_metrics = call_vast_api(
        client=client,
        endpoint='metrics',
        method='get',
        params={'limit': METRICS_API_LIMIT},
        whitelist=whitelist
    )
    return all_metrics

def list_monitors(cluster: str, object_type: Optional[str] = None) -> List[Dict]:
    """List all predefined monitors, optionally filtered by object_type.
    
    Args:
        cluster: Target cluster address or name (required)
        object_type: Optional object type to filter monitors (e.g., 'cluster', 'view', 'cnode')
        
    Returns:
        List of monitor dictionaries with id, name, object_type, prop_list, time_frame, etc.
    """
    config = load_config()
    cluster_address, cluster_config, cluster_name = resolve_cluster_identifier(cluster, config)
    
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()
    
    monitors = call_vast_api(
        client=client,
        endpoint='monitors',
        method='get',
        params={'page_size': REST_PAGE_SIZE},
        whitelist=whitelist
    )
    
    # Filter by object_type if provided
    if object_type:
        object_type_lower = object_type.lower()
        monitors = [m for m in monitors if m.get('object_type', '').lower() == object_type_lower]
    
    return monitors


def _get_monitor_by_name (client: VASTClient, monitor_name: str) -> Dict[str, Any]:
    """Get metrics by monitor name.
    
    Args:
        client: VAST client instance
        monitor_name: Monitor name
        
    Returns:
        Dictionary containing the monitor details
    """

    whitelist = get_api_whitelist()
    monitors = call_vast_api(
        client=client,
        endpoint='monitors',
        method='get',
        params={'page_size': REST_PAGE_SIZE},
        whitelist=whitelist
    )
    for m in monitors:
        if m['name'] == monitor_name:
            return m
    return None


def _build_metrics_map(client: VASTClient) -> Dict[str, Dict[str, Dict]]:
    """Build metrics map dynamically using the metrics API.
    
    Args:
        client: VAST client instance
        
    Returns:
        Dictionary mapping object types to their available metrics
    """
    try:
        # Get all available metrics from the API using unified function
        all_metrics = _get_metrics(client)
        
        # Create metrics map organized by object type
        metrics_map = {}
        
        # Process each metric from the API
        for metric in all_metrics:
            fqn = metric.get('fqn', '')
            object_types = metric.get('object_types', [])
            class_name = metric.get('class_name', '')
            units = metric.get('units', '')
            title = metric.get('title', '')
            for object_type in object_types:
                if object_type not in metrics_map:
                    metrics_map[object_type] = {}
                metrics_map[object_type][fqn] = {
                    'class_name': class_name,
                    'units': units,
                    'title': title
                }
        
        # Those are missing from the metrics API at the moment, so we add them manually
        metrics_map['tenant']['TenantMetrics,write_bw__time_avg'] = {
            "class_name": "ProtoMetrics", "units": "MB/Sec", "title": "Read Bandwidth"
        }
        metrics_map['tenant']['TenantMetrics,read_bw__time_avg'] = {
            "class_name": "ProtoMetrics", "units": "MB/Sec", "title": "Write Bandwidth"
        }
        metrics_map['tenant']['TenantMetrics,write_iops__time_avg'] = {
            "class_name": "ProtoMetrics", "units": "IOPS", "title": "Write IOPS"
        }
        metrics_map['tenant']['TenantMetrics,read_iops__time_avg'] = {
            "class_name": "ProtoMetrics", "units": "IOPS", "title": "Read IOPS"
        }
        metrics_map['tenant']['TenantMetrics,write_latency__avg'] = {
            "class_name": "ProtoMetrics", "units": "Milliseconds", "title": "Write Latency"
        }
        metrics_map['tenant']['TenantMetrics,read_latency__avg'] = {
            "class_name": "ProtoMetrics", "units": "Milliseconds", "title": "Read Latency"
        }
        return metrics_map
    except Exception as e:
        logging.error(f"Failed to build dynamic metrics map: {e}. Falling back to hardcoded metrics.")
        raise


def _get_instance_ids(client: VASTClient, object_name: str, instances: str, default_tenant: str) -> List[str]:
    """Get instance IDs for specified instances, supporting wildcards.
    
    Args:
        client: VAST client instance
        object_name: Object type name
        instances: Comma-separated list of instance names (optionally with tenant prefix)
                   Supports wildcards: "*" for all, "*pattern*" for pattern matching
        default_tenant: Default tenant name
        
    Returns:
        List of instance IDs
    """
    import fnmatch
    
    instance_ids = []
    if not instances:
        return instance_ids
    
    # Get whitelist once at the start to avoid repeated calls
    whitelist = get_api_whitelist()
    object_name_lower = object_name.lower()
    
    # Get all instances for wildcard matching
    all_instances_list = _get_all_instances(client, object_name)
    
    for instance_spec in instances.split(','):
        instance_spec = instance_spec.strip()
        if not instance_spec:
            continue
        
        # Parse tenant:instance format
        parts = instance_spec.split(':')
        tenant_explicitly_specified = len(parts) == 2
        if tenant_explicitly_specified:
            tenant_pattern = parts[0].strip()
            instance_pattern = parts[1].strip()
        else:
            tenant_pattern = None
            instance_pattern = instance_spec
        
        # Check if we have wildcards
        has_wildcard = '*' in instance_pattern or (tenant_pattern and '*' in tenant_pattern)
        
        if has_wildcard:
            # Handle wildcard matching
            if object_name_lower == 'view':
                # For views, we need to match against tenant:path combinations
                matching_ids = []
                
                # Get all tenants if tenant pattern has wildcard or is "*"
                if tenant_pattern and '*' in tenant_pattern:
                    # Need to check all tenants
                    all_tenants = call_vast_api(
                        client=client,
                        endpoint='tenants',
                        method='get',
                        params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
                        whitelist=whitelist
                    )
                elif tenant_pattern == '*':
                    # All tenants
                    all_tenants = call_vast_api(
                        client=client,
                        endpoint='tenants',
                        method='get',
                        params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
                        whitelist=whitelist
                    )
                elif tenant_pattern:
                    # Specific tenant (no wildcard)
                    tenant_id = get_id_by_name(client, 'tenants', tenant_pattern, whitelist=whitelist)
                    if tenant_id:
                        all_tenants = [{'id': tenant_id, 'name': tenant_pattern}]
                    else:
                        all_tenants = []
                else:
                    # No tenant specified
                    # If instance_pattern is "*", get all views from all tenants
                    # Otherwise, use default tenant
                    if instance_pattern == '*':
                        # Get all tenants for "*" pattern
                        all_tenants = call_vast_api(
                            client=client,
                            endpoint='tenants',
                            method='get',
                            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
                            whitelist=whitelist
                        )
                    else:
                        # Use default tenant for specific patterns
                        tenant_id = get_id_by_name(client, 'tenants', default_tenant, whitelist=whitelist)
                        if tenant_id:
                            all_tenants = [{'id': tenant_id, 'name': default_tenant}]
                        else:
                            all_tenants = []
                
                # Match views across all relevant tenants
                for tenant_info in all_tenants:
                    tenant_id = tenant_info['id']
                    tenant_name = tenant_info['name']
                    
                    # Check if tenant matches pattern
                    if tenant_pattern and tenant_pattern != '*':
                        if not fnmatch.fnmatch(tenant_name, tenant_pattern):
                            continue
                    
                    # Get views for this tenant
                    views = call_vast_api(
                        client=client,
                        endpoint='views',
                        method='get',
                        params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name', 'tenant_id': tenant_id},
                        whitelist=whitelist
                    )
                    
                    for view in views:
                        view_name = view.get('name', '')
                        view_id = view.get('id')
                        
                        # Match against instance pattern (use name only)
                        if instance_pattern == '*':
                            # Match all views
                            matching_ids.append(view_id)
                        elif fnmatch.fnmatch(view_name, instance_pattern):
                            matching_ids.append(view_id)
                
                instance_ids.extend(matching_ids)
            else:
                # For non-view objects, match against instance names
                matching_ids = []
                for inst in all_instances_list:
                    inst_name = inst.get('name', '')
                    if instance_pattern == '*':
                        matching_ids.append(inst['id'])
                    elif fnmatch.fnmatch(inst_name, instance_pattern):
                        matching_ids.append(inst['id'])
                instance_ids.extend(matching_ids)
        else:
            # No wildcard - use original logic
            instance = instance_pattern
            tenant = tenant_pattern if tenant_pattern else default_tenant
            
            tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
            if not tenant_id:
                raise ValueError(f"Tenant {tenant} not found.")
            
            if object_name_lower == 'view':
                # For views, use name only (not path)
                instance_id = get_id_by_name(client, f"{object_name_lower}s", instance, field='name', tenant_id=tenant_id, whitelist=whitelist)
            else:
                instance_id = get_id_by_name(client, f"{object_name_lower}s", instance, tenant_id=tenant_id, whitelist=whitelist)
            
            # Self-healing: If view not found and tenant was not explicitly specified, try other tenants
            if not instance_id and object_name_lower == 'view' and not tenant_explicitly_specified:
                try:
                    # Try to find in other tenants
                    all_tenants = call_vast_api(
                        client=client,
                        endpoint='tenants',
                        method='get',
                        params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
                        whitelist=whitelist
                    )
                    matches = []
                    for t_info in all_tenants:
                        t_id = t_info['id']
                        t_name = t_info['name']
                        if t_id == tenant_id:
                            continue  # Skip default tenant, already tried
                        
                        found_id = get_id_by_name(client, 'views', instance, field='name', tenant_id=t_id, whitelist=whitelist)
                        
                        if found_id:
                            matches.append({'tenant': t_name, 'tenant_id': t_id, 'view_id': found_id})
                    
                    if len(matches) == 1:
                        # Single match - use it and log suggestion
                        match = matches[0]
                        logging.info(
                            f"Found '{instance}' in tenant '{match['tenant']}' (not in default '{tenant}'). "
                            f"Using it. For explicit control, use instances='{match['tenant']}:<view_name>'"
                        )
                        instance_id = match['view_id']
                    elif len(matches) > 1:
                        # Multiple matches - suggest qualification
                        tenant_list = ", ".join([m['tenant'] for m in matches])
                        raise ValueError(
                            f"View '{instance}' found in multiple tenants: {tenant_list}. "
                            f"Please qualify with tenant: instances='<tenant_name>:<view_name>'"
                        )
                except Exception as e:
                    logging.debug(f"Auto-probe failed: {e}")
            
            if not instance_id:
                # Build helpful error message with suggestions
                suggestions = []
                if object_name_lower == 'view':
                    suggestions.append(f"Try: instances='{tenant}:<view_name>' (use view name, not path)")
                    suggestions.append(f"Or discover views: list_views_vast(cluster='<cluster>', tenant='{tenant}')")
                    suggestions.append(f"Or use discovery: list_view_instances_vast(cluster='<cluster>', tenant='{tenant}')")
                    suggestions.append(f"Tried: name='{instance}' in tenant '{tenant}'")
                    error_msg = f"{object_name} instance '{instance}' not found in tenant '{tenant}'."
                else:
                    suggestions.append(f"Verify the {object_name} name is correct")
                    suggestions.append(f"Check if the {object_name} exists in tenant '{tenant}'")
                    error_msg = f"{object_name} instance '{instance}' not found."
                
                if suggestions:
                    error_msg += "\nSuggestions:\n  - " + "\n  - ".join(suggestions)
                
                raise ValueError(error_msg)
            instance_ids.append(instance_id)
    
    return instance_ids


def _get_all_instances(client: VASTClient, object_name: str) -> List[Dict]:
    """Get all instances of an object type for name resolution.
    
    Args:
        client: VAST client instance
        object_name: Object type name
        
    Returns:
        List of instance dictionaries with 'id' and 'name' keys
    """
    # Get whitelist once at the start to avoid repeated calls
    whitelist = get_api_whitelist()
    object_name_lower = object_name.lower()
    
    if object_name_lower == 'view':
        all_instances = call_vast_api(
            client=client,
            endpoint='views',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name,tenant_id'},
            whitelist=whitelist
        )
        # Build tenant_id to tenant_name mapping
        all_tenants = call_vast_api(
            client=client,
            endpoint='tenants',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
            whitelist=whitelist
        )
        tenant_map = {t['id']: t['name'] for t in all_tenants}
        
        for d in all_instances:
            # Use view name (not path)
            d["name"] = d.get('name', '')
            d["tenant_id"] = d.get('tenant_id')
            d["tenant_name"] = tenant_map.get(d.get('tenant_id'), 'unknown')
    elif object_name_lower == 'tenant':
        all_instances = call_vast_api(
            client=client,
            endpoint='tenants',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
            whitelist=whitelist
        )
    elif object_name_lower == 'cnode':
        all_instances = call_vast_api(
            client=client,
            endpoint='cnodes',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
            whitelist=whitelist
        )
    elif object_name_lower == 'host':
        all_instances = call_vast_api(
            client=client,
            endpoint='hosts',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
            whitelist=whitelist
        )
    elif object_name_lower == 'user':
        all_instances = call_vast_api(
            client=client,
            endpoint='monitoredusers',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,title'},
            whitelist=whitelist
        )
        for d in all_instances:
            d["name"] = d['title']
    elif object_name_lower == 'vippool':
        all_instances = call_vast_api(
            client=client,
            endpoint='vippools',
            method='get',
            params={'page_size': REST_PAGE_SIZE, 'fields': 'id,name'},
            whitelist=whitelist
        )
    else:
        all_instances = []
    
    return all_instances


def _filter_metrics_for_object_type(prop_list: List[str], object_name: str) -> List[str]:
    """Filter metrics list based on object type requirements.
    
    Args:
        prop_list: List of metric property names
        object_name: Object type name
        
    Returns:
        Filtered list of metric property names
    """
    object_name_lower = object_name.lower()
    
    if object_name_lower == 'view':
        # Filter for view-specific metrics (only rate metrics, exclude certain patterns)
        filtered = [key for key in prop_list if '__rate' in key]
        for pattern in EXCLUDED_VIEW_METRIC_PATTERNS:
            filtered = [key for key in filtered if pattern not in key]
        return filtered
    elif object_name_lower == 'tenant':
        # Filter for tenant-specific metrics (averages only)
        return [key for key in prop_list if key.endswith('_avg')]
    else:
        # Filter for common metrics (bandwidth, IOPS, latency)
        return [key for key in prop_list if key.endswith((',bw', ',iops', ',latency'))]


def _get_granularity(timeframe_in_seconds: int) -> str:
    """Determine granularity based on timeframe.
    
    Args:
        timeframe_in_seconds: Timeframe in seconds
        
    Returns:
        Granularity string: 'seconds', 'minutes', 'hours', or 'days'
    """
    if timeframe_in_seconds > GRANULARITY_THRESHOLD_DAYS:
        return "days"
    elif timeframe_in_seconds > GRANULARITY_THRESHOLD_HOURS:
        return "hours"
    elif timeframe_in_seconds > GRANULARITY_THRESHOLD_SECONDS:
        return "minutes"
    else:
        return "seconds"


def _normalize_metric_display(display_name: str, units: str, metric_name: str) -> Tuple[str, str]:
    """Normalize metric display name and units.
    
    Args:
        display_name: Original display name
        units: Original units
        metric_name: Metric name for fallback detection
        
    Returns:
        Tuple of (normalized_display_name, normalized_units)
    """
    # Normalize display name
    if display_name == "ViewMetrics,read_latency__rate":
        display_name = "Read Latency"
    if display_name == "ViewMetrics,write_latency__rate":
        display_name = "Write Latency"
    display_name = display_name.replace(' (Rate)', '')
    display_name = display_name.replace(' (Sum)', '')
    if display_name.upper().endswith('IOPS'):
        display_name = display_name.rsplit(" ", 1)[0]
    if display_name.upper().endswith('BANDWIDTH'):
        display_name = display_name.rsplit(" ", 1)[0]
    if display_name.upper().endswith('BW'):
        display_name = display_name.rsplit(" ", 1)[0]
    if display_name.upper().endswith('LATENCY'):
        display_name = display_name.rsplit(" ", 1)[0]
    
    # Normalize units
    if not units:
        if 'iops' in metric_name.lower():
            units = 'iops'
        elif 'latency' in metric_name.lower():
            units = 'latency'
        else:
            units = 'bw'
    elif 'MB/Sec' in units:
        units = 'bw'
    elif units == 'ms':
        units = 'latency'
    elif units == 'IOPS':
        units = 'iops'
    
    # Remove spaces from units
    units = units.replace(" ", "")
    
    # Handle empty display name
    if display_name == '':
        display_name = 'All Protocols'
    if display_name.lower() in ['iops', 'latency', 'bandwidth']:
        display_name = 'All Protocols'
    
    return display_name, units


def _process_performance_data(
    metrics: Dict,
    metrics_map: Dict[str, Dict[str, Dict]],
    object_name: str,
    all_instances: List[Dict]
) -> Dict[str, Dict[str, Dict[str, Dict[str, float]]]]:
    """Process raw performance metrics data into structured format.
    
    Args:
        metrics: Raw metrics response from API
        metrics_map: Metrics map for display name/unit lookup
        object_name: Object type name
        all_instances: List of all instances for name resolution
        
    Returns:
        Nested dictionary: instance_name -> display_name -> units -> stat_name -> value
    """
    performance_table = {}
    
    if not (metrics and isinstance(metrics, dict) and 'data' in metrics and 'prop_list' in metrics):
        return performance_table
    
    prop_list_response = metrics['prop_list']
    data_points = metrics['data']
    object_ids = metrics['object_ids']
    
    for object_id in object_ids:
        # Resolve instance name from ID
        instance_name = next((v['name'] for v in all_instances if v['id'] == object_id), '')
        data_points_np = np.array([d for d in data_points if d[1] == object_id])
        
        if data_points_np.size == 0:
            logging.warning(f"No data points found for {object_name} instance: {instance_name} (ID: {object_id})")
            continue
        
        data_points_np = data_points_np[:, 2:]  # Exclude timestamp and object_id columns
        data_points_np = data_points_np.astype(np.float64)
        data_points_np = data_points_np[~np.isnan(data_points_np).any(axis=1)]  # Remove NaN rows
        
        # Check if array is empty after removing NaN rows
        if data_points_np.size == 0 or len(data_points_np) == 0:
            logging.warning(f"No valid data points found for {object_name} instance: {instance_name} (ID: {object_id}) after filtering NaN values")
            continue
        
        summarized_metrics_percentile = np.percentile(data_points_np, 95, axis=0)
        summarized_metrics_avg = np.mean(data_points_np, axis=0)
        summarized_metrics_max = np.max(data_points_np, axis=0)
        
        # Map metrics to values
        for i, metric_name in enumerate(prop_list_response[2:]):
            metric_info = metrics_map.get(object_name.lower(), {}).get(metric_name, {})
            display_name = metric_info.get('title', '')
            units = metric_info.get('units', '')
            
            display_name, units = _normalize_metric_display(display_name, units, metric_name)
            
            # Convert MB/s to bytes/s for bandwidth if needed
            if units == 'bw' and not metric_info.get('units'):
                summarized_metrics_avg[i] *= 1024 * 1024
                summarized_metrics_percentile[i] *= 1024 * 1024
                summarized_metrics_max[i] *= 1024 * 1024
            
            # Populate performance table
            if instance_name not in performance_table:
                performance_table[instance_name] = {}
            if display_name not in performance_table[instance_name]:
                performance_table[instance_name][display_name] = {}
            if units not in performance_table[instance_name][display_name]:
                performance_table[instance_name][display_name][units] = {}
            
            performance_table[instance_name][display_name][units]['Average'] = summarized_metrics_avg[i]
            performance_table[instance_name][display_name][units]['95th Percentile'] = summarized_metrics_percentile[i]
            performance_table[instance_name][display_name][units]['Max'] = summarized_metrics_max[i]
    
    return performance_table


def _format_performance_table(
    performance_table: Dict[str, Dict[str, Dict[str, Dict[str, float]]]],
    object_name: str
) -> Dict[str, List[Dict]]:
    """Format performance table into printable format.
    
    Args:
        performance_table: Structured performance data
        object_name: Object type name for column naming
        
    Returns:
        Dictionary mapping instance names to list of formatted rows
    """
    printable_performance_table = {}
    object_name_lower = object_name.lower()
    
    # Map object type to column name
    instance_column_map = {
        'tenant': 'Tenant',
        'cnode': 'CNode',
        'view': 'View',
        'host': 'Host',
        'user': 'User',
        'vippool': 'VIP Pool'
    }
    instance_column = instance_column_map.get(object_name_lower, 'Instance')
    
    for instance_name in performance_table:
        printable_performance_table[instance_name] = []
        for metric_name, stats in performance_table[instance_name].items():
            row = {'Metric': metric_name}
            row[instance_column] = instance_name
            
            for units, values in stats.items():
                for stat_name, stat_value in values.items():
                    col_name = f"{units.upper()} {stat_name}"
                    if units == 'iops':
                        row[col_name] = int(stat_value)
                    elif units == 'latency':
                        row[col_name] = f"{stat_value/1000:.2f} ms"
                    else:  # bytes/s
                        row[col_name] = pretty_size(stat_value) + '/s'
            
            printable_performance_table[instance_name].append(row)
    
    return printable_performance_table


def list_clusters(clusters: str = None):
    """List clusters with status and general info."""
    config = load_config()
    if clusters:
        clusters = clusters.split(',')

        #check if the provided list of clusters is in the list of clusters in the config
        # Check both cluster address and cluster_name
        for c in clusters:
            found = False
            for ci in config['clusters']:
                if ci['cluster'] == c or ci.get('cluster_name') == c:
                    found = True
                    break
            if not found:
                raise ValueError(f"Cluster {c} not found in config file")
        clusters = [c.strip() for c in clusters]
    logging.info(f"Listing clusters: {clusters if clusters else 'all'}")

    #return list of clusters with status and general info
    clusters_table = []
    for cluster_info in config['clusters']:

        # display information only for the specified clusters
        # Check both cluster address and cluster_name
        if clusters:
            cluster_match = (cluster_info['cluster'] in clusters or 
                           cluster_info.get('cluster_name') in clusters)
            if not cluster_match:
                continue
        
        # Check if this is a legacy version (< 5.3) - treat as SUPER_ADMIN
        from .utils import is_vast_version_legacy
        vast_version = cluster_info.get('vast_version', '')
        is_legacy = is_vast_version_legacy(vast_version)
        
        # Skip cluster only if it's not SUPER_ADMIN and not legacy version
        if not is_legacy and cluster_info.get('user_type') != 'SUPER_ADMIN':
            logging.info(f"Skipping cluster: {cluster_info['cluster']} since credentials are not for a super admin user")
        else:
            try:
                cluster = cluster_info['cluster']
                client = create_vast_client(cluster)
                whitelist = get_api_whitelist()
                all_clusters_dict = call_vast_api(
                    client=client,
                    endpoint='clusters',
                    method='get',
                    params={'page_size': REST_PAGE_SIZE},
                    whitelist=whitelist
                )
                # Convert to dict format expected by code
                all_clusters = {'results': all_clusters_dict}
                
                # Update config with cluster_name for this cluster address
                # This supports cluster renames and allows searching by cluster name
                config_updated = False
                for c in all_clusters['results']:
                    cluster_name_from_api = c.get('name')
                    if cluster_name_from_api:
                        # Find the cluster config entry by address
                        for cfg_cluster in config['clusters']:
                            if cfg_cluster['cluster'] == cluster:
                                # Update cluster_name if it's different or missing
                                if cfg_cluster.get('cluster_name') != cluster_name_from_api:
                                    cfg_cluster['cluster_name'] = cluster_name_from_api
                                    config_updated = True
                                    logging.debug(f"Updated cluster_name for {cluster}: {cluster_name_from_api}")
                                break
                    
                    cl = {
                        'Cluster': c['name'],
                        'State': c['state'],
                        'Version': ".".join(c['sw_version'].split(".")[:4]) if isinstance(c['sw_version'], str) and "." in c['sw_version'] and len(c['sw_version'].split(".")) >= 4 else c['sw_version'],
                        'Uptime': c['uptime'],
                        'Logical Used': pretty_size(c['logical_space_in_use']),
                        'Physical Used': pretty_size(c['physical_space_in_use']),
                        'Logical Free': pretty_size(c['free_logical_space']),
                        'Physical Free': pretty_size(c['free_physical_space']),
                        #'Logical Total': pretty_size(c['logical_space']),
                        #'Physical Total': pretty_size(c['physical_space']),
                        'IOPS': c['rd_iops'] + c['wr_iops'],
                        'Throughput': pretty_size(c['rd_bw'] + c['wr_bw']) + '/s'
                        }
                    clusters_table.append(cl)
                
                # Save updated config if cluster_name was added/updated
                if config_updated:
                    from .config import save_config, clear_config_cache
                    save_config(config)
                    clear_config_cache()  # Clear cache so next load gets updated config
            except Exception as e:
                cluster = cluster_info['cluster']
                logging.error(f"Failed to list cluster: {cluster}. Error: {e}")
                # Add error entry instead of raising - continue with other clusters
                error_message = str(e)
                # Extract a shorter error message for display
                if "Connection refused" in error_message or "Connection refused" in str(e):
                    error_display = "Connection refused"
                elif "timed out" in error_message.lower() or "timeout" in error_message.lower():
                    error_display = "Connection timeout"
                elif "Max retries exceeded" in error_message:
                    error_display = "Connection failed (max retries exceeded)"
                else:
                    # Use first part of error message, limit length
                    error_display = error_message.split('\n')[0][:50] + "..." if len(error_message) > 50 else error_message
                
                # Add error entry with cluster address/name
                error_cluster = {
                    'Cluster': cluster,
                    'State': 'ERROR',
                    'Version': 'N/A',
                    'Uptime': 'N/A',
                    'Logical Used': 'N/A',
                    'Physical Used': 'N/A',
                    'Logical Free': 'N/A',
                    'Physical Free': 'N/A',
                    #'Logical Total': 'N/A',
                    #'Physical Total': 'N/A',
                    'IOPS': 'N/A',
                    'Throughput': error_display  # Use this field to show error
                }
                clusters_table.append(error_cluster)
                # Continue with next cluster instead of raising   
    return clusters_table


def list_view_instances(cluster: str, tenant: str = None, name: str = None, path: str = None) -> List[Dict]:
    """
    List view instances to help discover available views.
    
    This function helps LLMs discover available view instances with their paths and tenants.
    It returns a list of views with tenant, name, path, protocols, and has_bucket information.
    
    Args:
        cluster: Target cluster address or name (required)
        tenant: Filter by tenant name (optional, supports wildcards)
        name: Filter by view name (optional, supports wildcards like *pvc*)
        path: Filter by view path (optional, supports wildcards like */data/*)
    
    Returns:
        List of dictionaries, each containing:
        - tenant: Tenant name
        - name: View name
        - path: View path
        - protocols: List of enabled protocols (e.g., ["NFS", "S3"])
        - has_bucket: Boolean indicating if S3 bucket is configured
    
    Examples:
        - List all views: list_view_instances(cluster="vast3115-var")
        - List views in specific tenant: list_view_instances(cluster="vast3115-var", tenant="tenant1")
        - Find views with "pvc" in name: list_view_instances(cluster="vast3115-var", name="*pvc*")
        - Find views in /data path: list_view_instances(cluster="vast3115-var", path="*/data/*")
    """
    import fnmatch
    
    if not cluster:
        raise ValueError("cluster parameter is required")
    
    config = load_config()
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    client = create_vast_client(cluster_address)
    
    # Get all tenants if tenant filter is specified
    tenant_id = None
    if tenant:
        whitelist = get_api_whitelist()
        tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
        if not tenant_id:
            raise ValueError(f"Tenant '{tenant}' not found.")
    
    # Query views
    views_params = {'page_size': REST_PAGE_SIZE}
    if tenant_id:
        views_params['tenant_id'] = tenant_id
    
    # Log API request with all parameters for debugging
    import urllib.parse
    query_parts = []
    for key, value in sorted(views_params.items()):
        if value is not None:
            query_parts.append(f"{key}={urllib.parse.quote(str(value))}")
    query_string = "&".join(query_parts)
    logging.debug(f"API Request: GET /api/views/?{query_string}")
    
    whitelist = get_api_whitelist()
    all_views_list = call_vast_api(
        client=client,
        endpoint='views',
        method='get',
        params=views_params,
        whitelist=whitelist
    )
    # Convert to dict format expected by code
    all_views = {'results': all_views_list}
    if isinstance(all_views, dict) and 'results' in all_views:
        views = all_views['results']
    elif isinstance(all_views, list):
        views = all_views
    else:
        views = []
    
    # Get tenant information for each view
    all_tenants = {}
    if views:
        tenant_ids = set(v.get('tenant_id') for v in views if v.get('tenant_id'))
        for tid in tenant_ids:
            tenant_name = get_name_by_id(client, 'tenants', tid)
            if tenant_name:
                all_tenants[tid] = tenant_name
    
    # Build result list with filtering
    result = []
    for view in views:
        view_tenant_id = view.get('tenant_id')
        view_tenant = all_tenants.get(view_tenant_id, '')
        view_name = view.get('name', '')
        view_path = view.get('path', '')
        
        # Apply filters
        if tenant and not fnmatch.fnmatch(view_tenant, tenant):
            continue
        if name and not fnmatch.fnmatch(view_name, name):
            continue
        if path and not fnmatch.fnmatch(view_path, path):
            continue
        
        # Extract protocols
        protocols = []
        if view.get('protocols'):
            if 'NFS' in view.get('protocols', []):
                protocols.append('NFS')
            if 'SMB' in view.get('protocols', []):
                protocols.append('SMB')
            if 'S3' in view.get('protocols', []):
                protocols.append('S3')
        
        # Check if bucket is configured
        has_bucket = bool(view.get('bucket'))
        
        result.append({
            'tenant': view_tenant,
            'name': view_name,
            'path': view_path,
            'protocols': protocols,
            'has_bucket': has_bucket
        })
    
    return result


def list_fields(command_name: str) -> Dict:
    """
    Get available fields for a command with metadata.
    
    This function helps LLMs understand what fields are available for a command,
    including their types, units, and whether they're sortable or filterable.
    
    Args:
        command_name: Name of the command (e.g., "views", "tenants", "snapshots")
    
    Returns:
        Dictionary containing:
        - command: Command name
        - fields: List of field dictionaries, each containing:
          - name: Field name
          - type: Field type (string, capacity, datetime, etc.)
          - unit: Unit for numeric/capacity fields (bytes, iops, etc.)
          - sortable: Whether the field can be used for sorting
          - filterable: Whether the field can be used for filtering
          - api_field: API parameter name (if different from display name)
          - description: Field description
    
    Examples:
        - Get fields for views: list_fields("views")
        - Get fields for tenants: list_fields("tenants")
    """
    template_path = TEMPLATE_MODIFICATIONS_FILE
    default_template_path = get_default_template_path()
    if not template_path or (not Path(template_path).exists() and not default_template_path):
        raise ValueError(f"Template modifications file not found: {template_path}")
    
    parser = TemplateParser(template_path, default_template_path=default_template_path)
    
    # Check if command exists
    if command_name not in parser.get_command_names():
        available_commands = ", ".join(parser.get_command_names())
        raise ValueError(
            f"Command '{command_name}' not found. Available commands: {available_commands}"
        )
    
    fields_config = parser.get_fields(command_name)
    args_config = parser.get_arguments(command_name)
    
    # Build a map of argument names to their config for quick lookup
    args_map = {arg.get('name', ''): arg for arg in args_config}
    
    # Build fields list with metadata
    fields_list = []
    for field in fields_config:
        # Skip hidden fields
        if field.get('hide', False):
            continue
        
        field_name = field.get('name', '')
        field_type = parser._infer_field_type(field)
        field_desc = parser._generate_field_description(field)
        
        # Determine if field is sortable/filterable
        # A field is sortable/filterable if it has an argument configuration
        arg_config = args_map.get(field_name)
        is_sortable = True  # Most fields can be sorted
        is_filterable = bool(arg_config and arg_config.get('filter', False))
        
        # Get API field name
        api_field = field.get('field', field_name)
        
        # Determine unit
        unit = None
        if field_type == 'capacity':
            unit = 'bytes'
        elif 'iops' in field_name.lower():
            unit = 'iops'
        elif 'latency' in field_name.lower() or 'time' in field_name.lower():
            unit = 'ms' if 'latency' in field_name.lower() else 'seconds'
        
        fields_list.append({
            'name': field_name,
            'type': field_type,
            'unit': unit,
            'sortable': is_sortable,
            'filterable': is_filterable,
            'api_field': api_field,
            'description': field_desc
        })
    
    return {
        'command': command_name,
        'fields': fields_list
    }


def describe_tool(tool_name: str) -> Dict:
    """
    Get tool schema with examples and accepted formats.
    
    This function returns comprehensive tool description including arguments with types,
    defaults, formats, examples, common pitfalls, and return structure.
    
    Args:
        tool_name: Name of the tool (e.g., "list_views_vast", "list_performance_vast", "list_view_instances_vast", "create_view_vast", "create_snapshot_vast")
    
    Returns:
        Dictionary containing:
        - tool_name: Tool name
        - description: Tool description
        - arguments: List of argument dictionaries with:
          - name: Argument name
          - type: Argument type
          - required: Whether argument is required
          - default: Default value (if any)
          - description: Argument description
          - examples: Example values
          - accepted_formats: Accepted format patterns
          - aliases: List of aliases (if any)
        - return_structure: Description of return value structure
        - examples: Usage examples
        - common_pitfalls: Common mistakes and how to avoid them
    
    Examples:
        - Describe views tool: describe_tool("list_views_vast")
        - Describe performance tool: describe_tool("list_performance_vast")
        - Describe create view tool: describe_tool("create_view_vast")
    """
    # Map tool names to their implementations
    tool_mappings = {
        'list_views_vast': {
            'type': 'dynamic',
            'command': 'views'
        },
        'list_tenants_vast': {
            'type': 'dynamic',
            'command': 'tenants'
        },
        'list_snapshots_vast': {
            'type': 'dynamic',
            'command': 'snapshots'
        },
        'list_performance_vast': {
            'type': 'static',
            'function': list_performance
        },
        'list_clusters_vast': {
            'type': 'static',
            'function': list_clusters
        },
        'list_view_instances_vast': {
            'type': 'static',
            'function': list_view_instances
        },
        'list_fields_vast': {
            'type': 'static',
            'function': list_fields
        },
        'query_users_vast': {
            'type': 'static',
            'function': query_users
        }
    }
    
    # Add create tools if available
    try:
        from .create_functions import (
            create_view, create_view_from_template, create_snapshot,
            create_clone, create_quota
        )
        tool_mappings.update({
            'create_view_vast': {
                'type': 'create',
                'function': create_view
            },
            'create_view_from_template_vast': {
                'type': 'create',
                'function': create_view_from_template
            },
            'create_snapshot_vast': {
                'type': 'create',
                'function': create_snapshot
            },
            'create_clone_vast': {
                'type': 'create',
                'function': create_clone
            },
            'create_quota_vast': {
                'type': 'create',
                'function': create_quota
            }
        })
    except ImportError:
        pass  # Create functions not available
    
    if tool_name not in tool_mappings:
        available_tools = ", ".join(tool_mappings.keys())
        raise ValueError(
            f"Tool '{tool_name}' not found. Available tools: {available_tools}"
        )
    
    tool_info = tool_mappings[tool_name]
    
    if tool_info['type'] == 'dynamic':
        # Handle dynamic tools (from YAML templates)
        command_name = tool_info['command']
        template_path = TEMPLATE_MODIFICATIONS_FILE
        default_template_path = get_default_template_path()
        if not template_path or (not Path(template_path).exists() and not default_template_path):
            raise ValueError(f"Template file not found: {template_path}")
        
        parser = TemplateParser(template_path, default_template_path=default_template_path)
        template = parser.get_command_template(command_name)
        if not template:
            raise ValueError(f"Command '{command_name}' not found in template")
        
        description = parser.get_description(command_name)
        args_config = parser.get_arguments(command_name)
        fields_config = parser.get_fields(command_name)
        
        # Build arguments list
        arguments = []
        for arg in args_config:
            arg_dict = {
                'name': arg.get('name', ''),
                'type': arg.get('type', 'str'),
                'required': arg.get('mandatory', False),
                'default': arg.get('default', None),
                'description': arg.get('description', ''),
                'aliases': arg.get('aliases', [])
            }
            
            # Add examples based on type
            arg_type = arg.get('type', 'str')
            if arg_type == 'str':
                arg_dict['examples'] = ['value', '*value*', 'value*']
                arg_dict['accepted_formats'] = ['exact match', 'wildcard: *value*', 'starts with: value*', 'ends with: *value']
            elif arg_type == 'int':
                arg_dict['examples'] = ['100', '>100', '>=100', '<100', '<=100']
                arg_dict['accepted_formats'] = ['exact: 100', 'greater: >100', 'greater or equal: >=100', 'less: <100', 'less or equal: <=100']
            elif arg_type == 'capacity':
                arg_dict['examples'] = ['1TB', '>500GB', '>=1M', '<100KB']
                arg_dict['accepted_formats'] = ['exact: 1TB', 'greater: >1TB', 'greater or equal: >=500GB', 'less: <100KB', 'less or equal: <=1M']
            elif arg_type == 'bool':
                arg_dict['examples'] = ['true', 'false', 'True', 'False', '1', '0']
                arg_dict['accepted_formats'] = ['boolean: true/false', 'case-insensitive', 'numeric: 1/0']
            else:
                arg_dict['examples'] = []
                arg_dict['accepted_formats'] = []
            
            arguments.append(arg_dict)
        
        # Build return structure from fields
        return_fields = []
        for field in fields_config:
            if field.get('hide', False):
                continue
            return_fields.append({
                'name': field.get('name', ''),
                'type': parser._infer_field_type(field),
                'description': parser._generate_field_description(field)
            })
        
        return_structure = {
            'type': 'list',
            'items': {
                'type': 'dict',
                'fields': return_fields
            }
        }
        
        # Build examples
        examples = [
            f"{tool_name}(cluster='vast3115-var')",
            f"{tool_name}(cluster='vast3115-var', tenant='tenant1')"
        ]
        
        # Build common pitfalls
        pitfalls = [
            "Don't use cluster names without first calling list_clusters_vast to discover available clusters",
            "For views, use name not path when filtering",
            "Use tenant:view_name format for view-specific operations"
        ]
        
    elif tool_info['type'] == 'create':
        # Handle create tools (use introspection similar to static tools but with better parsing)
        func = tool_info['function']
        import inspect
        sig = inspect.signature(func)
        doc = inspect.getdoc(func) or ""
        
        # Parse docstring for description
        description = doc.split('\n\n')[0] if doc else f"Tool: {tool_name}"
        
        # Build arguments from function signature
        arguments = []
        for param_name, param in sig.parameters.items():
            if param_name == 'mcp' or param_name == 'view_template_file':  # Skip internal parameters
                continue
            
            # Parse type annotation
            param_type = 'str'
            if param.annotation != inspect.Parameter.empty:
                type_str = str(param.annotation)
                if 'List' in type_str or 'list' in type_str:
                    param_type = 'list'
                elif 'Dict' in type_str or 'dict' in type_str:
                    param_type = 'dict'
                elif 'int' in type_str:
                    param_type = 'int'
                elif 'bool' in type_str:
                    param_type = 'bool'
                elif 'Optional' in type_str:
                    # Extract the inner type from Optional[Type]
                    inner_type = type_str.replace('Optional[', '').replace(']', '').strip()
                    if 'str' in inner_type:
                        param_type = 'str'
                    elif 'int' in inner_type:
                        param_type = 'int'
            
            arg_dict = {
                'name': param_name,
                'type': param_type,
                'required': param.default == inspect.Parameter.empty,
                'default': param.default if param.default != inspect.Parameter.empty else None,
                'description': '',
                'examples': [],
                'accepted_formats': []
            }
            
            # Try to extract description from docstring
            if doc:
                # Look for Args section
                if 'Args:' in doc:
                    args_section = doc.split('Args:')[1].split('Returns:')[0] if 'Returns:' in doc else doc.split('Args:')[1]
                    # Split by lines and look for the parameter
                    lines = args_section.split('\n')
                    for i, line in enumerate(lines):
                        line_stripped = line.strip()
                        # Match parameter name at start of line (with possible indentation)
                        if line_stripped.startswith(param_name + ':') or line_stripped.startswith(param_name + ' ('):
                            # Extract description after colon
                            if ':' in line_stripped:
                                desc = line_stripped.split(':', 1)[1].strip()
                                if desc:
                                    arg_dict['description'] = desc
                                    break
                        # Also check if this line contains just the param name and next line has description
                        elif line_stripped == param_name and i + 1 < len(lines):
                            next_line = lines[i + 1].strip()
                            if next_line and not next_line.startswith(param_name):
                                arg_dict['description'] = next_line
                                break
            
            # Add examples based on parameter name and type
            if param_name == 'cluster':
                arg_dict['examples'] = ['vast3115-var', 'cluster1']
                arg_dict['accepted_formats'] = ['cluster address or name']
            elif param_name == 'tenant' or param_name == 'source_tenant':
                arg_dict['examples'] = ['default', 'tenant1']
                arg_dict['accepted_formats'] = ['tenant name (defaults to "default")']
            elif param_name == 'path' or param_name == 'source_path' or param_name == 'destination_path':
                arg_dict['examples'] = ['/nfs/myshare', '/s3/mybucket']
                arg_dict['accepted_formats'] = ['absolute path starting with /']
            elif param_name == 'hard_quota' or param_name == 'hard_limit' or param_name == 'soft_limit':
                arg_dict['examples'] = ['10GB', '1TB', '500GB']
                arg_dict['accepted_formats'] = ['size with unit: B, KB, MB, GB, TB, PB']
            elif param_name == 'expiry_time':
                arg_dict['examples'] = ['2d', '3w', '1d6h', '30m']
                arg_dict['accepted_formats'] = ['duration: d (days), h (hours), m (minutes), w (weeks)']
            elif param_name == 'protocols':
                arg_dict['examples'] = ['NFS', 'NFS,S3', 'S3,SMB']
                arg_dict['accepted_formats'] = ['comma-separated list: NFS, S3, SMB, ENDPOINT']
            elif param_type == 'bool':
                arg_dict['examples'] = ['true', 'false']
                arg_dict['accepted_formats'] = ['boolean: true/false']
            
            arguments.append(arg_dict)
        
        # Parse return structure from docstring
        return_structure = {
            'type': 'varies',
            'description': 'See function docstring for return structure'
        }
        if doc and 'Returns:' in doc:
            returns_section = doc.split('Returns:')[1]
            return_structure['description'] = returns_section.strip().split('\n')[0]
        
        # Build examples
        examples = []
        if tool_name == 'create_view_vast':
            examples = [
                "create_view_vast(cluster='vast3115-var', tenant='default', path='/nfs/myshare')",
                "create_view_vast(cluster='vast3115-var', path='/s3/mybucket', protocols='S3', bucket='mybucket', bucket_owner='user1')"
            ]
        elif tool_name == 'create_snapshot_vast':
            examples = [
                "create_snapshot_vast(cluster='vast3115-var', path='/nfs/myshare', snapshot_name='backup1')",
                "create_snapshot_vast(cluster='vast3115-var', path='/nfs/myshare', snapshot_name='backup1', expiry_time='7d')"
            ]
        elif tool_name == 'create_clone_vast':
            examples = [
                "create_clone_vast(cluster='vast3115-var', source_path='/nfs/source', source_snapshot='snap1', destination_path='/nfs/clone')"
            ]
        elif tool_name == 'create_quota_vast':
            examples = [
                "create_quota_vast(cluster='vast3115-var', path='/nfs/myshare', hard_limit='1TB', soft_limit='800GB')"
            ]
        else:
            examples = [f"{tool_name}(cluster='<cluster>', ...)"]
        
        # Build common pitfalls
        pitfalls = [
            "Cluster is required - use list_clusters_vast to discover available clusters",
            "These operations require read-write mode - ensure MCP server was started with --read-write flag",
            "Tenant defaults to 'default' if not specified"
        ]
        
    else:
        # Handle static tools
        func = tool_info['function']
        import inspect
        sig = inspect.signature(func)
        doc = inspect.getdoc(func) or ""
        
        # Parse docstring for description
        description = doc.split('\n\n')[0] if doc else f"Tool: {tool_name}"
        
        # Build arguments from function signature
        arguments = []
        for param_name, param in sig.parameters.items():
            if param_name == 'mcp':  # Skip internal mcp parameter
                continue
            
            param_type = 'str'
            if param.annotation != inspect.Parameter.empty:
                type_str = str(param.annotation)
                if 'List' in type_str or 'list' in type_str:
                    param_type = 'list'
                elif 'Dict' in type_str or 'dict' in type_str:
                    param_type = 'dict'
                elif 'int' in type_str:
                    param_type = 'int'
                elif 'bool' in type_str:
                    param_type = 'bool'
            
            arg_dict = {
                'name': param_name,
                'type': param_type,
                'required': param.default == inspect.Parameter.empty,
                'default': param.default if param.default != inspect.Parameter.empty else None,
                'description': '',
                'examples': [],
                'accepted_formats': []
            }
            
            # Try to extract description from docstring
            if doc:
                # Look for Args section
                if 'Args:' in doc:
                    args_section = doc.split('Args:')[1].split('Returns:')[0] if 'Returns:' in doc else doc.split('Args:')[1]
                    for line in args_section.split('\n'):
                        if param_name in line and ':' in line:
                            arg_dict['description'] = line.split(':', 1)[1].strip()
                            break
            
            arguments.append(arg_dict)
        
        return_structure = {
            'type': 'varies',
            'description': 'See function docstring for return structure'
        }
        
        examples = [f"{tool_name}(...)"]
        pitfalls = []
    
    return {
        'tool_name': tool_name,
        'description': description,
        'arguments': arguments,
        'return_structure': return_structure,
        'examples': examples,
        'common_pitfalls': pitfalls
    }


def list_performance(object_name: str, cluster: str, timeframe: str = "5m", instances: str = None):
    """
    List performance metrics for cluster objects using ad_hoc_query API.
    
    CRITICAL: The object_name parameter must be the OBJECT TYPE only (e.g., "view", "cnode", "tenant"), NOT a specific instance name.
    If you want metrics for a specific instance like "view-142", you MUST:
    1. Set object_name to the type: "view"
    2. Set instances to the specific instance in the correct format: "tenant_name:view_name" (e.g., "tenant1:view-142")
    
    Args:
        object_name: Object TYPE (cluster, cnode, host, user, vip, vippool, view, tenant). 
                    DO NOT include instance names or identifiers here (e.g., use "view" NOT "view-142" or "view_name").
                    If you have a specific instance like "view-142", set object_name="view" and use instances parameter.
        cluster: Target cluster address or name (required)
        timeframe: Time frame for metrics (e.g., 5m, 1h, 24h). Defaults to "5m"
        instances: Comma-separated list of specific instance identifiers.
                   - For most object types: provide instance names directly (e.g., "cnode1,cnode2").
                   - For views (which are tenant-specific): MUST use format "tenant_name:view_name" (e.g., "tenant1:myview").
                     Use view name, NOT view path. Example: "tenant1:view1,tenant2:view2".
                   - Leave empty to get metrics for all instances of the object type.
                   - If object_name is "view" and you want specific views, instances is REQUIRED in "tenant:view_name" format.
        
    Returns:
        Dict where each key is the instance name which contains a list of performance metrics.
        
    Raises:
        ValueError: If cluster is not provided or invalid object_name
        
    Examples:
        - Get all view metrics: list_performance("view", "vast3115-var")
        - Get specific view "view-142": list_performance("view", "vast3115-var", instances="tenant1:view-142")
        - Get all cnode metrics: list_performance("cnode", "vast3115-var")
        - Get specific cnodes: list_performance("cnode", "vast3115-var", instances="cnode1,cnode2")
    """
    if not cluster:
        raise ValueError("cluster parameter is required")
    
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    default_tenant = cluster_config['tenant']
    
    # Validate timeframe
    try:
        timeframe_in_seconds = parse_time_duration(timeframe)
    except Exception as e:
        raise ValueError(f"Invalid timeframe format '{timeframe}': {e}")
    
    # Validate object type
    possible_object_types = ['cluster', 'cnode', 'host', 'user', 'vippool', 'view', 'tenant']
    object_name_lower = object_name.lower()
    
    # Check if user mistakenly passed an instance name instead of object type
    if object_name_lower not in possible_object_types:
        # Check if it looks like an instance name (contains hyphen, underscore, or numbers)
        if any(char in object_name for char in ['-', '_']) or any(char.isdigit() for char in object_name):
            suggestions = [
                f"object_name must be the OBJECT TYPE, not an instance name.",
                f"Valid object types: {', '.join(possible_object_types)}",
                f"Example: If you want metrics for '{object_name}', use:",
                f"  - object_name='view' (the type)",
                f"  - instances='tenant_name:view_name' (the specific instance)",
                f"Or discover available instances: list_view_instances_vast(cluster='{cluster}')"
            ]
            error_msg = (
                f"Invalid object_name '{object_name}'. It appears you passed an instance name instead of an object type.\n"
                + "\n".join([f"  - {s}" for s in suggestions])
            )
            raise ValueError(error_msg)
        raise ValueError(
            f"Invalid object_name '{object_name}'. Must be one of: {', '.join(possible_object_types)}. "
            f"Use list_view_instances_vast() to discover available instances."
        )
    
    # Create client
    client = create_vast_client(cluster_address)
    
    # Get instance IDs if specified
    instance_ids = _get_instance_ids(client, object_name_lower, instances, default_tenant) if instances else []
    
    # Get all instances for name resolution
    all_instances = _get_all_instances(client, object_name_lower)
    
    logging.info(f"Preparing to retrieve performance metrics for object: {object_name}, instances: {instances if instances else 'ALL'}, timeframe: {timeframe} on cluster: {cluster_address}")
    
    # Build metrics map dynamically
    metrics_map = _build_metrics_map(client)
    
    if object_name_lower not in metrics_map:
        raise ValueError(f"Object type '{object_name}' not recognized. Can be only one of: {', '.join(metrics_map.keys())}")
    
    # For 'view' object type, limit timeframe to maximum of 8 hours
    if object_name_lower == 'view' and timeframe_in_seconds > MAX_VIEW_TIMEFRAME_SECONDS:
        logging.warning("For 'view' object type, maximum timeframe supported is 8 hours. Adjusting timeframe to 8 hours.")
        timeframe = "8h"
        timeframe_in_seconds = MAX_VIEW_TIMEFRAME_SECONDS
    
    # Get and filter metrics for the specified object type
    prop_list = list(metrics_map.get(object_name_lower, {}).keys())
    prop_list = _filter_metrics_for_object_type(prop_list, object_name_lower)
    
    # Determine granularity
    granularity = _get_granularity(timeframe_in_seconds)
    
    try:
        logging.info(f"Retrieving performance metrics for {object_name_lower} with timeframe {timeframe}")
        
        # Query performance metrics using ad_hoc_query API
        whitelist = get_api_whitelist()
        if object_name_lower == 'view':
            metrics_list = call_vast_api(
                client=client,
                endpoint='monitors.ad_hoc_query',
                method='get',
                params={
                    'object_type': object_name_lower,
                    'time_frame': timeframe,
                    'prop_list': prop_list,
                    'object_ids': instance_ids if instance_ids else []
                },
                whitelist=whitelist
            )
            # monitors.ad_hoc_query returns a single dict, not a list
            # Extract from list if it was wrapped
            if isinstance(metrics_list, list) and len(metrics_list) == 1 and isinstance(metrics_list[0], dict):
                metrics = metrics_list[0]
            elif isinstance(metrics_list, dict):
                metrics = metrics_list
            else:
                metrics = {}
        else:
            metrics_list = call_vast_api(
                client=client,
                endpoint='monitors.ad_hoc_query',
                method='get',
                params={
                    'object_type': object_name_lower,
                    'time_frame': timeframe,
                    'prop_list': prop_list,
                    'granularity': granularity,
                    'aggregation': PERFORMANCE_AGGREGATION_FUNCTION,
                    'object_ids': instance_ids if instance_ids else []
                },
                whitelist=whitelist
            )
            # monitors.ad_hoc_query returns a single dict, not a list
            # Extract from list if it was wrapped
            if isinstance(metrics_list, list) and len(metrics_list) == 1 and isinstance(metrics_list[0], dict):
                metrics = metrics_list[0]
            elif isinstance(metrics_list, dict):
                metrics = metrics_list
            else:
                metrics = {}
        
        # Process the metrics response
        performance_table = _process_performance_data(metrics, metrics_map, object_name_lower, all_instances)
        
        # Format into printable table
        printable_performance_table = _format_performance_table(performance_table, object_name_lower)
        
        return printable_performance_table
        
    except Exception as e:
        logging.error(f"Failed to retrieve performance metrics for {object_name} on {cluster_address}. Error: {e}")
        raise


def _extract_metric_label(prop_string: str) -> str:
    """Extract a readable label from a prop_list string.
    
    Args:
        prop_string: Prop string like "ProtoMetrics,proto_name=SMBCommon,rd_iops"
        
    Returns:
        Readable label like "SMB Read IOPS"
    """
    # Split by comma to get parts
    parts = prop_string.split(',')
    
    # Try to extract meaningful parts
    label_parts = []
    proto_name = None
    metric_name = None
    
    for part in parts:
        part = part.strip()
        if '=' in part:
            key, value = part.split('=', 1)
            if key == 'proto_name':
                proto_name = value
        elif part and not part.startswith('ProtoMetrics') and not part.startswith('TenantMetrics'):
            # This is likely the metric name
            metric_name = part
    
    # Build readable label
    if proto_name and metric_name:
        # Extract read/write and metric type from metric_name
        if 'rd_' in metric_name or 'read' in metric_name.lower():
            direction = 'Read'
        elif 'wr_' in metric_name or 'write' in metric_name.lower():
            direction = 'Write'
        else:
            direction = ''
        
        # Extract metric type
        if 'iops' in metric_name.lower():
            metric_type = 'IOPS'
        elif 'bw' in metric_name.lower() or 'bandwidth' in metric_name.lower():
            metric_type = 'Bandwidth'
        elif 'latency' in metric_name.lower() or 'lat' in metric_name.lower():
            metric_type = 'Latency'
        else:
            metric_type = metric_name.replace('_', ' ').title()
        
        if direction:
            return f"{proto_name} {direction} {metric_type}"
        else:
            return f"{proto_name} {metric_type}"
    elif metric_name:
        # Just use the metric name, cleaned up
        return metric_name.replace('_', ' ').title()
    else:
        # Fallback to using the prop_string, but cleaned up
        return prop_string.split(',')[-1].replace('_', ' ').title()


def _process_performance_graph_stats(
    data_points: List[List],
    prop_list_response: List[str],
    monitor_prop_list: List[str],
    instance_data: Dict[int, str],
    object_ids: List[int]
) -> Dict[str, Any]:
    """Process performance data points to calculate statistics for graph metrics.
    
    Args:
        data_points: Raw data points array where each row is [timestamp, object_id, metric1, metric2, ...]
        prop_list_response: List of metric property names from API response (first two are timestamp and object_id)
        monitor_prop_list: List of prop_list items from monitor definition
        instance_data: Dictionary mapping object_id to instance name
        object_ids: List of object IDs that were plotted
        
    Returns:
        Dictionary containing:
        - instances: List of per-instance statistics (only if instances were specified)
        - summary: Aggregated statistics across all instances (always present)
    """
    # Map monitor_prop_list items to their indices in prop_list_response
    # prop_list_response format: [timestamp, object_id, metric1, metric2, ...]
    prop_to_index = {}  # monitor_prop_item -> index in prop_list_response
    
    for i, prop_name in enumerate(prop_list_response[2:], start=2):  # Skip timestamp (0) and object_id (1)
        # Try to match prop_name with items in monitor_prop_list
        for monitor_prop in monitor_prop_list:
            if monitor_prop in prop_to_index:
                continue  # Already matched
            # Try exact match first
            if prop_name == monitor_prop:
                prop_to_index[monitor_prop] = i
                break
            # Try matching by the last part (metric name)
            monitor_prop_parts = monitor_prop.split(',')
            prop_name_parts = prop_name.split(',')
            if len(monitor_prop_parts) > 0 and len(prop_name_parts) > 0:
                # Match by last part (the actual metric name)
                if monitor_prop_parts[-1] == prop_name_parts[-1]:
                    prop_to_index[monitor_prop] = i
                    break
                # Or check if one contains the other
                if monitor_prop_parts[-1] in prop_name or prop_name_parts[-1] in monitor_prop:
                    prop_to_index[monitor_prop] = i
                    break
    
    if not prop_to_index:
        logging.warning(f"Could not match monitor prop_list items to API response. Monitor props: {monitor_prop_list}, API props: {prop_list_response[2:]}")
        return {"summary": {"metrics": []}}
    
    # Determine unit type for each prop_list item
    def _detect_unit_type(prop_string: str) -> str:
        """Detect unit type from prop string."""
        prop_lower = prop_string.lower()
        if 'iops' in prop_lower:
            return 'iops'
        elif 'bw' in prop_lower or 'bandwidth' in prop_lower:
            return 'bw'
        elif 'latency' in prop_lower or 'lat' in prop_lower:
            return 'latency'
        else:
            return 'unknown'
    
    # Convert data_points to numpy array for easier processing
    if not data_points:
        return {"summary": {"metrics": []}}
    
    # Determine if we have multiple distinct instances
    unique_object_ids = set()
    for dp in data_points:
        if len(dp) > 1:
            try:
                unique_object_ids.add(int(dp[1]))  # object_id is at index 1
            except (ValueError, TypeError):
                continue
    
    # Process per-instance statistics (only if multiple distinct instances exist)
    instances_stats = []
    instances_specified = len(unique_object_ids) > 1
    
    if instances_specified:
        for object_id in unique_object_ids:
            instance_name = instance_data.get(object_id, f"Unknown-{object_id}")
            # Filter data points for this specific object_id
            instance_data_points = []
            for d in data_points:
                if len(d) > 1:
                    try:
                        if int(d[1]) == object_id:
                            instance_data_points.append(d)
                    except (ValueError, TypeError):
                        continue
            
            if not instance_data_points:
                continue
            
            instance_metrics = []
            
            for monitor_prop in monitor_prop_list:
                if monitor_prop not in prop_to_index:
                    continue
                
                prop_index = prop_to_index[monitor_prop]
                # Extract values for this prop across all data points for this instance
                values = []
                for dp in instance_data_points:
                    if len(dp) > prop_index:
                        try:
                            val = float(dp[prop_index])
                            if not np.isnan(val):
                                values.append(val)
                        except (ValueError, TypeError):
                            continue
                
                if not values:
                    continue
                
                values_array = np.array(values, dtype=np.float64)
                # Filter out NaN and Inf values
                values_array = values_array[np.isfinite(values_array)]
                
                if len(values_array) == 0:
                    continue
                
                metric_name = _extract_metric_label(monitor_prop)
                unit = _detect_unit_type(monitor_prop)
                
                # Calculate statistics
                stats = {
                    "metric_name": metric_name,
                    "prop": monitor_prop,
                    "avg": float(np.mean(values_array)),
                    "p95": float(np.percentile(values_array, 95)),
                    "max": float(np.max(values_array)),
                    "unit": unit
                }
                instance_metrics.append(stats)
            
            if instance_metrics:
                instances_stats.append({
                    "instance_name": instance_name,
                    "metrics": instance_metrics
                })
    
    # Calculate summary statistics (aggregate across all instances)
    summary_metrics = []
    
    for monitor_prop in monitor_prop_list:
        if monitor_prop not in prop_to_index:
            continue
        
        prop_index = prop_to_index[monitor_prop]
        # Extract values for this prop across ALL data points (all instances)
        values = []
        for dp in data_points:
            if len(dp) > prop_index:
                try:
                    val = float(dp[prop_index])
                    if not np.isnan(val):
                        values.append(val)
                except (ValueError, TypeError):
                    continue
        
        if not values:
            continue
        
        values_array = np.array(values, dtype=np.float64)
        # Filter out NaN and Inf values
        values_array = values_array[np.isfinite(values_array)]
        
        if len(values_array) == 0:
            continue
        
        metric_name = _extract_metric_label(monitor_prop)
        unit = _detect_unit_type(monitor_prop)
        
        # Calculate statistics
        stats = {
            "metric_name": metric_name,
            "prop": monitor_prop,
            "avg": float(np.mean(values_array)),
            "p95": float(np.percentile(values_array, 95)),
            "max": float(np.max(values_array)),
            "unit": unit
        }
        summary_metrics.append(stats)
    
    result = {
        "summary": {
            "metrics": summary_metrics
        }
    }
    
    # Only include instances if they were specified
    if instances_specified and instances_stats:
        result["instances"] = instances_stats
    
    return result


def _create_performance_graph(
    data_points: List[List],
    prop_list_response: List[str],
    metrics_map: Dict[str, Dict[str, Dict]],
    object_name: str,
    instance_data: Dict[int, str],
    monitor_prop_list: List[str],
    output_path: str,
    timeframe: str,
    granularity: str,
    cluster_name: Optional[str] = None
) -> None:
    """Create a matplotlib performance graph from raw data points.
    
    Args:
        data_points: Raw data points array where each row is [timestamp, object_id, metric1, metric2, ...]
        prop_list_response: List of metric property names from API response (first two are timestamp and object_id)
        metrics_map: Metrics map for display name/unit lookup (may be empty for monitor-based graphs)
        object_name: Object type name
        instance_data: Dictionary mapping object_id to instance name
        monitor_prop_list: List of prop_list items from monitor definition (each becomes a separate line)
        output_path: Path where to save the graph image
        timeframe: Timeframe string for title
        granularity: Granularity string ('seconds', 'minutes', 'hours', 'days') for x-axis formatting
        cluster_name: Cluster name for title
    """
    try:
        import matplotlib
        matplotlib.use('Agg')  # Use non-interactive backend
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter
    except ImportError:
        raise ImportError("matplotlib is required for graph generation. Install it with: pip install matplotlib>=3.5.0")
    
    # Map monitor_prop_list items to their indices in prop_list_response
    # prop_list_response format: [timestamp, object_id, metric1, metric2, ...]
    # We need to find which index in prop_list_response corresponds to each monitor_prop_list item
    prop_to_index = {}  # monitor_prop_item -> index in prop_list_response
    
    for i, prop_name in enumerate(prop_list_response[2:], start=2):  # Skip timestamp (0) and object_id (1)
        # Try to match prop_name with items in monitor_prop_list
        for monitor_prop in monitor_prop_list:
            if monitor_prop in prop_to_index:
                continue  # Already matched
            # The prop_name from API might be the full prop string or just the metric part
            # Try exact match first, then check if prop_name contains the monitor_prop or vice versa
            if prop_name == monitor_prop:
                prop_to_index[monitor_prop] = i
                break
            # Try matching by the last part (metric name)
            monitor_prop_parts = monitor_prop.split(',')
            prop_name_parts = prop_name.split(',')
            if len(monitor_prop_parts) > 0 and len(prop_name_parts) > 0:
                # Match by last part (the actual metric name)
                if monitor_prop_parts[-1] == prop_name_parts[-1]:
                    prop_to_index[monitor_prop] = i
                    break
                # Or check if one contains the other
                if monitor_prop_parts[-1] in prop_name or prop_name_parts[-1] in monitor_prop:
                    prop_to_index[monitor_prop] = i
                    break
    
    if not prop_to_index:
        raise ValueError(f"Could not match monitor prop_list items to API response. Monitor props: {monitor_prop_list}, API props: {prop_list_response[2:]}")
    
    # Determine unit type for each prop_list item
    def _detect_unit_type(prop_string: str) -> Tuple[str, str]:
        """Detect unit type and label from prop string.
        
        Returns:
            Tuple of (unit_type, y_label) where unit_type is 'iops', 'bw', 'latency', or 'unknown'
        """
        prop_lower = prop_string.lower()
        if 'iops' in prop_lower:
            return ('iops', 'IOPS')
        elif 'bw' in prop_lower or 'bandwidth' in prop_lower:
            return ('bw', 'MB/s')
        elif 'latency' in prop_lower or 'lat' in prop_lower:
            return ('latency', 'ms')
        else:
            return ('unknown', 'Value')
    
    # Map each prop to its unit type
    prop_units = {}  # monitor_prop -> (unit_type, y_label)
    for monitor_prop in monitor_prop_list:
        prop_units[monitor_prop] = _detect_unit_type(monitor_prop)
    
    # Check if we have multiple unit types (need dual y-axis)
    unique_units = set(unit_type for unit_type, _ in prop_units.values())
    has_multiple_units = len(unique_units) > 1
    
    # Organize data by prop_list item (each becomes a separate line)
    # Structure: prop_label -> {timestamps: [], values: []}
    plot_data = {}
    
    for data_point in data_points:
        if len(data_point) < 3:
            continue
        
        timestamp = data_point[0]
        object_id = data_point[1]
        
        # Convert object_id to int if it's a string (for dictionary lookup)
        if isinstance(object_id, str):
            try:
                object_id = int(object_id)
            except (ValueError, TypeError):
                logging.warning(f"Could not convert object_id to int: {object_id}")
                continue
        
        # Process each prop_list item
        for monitor_prop in monitor_prop_list:
            if monitor_prop not in prop_to_index:
                continue
            
            idx = prop_to_index[monitor_prop]
            if idx >= len(data_point):
                continue
            
            # Extract readable label for this prop
            prop_label = _extract_metric_label(monitor_prop)
            
            unit_type, y_label = prop_units[monitor_prop]
            
            if prop_label not in plot_data:
                plot_data[prop_label] = {
                    'timestamps': [], 
                    'values': [],
                    'unit_type': unit_type,
                    'y_label': y_label
                }
            
            # Get the value
            val = data_point[idx]
            try:
                if isinstance(val, str):
                    val = float(val)
                elif not isinstance(val, (int, float)):
                    val = float(val)
                
                if not np.isnan(val):
                    # Convert units based on detected unit type
                    if unit_type == 'bw' and val > 1000:  # Likely bytes/s, convert to MB/s
                        val = val / (1024 * 1024)
                    elif unit_type == 'latency' and val > 1000:  # Likely microseconds, convert to ms
                        val = val / 1000.0
                    
                    plot_data[prop_label]['timestamps'].append(timestamp)
                    plot_data[prop_label]['values'].append(val)
            except (ValueError, TypeError):
                continue
    
    if not plot_data or all(not data['timestamps'] for data in plot_data.values()):
        raise ValueError(f"No valid data points found for monitor prop_list items")
    
    # Create the graph with dark theme
    plt.style.use('dark_background')
    
    # Determine if we need dual y-axes
    if has_multiple_units:
        # Create figure with dual y-axes
        fig, ax1 = plt.subplots(figsize=(12, 6), facecolor='#1a4d4d')
        ax1.set_facecolor('#1a4d4d')
        ax2 = ax1.twinx()  # Create second y-axis
        ax2.set_facecolor('#1a4d4d')
        
        # Determine which unit goes on which axis
        # Priority: bandwidth/iops on left, latency on right (or first unit on left, second on right)
        unit_list = list(unique_units)
        left_unit = unit_list[0]
        right_unit = unit_list[1] if len(unit_list) > 1 else unit_list[0]
        
        # Prefer latency on right axis if present
        if 'latency' in unique_units:
            if 'latency' != left_unit:
                left_unit, right_unit = right_unit, left_unit
            right_unit = 'latency'
            left_unit = [u for u in unique_units if u != 'latency'][0]
        
        # Get labels for axes
        left_label = next((label for unit, label in prop_units.values() if unit == left_unit), 'Value')
        right_label = next((label for unit, label in prop_units.values() if unit == right_unit), 'Value')
        
        ax1.set_ylabel(left_label, fontsize=12, color='white')
        ax2.set_ylabel(right_label, fontsize=12, color='white')
        ax1.tick_params(axis='y', colors='white')
        ax2.tick_params(axis='y', colors='white')
        
        ax = ax1  # Use ax1 as primary axis for x-axis formatting
    else:
        # Single y-axis
        fig, ax = plt.subplots(figsize=(12, 6), facecolor='#1a4d4d')
        ax.set_facecolor('#1a4d4d')
        # Use the y_label from the first (or only) unit type
        y_label = next(iter(prop_units.values()))[1]
        ax.set_ylabel(y_label, fontsize=12, color='white')
        ax.tick_params(colors='white')
        left_unit = None
        right_unit = None
    
    # Color palette for multiple lines - vibrant colors that work well on dark backgrounds
    color_palette = ['#00D9FF', '#00FF88', '#FF6B9D', '#FFB800', '#9D4EDD', '#FF006E', '#06FFA5', '#FFBE0B']
    
    # Get cluster/instance name for title
    if cluster_name:
        title = f"Cluster {cluster_name}"
    else:
        title = "Performance Graph"
    
    # Convert timestamps to datetime for each prop
    processed_plot_data = {}
    for prop_label, data in plot_data.items():
        if not data['timestamps'] or not data['values']:
            continue
        
        timestamps_dt = []
        values_processed = []
        
        for ts, val in zip(data['timestamps'], data['values']):
            try:
                dt = None
                # Handle ISO 8601 format strings (e.g., "2025-12-25T11:45:12Z")
                if isinstance(ts, str):
                    if 'T' in ts or 'Z' in ts or '+' in ts or ts.count('-') >= 2:
                        ts_clean = ts.replace('Z', '+00:00')
                        if '+' not in ts_clean and ts_clean.count(':') >= 2:
                            if ts_clean.endswith(':'):
                                ts_clean = ts_clean.rstrip(':')
                            if not ('+' in ts_clean or ts_clean.count('-') > 2):
                                ts_clean = ts_clean + '+00:00'
                        try:
                            dt = datetime.fromisoformat(ts_clean)
                        except ValueError:
                            import re
                            match = re.match(r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})', ts)
                            if match:
                                date_part, time_part = match.groups()
                                dt = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
                                dt = dt.replace(tzinfo=timezone.utc)
                            else:
                                continue
                    else:
                        ts = float(ts)
                        if ts > 1e10:
                            ts = ts / 1000.0
                        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                elif isinstance(ts, (int, float)):
                    if ts > 1e10:
                        ts = ts / 1000.0
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    continue
                
                if dt:
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    timestamps_dt.append(dt)
                    values_processed.append(val)
            except (ValueError, TypeError, OSError):
                continue
        
        if timestamps_dt:
            processed_plot_data[prop_label] = {
                'timestamps': timestamps_dt,
                'values': values_processed,
                'unit_type': data.get('unit_type', 'unknown'),
                'y_label': data.get('y_label', 'Value')
            }
    
    if not processed_plot_data:
        raise ValueError(f"No valid data points found after timestamp conversion")
    
    # Plot using filled area style with smooth, rounded curves
    try:
        from scipy.interpolate import make_interp_spline, interp1d
        from matplotlib.dates import date2num, num2date
        scipy_available = True
    except ImportError:
        scipy_available = False
        from matplotlib.dates import date2num, num2date
    
    color_idx = 0
    
    for prop_label, data in processed_plot_data.items():
        if not data['timestamps'] or not data['values']:
            continue
        
        # Determine which axis to use for this metric
        unit_type = data.get('unit_type', 'unknown')
        if has_multiple_units:
            # Use appropriate axis based on unit type
            if unit_type == right_unit:
                plot_axis = ax2
            else:
                plot_axis = ax1
        else:
            plot_axis = ax
        
        # Sort by timestamp to ensure proper plotting
        sorted_data = sorted(zip(data['timestamps'], data['values']))
        sorted_timestamps = [t for t, v in sorted_data]
        sorted_values = [v for t, v in sorted_data]
        
        if len(sorted_timestamps) < 2:
            # Single point - just plot it
            plot_axis.fill_between(sorted_timestamps, sorted_values, alpha=0.35, 
                          label=prop_label, color=color_palette[color_idx % len(color_palette)])
            color_idx += 1
            continue
        
        try:
            # Convert timestamps to numeric for interpolation
            timestamps_num = date2num(sorted_timestamps)
            
            # Create smooth curve using interpolation
            # Generate more points for smoother, rounded curves
            num_points = min(500, max(100, len(timestamps_num) * 5))
            timestamps_smooth = np.linspace(timestamps_num.min(), timestamps_num.max(), num_points)
            
            if scipy_available and len(timestamps_num) >= 4:
                # Use cubic spline interpolation for smooth, rounded curves
                try:
                    # Use cubic spline for smooth curves
                    spline = make_interp_spline(timestamps_num, sorted_values, k=min(3, len(timestamps_num) - 1))
                    values_smooth = spline(timestamps_smooth)
                except (ValueError, np.linalg.LinAlgError):
                    # Fallback to cubic interpolation if spline fails
                    f = interp1d(timestamps_num, sorted_values, kind='cubic', 
                                bounds_error=False, fill_value='extrapolate')
                    values_smooth = f(timestamps_smooth)
            elif scipy_available and len(timestamps_num) >= 2:
                # Use linear interpolation for small datasets
                f = interp1d(timestamps_num, sorted_values, kind='linear', 
                            bounds_error=False, fill_value='extrapolate')
                values_smooth = f(timestamps_smooth)
            else:
                # Fallback: use numpy interpolation
                values_smooth = np.interp(timestamps_smooth, timestamps_num, sorted_values)
            
            # Convert back to datetime
            timestamps_smooth_dt = [num2date(ts) for ts in timestamps_smooth]
            
            # Ensure no negative values for filled area
            values_smooth = np.maximum(values_smooth, 0)
            
            # Use fill_between for filled area style with smooth, rounded curves
            # Set linewidth=0 to remove edge lines and use antialiasing for smooth appearance
            plot_axis.fill_between(timestamps_smooth_dt, values_smooth, alpha=0.35, 
                          label=prop_label, color=color_palette[color_idx % len(color_palette)], 
                          linewidth=0, antialiased=True, interpolate=True)
        except Exception as e:
            # Fallback to simple plotting if smoothing fails
            logging.warning(f"Could not create smooth curve for {prop_label}, using simple plot: {e}")
            plot_axis.fill_between(sorted_timestamps, sorted_values, alpha=0.35, 
                          label=prop_label, color=color_palette[color_idx % len(color_palette)], 
                          linewidth=0, antialiased=True, interpolate=True)
        
        color_idx += 1
    
    # Format the graph
    ax.set_title(title, fontsize=16, fontweight='bold', color='white')
    ax.set_xlabel('Time', fontsize=12, color='white')
    
    # Y-axis labels are already set above for dual-axis case
    if not has_multiple_units:
        # Single axis - set y-label here
        y_label = next(iter(prop_units.values()))[1]
        ax.set_ylabel(y_label, fontsize=12, color='white')
        ax.tick_params(colors='white')
    
    ax.grid(True, alpha=0.2, color='white')
    
    # Format x-axis dates based on granularity
    if granularity == 'days':
        # For days, show date and time
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M'))
    elif granularity == 'hours':
        # For hours, show date and time
        ax.xaxis.set_major_formatter(DateFormatter('%m-%d %H:%M'))
    elif granularity == 'minutes':
        # For minutes, show time
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
    else:
        # For seconds, show time with seconds
        ax.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))
    plt.xticks(rotation=45, ha='right', color='white')
    
    if not has_multiple_units:
        plt.yticks(color='white')
    
    # Legend with white text - combine legends if dual axis
    if has_multiple_units:
        # Combine legends from both axes
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best', facecolor='#1a4d4d', edgecolor='white', labelcolor='white')
    else:
        ax.legend(loc='best', facecolor='#1a4d4d', edgecolor='white', labelcolor='white')
    
    plt.tight_layout()
    
    # Save the graph
    try:
        plt.savefig(output_path, format='png', dpi=100, bbox_inches='tight', 
                   facecolor='#1a4d4d', edgecolor='none')
        logging.info(f"Performance graph saved to {output_path}")
    except Exception as e:
        raise IOError(f"Failed to save graph to {output_path}: {e}")
    finally:
        plt.close(fig)


def list_performance_graph(
    monitor_name: str,
    cluster: str,
    timeframe: Optional[str] = None,
    instances: Optional[str] = None,
    object_name: Optional[str] = None,
    format: str = "png"
) -> Dict[str, Any]:
    """
    Generate a time-series performance graph using a predefined VAST monitor.
    
    This function creates a PNG graph showing performance metrics over time from a predefined monitor
    and returns the file path and resource URI for MCP client display. Each item in the monitor's
    prop_list becomes a separate line in the graph.
    
    Args:
        monitor_name: Name of the predefined monitor (required). Use list_monitors() to discover available monitors.
        cluster: Target cluster address or name (required)
        timeframe: Optional time frame for metrics (e.g., "5m", "1h", "24h"). 
                   If not provided, uses the monitor's default time_frame.
        instances: Optional comma-separated list of specific instance identifiers.
                   Format same as list_performance (e.g., "cnode1,cnode2" or "tenant1:viewname").
        object_name: Optional object type for validation. If provided, validates that monitor's object_type matches.
        format: Image format. Currently only "png" is supported. Defaults to "png"
        
    Returns:
        Dictionary containing:
        - resource_uri: URI to access the graph image (file:// URI)
        - file_path: Absolute file path to the graph image
        - monitor_name: The monitor name that was used
        - timeframe: The timeframe used
        - instances: List of instance names that were plotted
        
    Raises:
        ValueError: If cluster is not provided, monitor not found, object_type mismatch,
                    or if no data points are available
        ImportError: If matplotlib is not installed
        
    Examples:
        - Get graph for "Cluster SMB IOPS" monitor: list_performance_graph("Cluster SMB IOPS", "vast3115-var")
        - Get graph with custom timeframe: list_performance_graph("Cluster SMB IOPS", "vast3115-var", timeframe="1h")
        - Get graph for specific instances: list_performance_graph("Cluster SMB IOPS", "vast3115-var", 
          instances="cnode1,cnode2")
    """
    if not cluster:
        raise ValueError("cluster parameter is required")
    
    if not monitor_name:
        raise ValueError("monitor_name parameter is required")
    
    # Validate format
    if format.lower() != 'png':
        raise ValueError(f"format must be 'png'. Got: {format}")
    
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, cluster_name = resolve_cluster_identifier(cluster, config)
    default_tenant = cluster_config['tenant']
    
    # Create client
    client = create_vast_client(cluster_address)
    
    # Get monitor details
    monitor = _get_monitor_by_name(client, monitor_name)
    if not monitor:
        raise ValueError(f"Monitor '{monitor_name}' not found. Use list_monitors() to see available monitors.")
    
    monitor_id = monitor.get('id')
    monitor_object_type = monitor.get('object_type')
    monitor_prop_list = monitor.get('prop_list', [])
    monitor_time_frame = monitor.get('time_frame')
    
    if not monitor_prop_list:
        raise ValueError(f"Monitor '{monitor_name}' has no prop_list items")
    
    # Validate object_type if provided
    if object_name:
        object_name_lower = object_name.lower()
        if monitor_object_type and monitor_object_type.lower() != object_name_lower:
            logging.warning(f"Monitor object_type '{monitor_object_type}' does not match provided object_name '{object_name}'")
    
    # Use provided timeframe or monitor's default
    if timeframe:
        try:
            timeframe_in_seconds = parse_time_duration(timeframe)
        except Exception as e:
            raise ValueError(f"Invalid timeframe format '{timeframe}': {e}")
    else:
        timeframe = monitor_time_frame or "5m"
        try:
            timeframe_in_seconds = parse_time_duration(timeframe)
        except Exception as e:
            logging.warning(f"Monitor has invalid time_frame '{monitor_time_frame}', using default '5m'")
            timeframe = "5m"
            timeframe_in_seconds = parse_time_duration(timeframe)
    
    # For 'view' object type, limit timeframe to maximum of 8 hours (same as list_performance)
    object_type_for_validation = monitor_object_type or object_name
    if object_type_for_validation and object_type_for_validation.lower() == 'view':
        if timeframe_in_seconds > MAX_VIEW_TIMEFRAME_SECONDS:
            logging.warning("For 'view' object type, maximum timeframe supported is 8 hours. Adjusting timeframe to 8 hours.")
            timeframe = "8h"
            timeframe_in_seconds = MAX_VIEW_TIMEFRAME_SECONDS
    
    # Determine granularity (same as list_performance)
    # For views, don't use granularity parameter (views only support seconds resolution and no aggregation)
    if object_type_for_validation and object_type_for_validation.lower() == 'view':
        granularity = None  # Don't pass granularity for views
    else:
        granularity = _get_granularity(timeframe_in_seconds)
    
    # Get instance IDs if specified
    object_type_for_instances = monitor_object_type or object_name
    if object_type_for_instances and instances:
        instance_ids = _get_instance_ids(client, object_type_for_instances.lower(), instances, default_tenant)
    else:
        instance_ids = []
    
    # Get all instances for name resolution
    if object_type_for_instances:
        all_instances = _get_all_instances(client, object_type_for_instances.lower())
    else:
        all_instances = []
    
    logging.info(f"Preparing to generate performance graph for monitor: {monitor_name}, instances: {instances if instances else 'ALL'}, timeframe: {timeframe} on cluster: {cluster_address}")
    
    try:
        logging.info(f"Retrieving performance metrics for monitor {monitor_name} (ID: {monitor_id}) with timeframe {timeframe}")
        
        # Query performance metrics using monitor query endpoint
        whitelist = get_api_whitelist()
        
        # Build query parameters (same as list_performance)
        query_params = {}
        if timeframe:
            query_params['time_frame'] = timeframe
        # For views, don't pass granularity (views only support seconds resolution and no aggregation)
        # For other object types, pass granularity
        if granularity is not None:
            query_params['granularity'] = granularity
        if instance_ids:
            query_params['object_ids'] = instance_ids
        
        # Call monitor query endpoint: monitors.{id}.query
        endpoint = f"monitors.{monitor_id}.query"
        metrics_list = call_vast_api(
            client=client,
            endpoint=endpoint,
            method='get',
            params=query_params,
            whitelist=whitelist
        )
        
        # Extract metrics dict
        if isinstance(metrics_list, list) and len(metrics_list) == 1 and isinstance(metrics_list[0], dict):
            metrics = metrics_list[0]
        elif isinstance(metrics_list, dict):
            metrics = metrics_list
        else:
            metrics = {}
        
        if not (metrics and isinstance(metrics, dict) and 'data' in metrics and 'prop_list' in metrics):
            raise ValueError(f"No performance data available for monitor '{monitor_name}' on cluster {cluster_name}")
        
        prop_list_response = metrics['prop_list']
        data_points = metrics['data']
        object_ids = metrics.get('object_ids', [])
        
        # Build instance data mapping
        instance_data = {}
        object_type_for_instances_lower = (monitor_object_type or object_name or '').lower()
        
        for obj in all_instances:
            if object_type_for_instances_lower == 'view' and 'tenant_name' in obj:
                # For views, format as "tenant_name:view_name"
                instance_data[obj['id']] = f"{obj['tenant_name']}:{obj['name']}"
            else:
                instance_data[obj['id']] = obj['name']
        
        # Ensure graph temp directory exists
        os.makedirs(GRAPH_TEMP_DIR, exist_ok=True)
        
        # Generate unique filename
        timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        safe_cluster = cluster_name.replace('/', '_').replace('\\', '_')
        safe_monitor = monitor_name.replace('/', '_').replace('\\', '_').replace(' ', '_')
        filename = f"performance_graph_{safe_monitor}_{safe_cluster}_{timestamp_str}.png"
        file_path = os.path.join(GRAPH_TEMP_DIR, filename)
        
        # Create the graph
        _create_performance_graph(
            data_points=data_points,
            prop_list_response=prop_list_response,
            metrics_map={},  # Empty metrics_map for monitor-based graphs
            object_name=monitor_object_type or 'cluster',
            instance_data=instance_data,
            monitor_prop_list=monitor_prop_list,
            output_path=file_path,
            timeframe=timeframe,
            granularity=granularity,
            cluster_name=cluster_name
        )
        
        # Get instance names that were plotted
        plotted_instances = [instance_data.get(oid, f"Unknown-{oid}") for oid in object_ids if oid in instance_data]
        
        # Calculate statistics
        statistics = _process_performance_graph_stats(
            data_points=data_points,
            prop_list_response=prop_list_response,
            monitor_prop_list=monitor_prop_list,
            instance_data=instance_data,
            object_ids=object_ids
        )
        
        # Convert Docker container path to host path if running in Docker
        host_file_path = convert_docker_path_to_host(file_path)
        
        # Create resource URI using host path
        resource_uri = f"file://{host_file_path}"
        
        return {
            'resource_uri': resource_uri,
            'file_path': host_file_path,
            'monitor_name': monitor_name,
            'timeframe': timeframe,
            'instances': plotted_instances,
            'statistics': statistics,
            'display_note': 'Please display the graph image using the resource_uri. The graph visualizes performance metrics over time.'
        }
        
    except Exception as e:
        logging.error(f"Failed to generate performance graph for monitor '{monitor_name}' on {cluster_address}. Error: {e}")
        raise


def query_users(cluster: str, tenant: str = 'default', prefix: str = '', top: int = QUERY_USERS_DEFAULT_TOP) -> List[Dict]:
    """
    Query user names from VAST cluster using users/names endpoint.
    
    Args:
        cluster: Target cluster address or name (required)
        tenant: Tenant name to query (required, defaults to 'default')
        prefix: Prefix to filter usernames (required, must be at least 1 character)
        top: Maximum number of results to return (optional, defaults to QUERY_USERS_DEFAULT_TOP, maximum: QUERY_USERS_MAX_TOP due to API limit)
    
    Returns:
        List of user dictionaries containing:
        - login_name: Login name
        - fqdn: Fully qualified domain name
        - name: Display name
        - uid: User ID
        - gid: Leading group GID
        - group: Leading group name
        - primary group: Primary group name
        - groups: Comma-separated list of group names
        - origins: Comma-separated key:value pairs of user origins
        - s3 info: Comma-separated S3 permissions and connection info (e.g., "allow create bucket", "allow delete bucket", "superuser", "s3 connectons:N")
        - access_keys: Comma-separated list of enabled S3 access keys
        - identity policies: Comma-separated list of S3 identity policy names
    
    Raises:
        ValueError: If cluster is not provided, prefix is not provided or empty, or tenant is not found
    
    Examples:
        - Query users with prefix: query_users(cluster="vast3115-var", tenant="default", prefix="hmarko")
        - Query users with custom top limit: query_users(cluster="vast3115-var", tenant="tenant1", prefix="admin", top=50)
    """
    if not cluster:
        raise ValueError("cluster parameter is required")
    
    # Normalize prefix (strip whitespace)
    prefix = prefix.strip() if prefix else ''
    if not prefix or len(prefix) < 1:
        raise ValueError("prefix parameter is required and must be at least 1 character")
    
    # Enforce maximum top limit (API hardcoded limit)
    if top > QUERY_USERS_MAX_TOP:
        logging.warning(f"top parameter ({top}) exceeds maximum allowed ({QUERY_USERS_MAX_TOP}), limiting to {QUERY_USERS_MAX_TOP}")
        top = QUERY_USERS_MAX_TOP
    
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    
    # Create client
    client = create_vast_client(cluster_address)
    
    # Resolve tenant name to tenant_id
    whitelist = get_api_whitelist()
    tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant '{tenant}' not found.")
    
    logging.info(f"Querying users with prefix '{prefix}' in tenant '{tenant}' (ID: {tenant_id}) on cluster {cluster_address}, limit: {top}")
    
    # Query users/names endpoint
    try:
        users_list = call_vast_api(
            client=client,
            endpoint='users.names',
            method='get',
            params={
                'tenant_id': tenant_id,
                'prefix': prefix
            },
            whitelist=whitelist
        )
        
        # Limit results to top (apply limit on API response)
        if len(users_list) > top:
            users_list = users_list[:top]
            logging.info(f"Limited results to {top} users (API returned {len(users_list)} total)")
        
        # Remove unwanted fields from each user record
        fields_to_remove = ['label', 'value', 'sid_str', 'uid_or_gid', 'is_sid']
        filtered_users_list = []
        for user in users_list:
            filtered_user = {k: v for k, v in user.items() if k not in fields_to_remove}
            filtered_users_list.append(filtered_user)
        
        # for each user need to use users.query with the username=username context=aggregated and tenant_id=tenant_id to get the full user information    
        for user in filtered_users_list:
            try:
                user_api = call_vast_api(
                    client=client,
                    endpoint='users.query',
                    method='get',
                    params={'username': user.get('name') or user.get('login_name'), 'context': 'aggregated', 'tenant_id': tenant_id}
                )
                
                # Handle empty or None response
                if not user_api or len(user_api) == 0:
                    logging.warning(f"No user details found for user '{user.get('name') or user.get('login_name')}' in tenant '{tenant}' (ID: {tenant_id})")
                    # Set default values for missing fields
                    user['uid'] = None
                    user['gid'] = None
                    user['group'] = None
                    user['primary group'] = None
                    user['groups'] = ''
                    user['origins'] = ''
                    user['s3 info'] = ''
                    user['access_keys'] = ''
                    user['identity policies'] = ''
                    continue
                
                user_api = user_api[0] if isinstance(user_api, list) else user_api
                
                # Extract user details with safe defaults
                user['uid'] = user_api.get('uid', None)
                user['gid'] = user_api.get('leading_group_gid', None)
                user['group'] = user_api.get('leading_group_name', None)
                user['primary group'] = user_api.get('primary_group_name', None)
                user['groups'] = ','.join(user_api.get('groups', [])) if user_api.get('groups') else ''
                #for origin dictionary join key:value with comma separator
                origins_dict = user_api.get('origins', {})
                user['origins'] = ','.join([f"{k}:{v}" for k, v in origins_dict.items()]) if origins_dict else ''
                s3 = []
                if user_api.get('allow_create_bucket', False): s3.append('allow create bucket')
                if user_api.get('allow_delete_bucket', False): s3.append('allow delete bucket')
                if user_api.get('s3_superuser', False): s3.append('superuser')
                if user_api.get('s3_connections_count', 0): s3.append(f"s3 connectons:{user_api.get('s3_connections_count', 0)}")
                user['s3 info'] = ",".join(s3) if s3 else ''
                # Extract enabled access keys
                access_keys_list = user_api.get('access_keys', [])
                enabled_keys = [key.get('access_key') for key in access_keys_list if isinstance(key, dict) and key.get('enabled', False)]
                user['access_keys'] = ','.join(enabled_keys) if enabled_keys else ''
                # Extract identity policies
                s3_policies_list = user_api.get('s3_policies', [])
                user['identity policies'] = ','.join(s3_policies_list) if s3_policies_list else ''
            except Exception as e:
                logging.warning(f"Failed to retrieve user details for '{user.get('name') or user.get('login_name')}' in tenant '{tenant}': {e}")
                # Set default values for missing fields on error
                user['uid'] = None
                user['gid'] = None
                user['group'] = None
                user['primary group'] = None
                user['groups'] = ''
                user['origins'] = ''
                user['s3 info'] = ''
                user['access_keys'] = ''
                user['identity policies'] = ''

        logging.info(f"Retrieved {len(filtered_users_list)} users matching prefix '{prefix}'")
        return filtered_users_list
    
        
    except Exception as e:
        logging.error(f"Failed to query users with prefix '{prefix}' in tenant '{tenant}' on cluster {cluster_address}. Error: {e}")
        raise


def _generate_mcp_debug_code(command_name: str, template_parser: TemplateParser) -> str:
    """Generate Python code representation of MCP function for debugging.
    
    Args:
        command_name: Name of the command
        template_parser: Template parser instance
        
    Returns:
        Python code string representing the MCP function
    """
    args_config = template_parser.get_arguments(command_name)
    description = template_parser.get_description(command_name)
    
    # Add order and top arguments (same as in mcp_server.py)
    args_config.append({
        'name': 'order',
        'type': 'str',
        'mandatory': False,
        'description': 'Sort results by field. Format: "field_name:direction" using colon separator. IMPORTANT: Use underscores for field names (e.g., "logical_used" not "logical used"). Examples: "physical_used:desc", "logical_used:asc", "name:desc". Direction: Use a/as/asc/ascending for ascending, or d/de/desc/descending for descending. Default is asc if omitted. Multiple fields: "field1:desc,field2:asc".'
    })
    args_config.append({
        'name': 'top',
        'type': 'int',
        'mandatory': False,
        'description': 'Limit output to top N results'
    })
    
    # Build parameter definitions
    param_defs = []
    param_names = []
    for arg in args_config:
        arg_name = arg.get('name', '')
        arg_type = arg.get('type', 'str')
        arg_mandatory = arg.get('mandatory', False)
        is_filter = arg.get('filter', False)
        
        # Convert spaces to underscores for Python identifier
        python_param_name = arg_name.replace(' ', '_')
        param_names.append(python_param_name)
        
        # For int arguments with filter:true, use str type to allow filter syntax
        if arg_type == 'int' and is_filter:
            arg_type_for_sig = 'str'
        # For list type with argument_list=true, it's represented as str (comma-separated)
        elif arg_type == 'list' and arg.get('argument_list', False):
            arg_type_for_sig = 'str'
        else:
            arg_type_for_sig = arg_type
        
        # Set default value based on type
        if arg_mandatory:
            # Required parameter - no default
            if arg_type_for_sig == 'int':
                param_defs.append(f"{python_param_name}: int")
            elif arg_type_for_sig == 'bool':
                param_defs.append(f"{python_param_name}: bool")
            else:
                param_defs.append(f"{python_param_name}: str")
        else:
            # Optional parameter with default
            if arg_type_for_sig == 'int':
                param_defs.append(f"{python_param_name}: int = 0")
            elif arg_type_for_sig == 'bool':
                param_defs.append(f"{python_param_name}: bool = False")
            else:
                param_defs.append(f"{python_param_name}: str = ''")
    
    # Build function signature
    func_params = ',\n        '.join(param_defs)
    tool_name = f"list_{command_name}_vast"
    func_name = f"list_{command_name}_mcp"
    
    # Build docstring from description
    if 'Args:' in description or 'Arguments:' in description:
        # Description already has Args section - need to append order and top descriptions
        order_arg = [arg for arg in args_config if arg.get('name') == 'order']
        top_arg = [arg for arg in args_config if arg.get('name') == 'top']
        
        order_top_lines = []
        if order_arg:
            order_desc = order_arg[0].get('description', '')
            order_top_lines.append(f"        order (str) (optional): {order_desc} Defaults to empty string.")
        if top_arg:
            top_desc = top_arg[0].get('description', '')
            order_top_lines.append(f"        top (int) (optional): {top_desc} Defaults to 0.")
        
        if 'Returns:' not in description:
            docstring = description + "\n" + "\n".join(order_top_lines) + "\n\n        Returns:\n            A list of dictionaries containing the requested information."
        else:
            # Insert order/top before Returns section
            parts = description.rsplit('Returns:', 1)
            docstring = parts[0] + "\n".join(order_top_lines) + "\n        Returns:" + parts[1]
    else:
        # Build docstring with Args section
        docstring_parts = [description, "\n        Args:"]
        for arg in args_config:
            arg_name = arg.get('name')
            arg_desc = arg.get('description', '')
            arg_mandatory = arg.get('mandatory', False)
            arg_type = arg.get('type', 'str')
            # Get default value for display
            if arg_mandatory:
                default_str = ""
            else:
                if arg_type == 'int':
                    default_str = " Defaults to 0."
                elif arg_type == 'bool':
                    default_str = " Defaults to False."
                elif arg_type == 'list':
                    default_str = " Defaults to empty string."
                else:
                    default_str = " Defaults to empty string."
            docstring_parts.append(f"            {arg_name} ({arg_type}): {arg_desc}{default_str}")
        docstring_parts.append("\n        Returns:")
        docstring_parts.append("            A list of dictionaries containing the requested information.")
        docstring = "\n        ".join(docstring_parts)
    
    # Format docstring with proper indentation (8 spaces for docstring content)
    docstring_lines = docstring.split('\n')
    formatted_docstring_lines = []
    for line in docstring_lines:
        if line.strip():
            stripped = line.lstrip()
            current_indent = len(line) - len(stripped)
            if current_indent < 8:
                formatted_docstring_lines.append('        ' + stripped)
            else:
                formatted_docstring_lines.append(line)
        else:
            formatted_docstring_lines.append('        ')
    formatted_docstring = '\n'.join(formatted_docstring_lines)
    
    # Build kwargs dict for function call
    kwargs_lines = []
    for python_param_name in param_names:
        # Find the original argument name (with spaces) for the kwargs
        original_arg_name = None
        for arg in args_config:
            if arg.get('name', '').replace(' ', '_') == python_param_name:
                original_arg_name = arg.get('name', '')
                break
        if not original_arg_name:
            original_arg_name = python_param_name
        
        # Always use dictionary syntax with quotes for consistency
        kwargs_lines.append(f'                "{original_arg_name}": {python_param_name},')
    
    # Get short description for decorator (first line only)
    short_desc = description.split('\n')[0] if description else f'Retrieve {command_name} from VAST cluster'
    # Escape quotes in description
    short_desc = short_desc.replace('"', '\\"')
    
    # Generate Python code
    python_code = f'''    @mcp.tool(name="{tool_name}", description="{short_desc}")
    async def {func_name}(
        {func_params}
    ) -> list:
        """
{formatted_docstring}
        """
        try:
            from vast_admin_mcp.functions import list_dynamic
            kwargs = {{
{chr(10).join(kwargs_lines)}
            }}
            # Remove empty values to use defaults
            kwargs = {{k: v for k, v in kwargs.items() if v not in ('', 0, False)}}
            results = list_dynamic('{command_name}', **kwargs)
            return results
        except Exception as e:
            import logging
            logging.error(f"Error listing {command_name}: {{e}}")
            raise
'''
    
    return python_code


def _generate_merged_mcp_code(merged_name: str, template_parser: TemplateParser) -> str:
    """Generate Python code representation of merged MCP function for debugging.
    
    Args:
        merged_name: Name of the merged command
        template_parser: Template parser instance
        
    Returns:
        Python code string representing the merged MCP function
    """
    merged_template = template_parser.get_merged_command_template(merged_name)
    if not merged_template:
        raise ValueError(f"Merged command '{merged_name}' not found")
    
    source_functions = merged_template.get('functions', [])
    args_config = template_parser.get_merged_arguments(merged_name)
    description = template_parser.get_description(merged_name)
    
    # Add order and top arguments (same as in mcp_server.py)
    args_config.append({
        'name': 'order',
        'type': 'str',
        'mandatory': False,
        'description': 'Sort results by field. Format: "field_name:direction" using colon separator. IMPORTANT: Use underscores for field names (e.g., "logical_used" not "logical used"). Examples: "physical_used:desc", "logical_used:asc", "name:desc". Direction: Use a/as/asc/ascending for ascending, or d/de/desc/descending for descending. Default is asc if omitted. Multiple fields: "field1:desc,field2:asc".'
    })
    args_config.append({
        'name': 'top',
        'type': 'int',
        'mandatory': False,
        'description': 'Limit output to top N results'
    })
    
    # Build parameter definitions
    param_defs = []
    param_names = []
    for arg in args_config:
        arg_name = arg.get('name', '')
        arg_type = arg.get('type', 'str')
        arg_mandatory = arg.get('mandatory', False)
        is_filter = arg.get('filter', False)
        
        # Convert spaces to underscores for Python identifier
        python_param_name = arg_name.replace(' ', '_')
        param_names.append(python_param_name)
        
        # For int arguments with filter:true, use str type to allow filter syntax
        if arg_type == 'int' and is_filter:
            arg_type_for_sig = 'str'
        # For list type with argument_list=true, it's represented as str (comma-separated)
        elif arg_type == 'list' and arg.get('argument_list', False):
            arg_type_for_sig = 'str'
        else:
            arg_type_for_sig = arg_type
        
        # Set default value based on type
        if arg_mandatory:
            # Required parameter - no default
            if arg_type_for_sig == 'int':
                param_defs.append(f"{python_param_name}: int")
            elif arg_type_for_sig == 'bool':
                param_defs.append(f"{python_param_name}: bool")
            else:
                param_defs.append(f"{python_param_name}: str")
        else:
            # Optional parameter with default
            if arg_type_for_sig == 'int':
                param_defs.append(f"{python_param_name}: int = 0")
            elif arg_type_for_sig == 'bool':
                param_defs.append(f"{python_param_name}: bool = False")
            else:
                param_defs.append(f"{python_param_name}: str = ''")
    
    # Build function signature
    func_params = ',\n        '.join(param_defs)
    tool_name = f"list_{merged_name}_vast"
    func_name = f"list_{merged_name}_mcp"
    
    # Build docstring from description
    if 'Args:' in description or 'Arguments:' in description:
        # Description already has Args section - need to append order and top descriptions
        order_arg = [arg for arg in args_config if arg.get('name') == 'order']
        top_arg = [arg for arg in args_config if arg.get('name') == 'top']
        
        order_top_lines = []
        if order_arg:
            order_desc = order_arg[0].get('description', '')
            order_top_lines.append(f"        order (str) (optional): {order_desc} Defaults to empty string.")
        if top_arg:
            top_desc = top_arg[0].get('description', '')
            order_top_lines.append(f"        top (int) (optional): {top_desc} Defaults to 0.")
        
        if 'Returns:' not in description:
            docstring = description + "\n" + "\n".join(order_top_lines) + "\n\n        Returns:\n            A list of dictionaries containing the requested information."
        else:
            # Insert order/top before Returns section
            parts = description.rsplit('Returns:', 1)
            docstring = parts[0] + "\n".join(order_top_lines) + "\n        Returns:" + parts[1]
    else:
        # Build docstring with Args section
        docstring_parts = [description, "\n        Args:"]
        for arg in args_config:
            arg_name = arg.get('name')
            arg_desc = arg.get('description', '')
            arg_mandatory = arg.get('mandatory', False)
            arg_type = arg.get('type', 'str')
            # Get default value for display
            if arg_mandatory:
                default_str = ""
            else:
                if arg_type == 'int':
                    default_str = " Defaults to 0."
                elif arg_type == 'bool':
                    default_str = " Defaults to False."
                elif arg_type == 'list':
                    default_str = " Defaults to empty string."
                else:
                    default_str = " Defaults to empty string."
            docstring_parts.append(f"            {arg_name} ({arg_type}): {arg_desc}{default_str}")
        docstring_parts.append("\n        Returns:")
        docstring_parts.append("            A list of dictionaries containing the requested information.")
        docstring = "\n        ".join(docstring_parts)
    
    # Format docstring with proper indentation (8 spaces for docstring content)
    docstring_lines = docstring.split('\n')
    formatted_docstring_lines = []
    for line in docstring_lines:
        if line.strip():
            stripped = line.lstrip()
            current_indent = len(line) - len(stripped)
            if current_indent < 8:
                formatted_docstring_lines.append('        ' + stripped)
            else:
                formatted_docstring_lines.append(line)
        else:
            formatted_docstring_lines.append('        ')
    formatted_docstring = '\n'.join(formatted_docstring_lines)
    
    # Build kwargs dict for function call
    kwargs_lines = []
    for python_param_name in param_names:
        # Find the original argument name (with spaces) for the kwargs
        original_arg_name = None
        for arg in args_config:
            if arg.get('name', '').replace(' ', '_') == python_param_name:
                original_arg_name = arg.get('name', '')
                break
        if not original_arg_name:
            original_arg_name = python_param_name
        
        # Always use dictionary syntax with quotes for consistency
        kwargs_lines.append(f'                "{original_arg_name}": {python_param_name},')
    
    # Get short description for decorator (first line only)
    short_desc = description.split('\n')[0] if description else f'Retrieve merged {merged_name} from VAST cluster'
    # Escape quotes in description
    short_desc = short_desc.replace('"', '\\"')
    
    # Generate Python code
    source_funcs_str = ', '.join([f"'{f}'" for f in source_functions])
    python_code = f'''    @mcp.tool(name="{tool_name}", description="{short_desc}")
    async def {func_name}(
        {func_params}
    ) -> list:
        """
{formatted_docstring}
        """
        try:
            from vast_admin_mcp.functions import list_merged
            kwargs = {{
{chr(10).join(kwargs_lines)}
            }}
            # Remove empty values to use defaults
            kwargs = {{k: v for k, v in kwargs.items() if v not in ('', 0, False)}}
            # Merged command combines results from: [{source_funcs_str}]
            results = list_merged('{merged_name}', **kwargs)
            return results
        except Exception as e:
            import logging
            logging.error(f"Error listing merged {merged_name}: {{e}}")
            raise
'''
    
    return python_code


def list_dynamic(command_name: str, **kwargs) -> List[Dict]:
    """
    Dynamically execute a list command based on YAML template.
    
    If mcp=True in kwargs, returns debug information about the MCP tool structure.
    """
    default_template_path = get_default_template_path()
    template_parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
    
    # Check if command exists
    if not template_parser.get_command_template(command_name):
        raise ValueError(f"Command '{command_name}' not found in template file")
    
    # Handle MCP debug mode - generate Python code representation
    if kwargs.get('mcp', False):
        python_code = _generate_mcp_debug_code(command_name, template_parser)
        # Return as a special dict that will be printed as code
        return [{"_mcp_python_code": python_code}]
    
    # Get cluster(s) from kwargs
    import os
    
    # Load template file
    template_path = TEMPLATE_MODIFICATIONS_FILE
    default_template_path = get_default_template_path()
    if not os.path.exists(template_path) and not default_template_path:
        raise ValueError(f"Template modifications file not found: {template_path}")
    
    # Parse template
    parser = TemplateParser(template_path, default_template_path=default_template_path)
    
    # Get clusters from kwargs - support both 'cluster' (single) and 'clusters' (comma-separated)
    cluster_arg = kwargs.get('cluster') or kwargs.get('clusters')
    
    # Parse cluster list
    if cluster_arg:
        if isinstance(cluster_arg, str):
            cluster_list = [c.strip() for c in cluster_arg.split(',') if c.strip()]
        else:
            cluster_list = [cluster_arg]
    else:
        # Use all clusters from configuration when cluster is not provided
        config = load_config()
        cluster_list = [c['cluster'] for c in config.get('clusters', [])]
        if not cluster_list:
            raise ValueError("No clusters configured. Please run 'vast-admin-mcp setup' to configure clusters.")
    
    # Execute command for each cluster and combine results
    all_results = []
    config = load_config()
    
    # Cache for clients to reuse across cluster resolutions
    cluster_clients = {}  # cluster_address -> client (reuse clients)
    
    for cluster_arg_item in cluster_list:
        # Use shared resolution function with client cache
        try:
            cluster_address, cluster_config, cluster_name = resolve_cluster_identifier(
                cluster_arg_item, config, client_cache=cluster_clients
            )
        except ValueError as e:
            logging.error(f"Error resolving cluster {cluster_arg_item}: {e}")
            continue
        
        # Create kwargs for this cluster execution
        # Don't include 'cluster' in kwargs - the executor already has the correct cluster address
        # The cluster name will be added to results during field transformation
        cluster_kwargs = {k: v for k, v in kwargs.items() if k not in ['cluster', 'clusters', 'order', 'top']}
        
        # Create executor with cluster address (for API calls)
        # Reuse client if we already have one to avoid redundant create_vast_client calls
        cached_client = cluster_clients.get(cluster_address)
        executor = CommandExecutor(parser, cluster=cluster_address, client=cached_client)
        # Cache the client for future use if we just created it
        if executor.client and cluster_address not in cluster_clients:
            cluster_clients[cluster_address] = executor.client
        
        # Execute command for this cluster (without ordering and top limit - apply after combining all clusters)
        try:
            # Pass cluster name separately so it can be used in field transformation ($(cluster))
            # The executor already has the cluster address set, so it won't use the default
            cluster_kwargs_with_name = cluster_kwargs.copy()
            cluster_kwargs_with_name['cluster'] = cluster_name  # For $(cluster) field transformation
            
            results = executor.execute(command_name, cluster_kwargs_with_name)
            all_results.extend(results)
        except Exception as e:
            logging.error(f"Error executing dynamic command {command_name} on cluster {cluster_name}: {e}")
            # Continue with other clusters even if one fails
            continue
    
    # Apply ordering and top limit to combined results from all clusters
    if all_results:
        # Re-apply ordering if specified (on combined results from all clusters)
        # Use raw field values stored during transformation (prefixed with _raw_)
        if 'order' in kwargs and kwargs.get('order'):
            # Parse order argument
            order_arg = kwargs.get('order')
            if isinstance(order_arg, str):
                order_specs = [o.strip() for o in order_arg.split(',')]
            elif isinstance(order_arg, list):
                order_specs = order_arg
            else:
                order_specs = []
            
            # Parse order specifications using shared utility
            order_configs = []
            for order_spec in order_specs:
                order_config = parse_order_spec(order_spec, use_raw_prefix=True)
                if order_config:
                    order_configs.append(order_config)
            
            # Apply ordering using shared utility
            if order_configs:
                all_results = apply_ordering(all_results, order_configs, remove_raw_fields=True)
        
        # Apply top limit to combined results from all clusters
        if 'top' in kwargs and kwargs.get('top'):
            try:
                top = int(kwargs['top'])
                if top > 0:
                    all_results = all_results[:top]
            except (ValueError, TypeError):
                pass
    
    # Remove _raw_ fields before returning to MCP client
    for row in all_results:
        keys_to_remove = [k for k in row.keys() if k.startswith('_raw_')]
        for k in keys_to_remove:
            del row[k]
    
    return all_results


def list_merged(command_name: str, **kwargs) -> List[Dict]:
    """
    Execute a merged list command that combines results from multiple source functions.
    
    Merged commands combine outputs from multiple list commands (e.g., cnodes and dnodes)
    into a single unified result set with merged arguments and fields.
    
    If mcp=True in kwargs, returns debug information about the MCP tool structure.
    """
    default_template_path = get_default_template_path()
    template_parser = TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)
    
    # Check if merged command exists
    merged_template = template_parser.get_merged_command_template(command_name)
    if not merged_template:
        raise ValueError(f"Merged command '{command_name}' not found in template file")
    
    # Handle MCP debug mode - generate Python code representation
    if kwargs.get('mcp', False):
        python_code = _generate_merged_mcp_code(command_name, template_parser)
        # Return as a special dict that will be printed as code
        return [{"_mcp_python_code": python_code}]
    
    # Get source functions to execute
    source_functions = merged_template.get('functions', [])
    if not source_functions:
        raise ValueError(f"Merged command '{command_name}' has no source functions defined")
    
    # Get merged field set (all fields from first function, then unique from others)
    merged_field_names = template_parser.get_merged_fields(command_name)
    
    # Execute each source function and collect results
    all_results = []
    errors = []
    
    for func_name in source_functions:
        try:
            # Execute source function with same kwargs
            results = list_dynamic(func_name, **kwargs)
            all_results.extend(results)
        except Exception as e:
            error_msg = f"Error executing source function '{func_name}' for merged command '{command_name}': {e}"
            logging.error(error_msg)
            errors.append(error_msg)
            # Continue with other functions even if one fails
    
    # If all functions failed, raise an error
    if not all_results and errors:
        raise ValueError(f"All source functions failed for merged command '{command_name}': {'; '.join(errors)}")
    
    # Normalize all rows to have the same field structure
    # Missing fields from one function should be set to None
    normalized_results = []
    for row in all_results:
        normalized_row = {}
        # Add all merged fields, using row value if present, otherwise None
        for field_name in merged_field_names:
            # Try both normalized and original field name (with spaces)
            value = row.get(field_name)
            if value is None:
                # Try with spaces instead of underscores
                field_name_with_spaces = field_name.replace('_', ' ')
                value = row.get(field_name_with_spaces)
            if value is None:
                # Try original field name from row keys
                for key in row.keys():
                    if key.replace(' ', '_') == field_name or key.replace('_', ' ') == field_name:
                        value = row.get(key)
                        break
            
            normalized_row[field_name] = value
        
        # Also preserve _raw_ fields for ordering compatibility
        for key, value in row.items():
            if key.startswith('_raw_'):
                # Normalize the field name part using the same normalization function
                raw_field_name = key[5:]  # Remove '_raw_' prefix
                normalized_raw_field_name = normalize_field_name(raw_field_name, 'to_underscore')
                normalized_row[f'_raw_{normalized_raw_field_name}'] = value
        
        normalized_results.append(normalized_row)
    
    # Apply ordering and top limit to combined results (same as list_dynamic)
    if normalized_results:
        # Re-apply ordering if specified
        if 'order' in kwargs and kwargs.get('order'):
            order_arg = kwargs.get('order')
            if isinstance(order_arg, str):
                order_specs = [o.strip() for o in order_arg.split(',')]
            elif isinstance(order_arg, list):
                order_specs = order_arg
            else:
                order_specs = []
            
            # Parse order specifications using shared utility
            order_configs = []
            for order_spec in order_specs:
                order_config = parse_order_spec(order_spec, use_raw_prefix=True)
                if order_config:
                    order_configs.append(order_config)
            
            # Apply ordering using shared utility
            if order_configs:
                normalized_results = apply_ordering(normalized_results, order_configs, remove_raw_fields=True)
        
        # Apply top limit
        if 'top' in kwargs and kwargs.get('top'):
            try:
                top = int(kwargs['top'])
                if top > 0:
                    normalized_results = normalized_results[:top]
            except (ValueError, TypeError):
                pass
        
        # Remove _raw_ fields before returning to MCP client
        for row in normalized_results:
            keys_to_remove = [k for k in row.keys() if k.startswith('_raw_')]
            for k in keys_to_remove:
                del row[k]
        
        return normalized_results

