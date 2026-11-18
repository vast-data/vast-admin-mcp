"""Core business logic functions for vast-admin-mcp: create operations."""

import fnmatch
import json
import re
import uuid
import logging
import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta

from vastpy import VASTClient

from .config import load_config, CONFIG_FILE, VIEW_TEMPLATE_FILE, get_default_template_path
from .utils import (
    get_size_in_bytes, validate_path, format_simple_datetime, parse_time_duration, get_api_whitelist
)
from .client import (
    create_vast_client, get_id_by_name, resolve_cluster_identifier, call_vast_api
)
from .functions import list_dynamic, query_users
from .template_parser import TemplateParser
from .config import TEMPLATE_MODIFICATIONS_FILE, get_default_template_path


MAX_LOCAL_S3_KEYS_PER_USER = 2


def _resolve_local_provider_for_tenant(client, tenant_name: str, whitelist) -> Dict[str, Any]:
    """Find the local provider assigned to a tenant via assigned_tenants_preview."""
    providers = call_vast_api(
        client=client,
        endpoint='localproviders',
        method='get',
        whitelist=whitelist,
    )
    matches = []
    for provider in providers:
        preview = provider.get('assigned_tenants_preview') or ''
        assigned = [t.strip() for t in str(preview).split(',') if t.strip()]
        if tenant_name in assigned:
            matches.append(provider)

    if len(matches) == 1:
        return matches[0]
    if not matches:
        # Fall back to a provider named 'default' when present
        for provider in providers:
            if str(provider.get('name', '')).strip().lower() == 'default':
                return provider
        raise ValueError(
            f"No local provider is assigned to tenant '{tenant_name}'. "
            f"Pass provider explicitly."
        )
    names = ', '.join(str(p.get('name')) for p in matches)
    raise ValueError(
        f"Multiple local providers are assigned to tenant '{tenant_name}': {names}. "
        f"Pass provider explicitly."
    )


def _resolve_provider(client, provider: Optional[str], tenant_name: str, whitelist) -> Dict[str, Any]:
    """Resolve provider by name/id, or the tenant's assigned local provider."""
    if provider is None or not str(provider).strip():
        return _resolve_local_provider_for_tenant(client, tenant_name, whitelist)

    token = str(provider).strip()
    providers = call_vast_api(
        client=client,
        endpoint='localproviders',
        method='get',
        whitelist=whitelist,
    )
    if token.isdigit():
        for p in providers:
            if str(p.get('id')) == token:
                return p
        raise ValueError(f"Local provider id '{token}' not found")

    matches = [p for p in providers if str(p.get('name', '')).strip() == token]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"Local provider '{token}' not found")
    raise ValueError(f"Ambiguous local provider name '{token}'")


def _exact_query_user_match(users: List[Dict[str, Any]], user_name: str) -> Dict[str, Any]:
    """Pick an exact name/login_name match from query_users results."""
    needle = user_name.strip()
    matches = []
    for user in users:
        names = {
            str(user.get('name') or '').strip(),
            str(user.get('login_name') or '').strip(),
        }
        if needle in names:
            matches.append(user)
    if not matches:
        raise ValueError(f"User '{needle}' not found via query_users")
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous user '{needle}': query_users returned {len(matches)} exact matches"
        )
    return matches[0]


def _count_enabled_access_keys(user: Dict[str, Any]) -> int:
    raw = user.get('access_keys') or ''
    if isinstance(raw, list):
        return len([
            k for k in raw
            if (isinstance(k, dict) and k.get('enabled', True) and k.get('access_key'))
            or (isinstance(k, str) and k.strip())
        ])
    return len([k for k in str(raw).split(',') if k.strip()])



def get_user_paths(
    cluster: Optional[str] = None,
    tenant: Optional[str] = None,
    view_path: Optional[str] = None
) -> List[Dict[str, str]]:
    """Get user-facing access paths for a view based on protocols."""
    config = load_config()
    if not cluster:
        cluster = config['clusters'][0]['cluster']
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    if not tenant:
        tenant = cluster_config.get('tenant', 'default')

    if not view_path:
        raise ValueError("view_path is required")
    
    client = create_vast_client(cluster_address)
    
    # Get API whitelist
    whitelist = get_api_whitelist()
    
    # Get tenant ID
    tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant {tenant} not found on cluster {cluster_address}")
    
    # Get view details
    view = call_vast_api(
        client=client,
        endpoint='views',
        method='get',
        params={'path': view_path},
        tenant_id=tenant_id,
        whitelist=whitelist
    )
    if not view or not len(view):
        raise ValueError(f"View {view_path} not found for tenant {tenant} on cluster {cluster_address}")
    
    view = view[0]
    protocols = view.get('protocols', [])
    share = view.get('share', '')
    bucket = view.get('bucket', '')
    policy_id = view.get('policy_id')
    
    # Get VIP pool names from view policy
    vip_pool_names = []
    if policy_id:
        try:
            policy = call_vast_api(
                client=client,
                endpoint='viewpolicies',
                method='get',
                params={'id': policy_id},
                whitelist=whitelist
            )
            if policy and len(policy):
                policy = policy[0]
                # Check if policy has vip_pools field (list of pool names)
                if 'vip_pools' in policy and policy['vip_pools']:
                    vip_pool_names = policy['vip_pools']
        except Exception as e:
            logging.debug(f"Could not retrieve VIP pools from policy: {e}")
    
    # If no VIP pools in policy, get from tenant
    if not vip_pool_names:
        try:
            tenant_obj = call_vast_api(
                client=client,
                endpoint='tenants',
                method='get',
                params={'id': tenant_id},
                whitelist=whitelist
            )
            if tenant_obj and len(tenant_obj):
                tenant_obj = tenant_obj[0]
                # Try vippool_names first (list of strings), then vippools (list of objects)
                if 'vippool_names' in tenant_obj and tenant_obj['vippool_names']:
                    vip_pool_names = tenant_obj['vippool_names']
                elif 'vippools' in tenant_obj and tenant_obj['vippools']:
                    vip_pool_names = [vp['name'] for vp in tenant_obj['vippools']]
        except Exception as e:
            logging.debug(f"Could not retrieve VIP pools from tenant: {e}")
    
    # If no VIP pools found, return empty list (don't fall back to all cluster VIP pools)
    if not vip_pool_names:
        logging.warning(f"No VIP pools configured for view {view_path} on tenant {tenant}. Check policy or tenant configuration.")
        return []
    
    # Get DNS domain suffix from cluster DNS configuration
    dns_domain = ''
    try:
        dns_config = call_vast_api(
            client=client,
            endpoint='dns',
            method='get',
            whitelist=whitelist
        )
        if dns_config and len(dns_config):
            dns_domain = dns_config[0].get('domain_suffix', '')
    except Exception as e:
        logging.debug(f"Could not retrieve DNS domain: {e}")
    
    # Normalize protocols to avoid duplicates (e.g., NFS and NFS4 should generate one path)
    normalized_protocols = set()
    for proto in protocols:
        if proto.startswith('NFS'):
            normalized_protocols.add('NFS')
        else:
            normalized_protocols.add(proto)
    
    # Build client paths
    client_paths = []
    for protocol in normalized_protocols:
        path_info = {'protocol': protocol}
        
        if protocol == 'NFS':
            # NFS path: <vip_pool>:<view_path>
            for vip_pool in vip_pool_names:
                if dns_domain:
                    nfs_path = f"{vip_pool}.{dns_domain}:{view_path}"
                else:
                    nfs_path = f"{vip_pool}:{view_path}"
                path_info['path'] = nfs_path
                client_paths.append(path_info.copy())
        elif protocol == 'SMB':
            # SMB path: \\<vip_pool>\<share>
            if share:
                for vip_pool in vip_pool_names:
                    if dns_domain:
                        smb_path = f"\\\\{vip_pool}.{dns_domain}\\{share}"
                    else:
                        smb_path = f"\\\\{vip_pool}\\{share}"
                    path_info['path'] = smb_path
                    client_paths.append(path_info.copy())
        elif protocol in ['S3', 'ENDPOINT']:
            # S3 path: https://<bucket>.s3.<vip_pool> or https://<vip_pool>/<bucket>
            if bucket:
                for vip_pool in vip_pool_names:
                    if dns_domain:
                        s3_path = f"https://{bucket}.s3.{vip_pool}.{dns_domain}"
                    else:
                        s3_path = f"https://{bucket}.s3.{vip_pool}"
                    path_info['path'] = s3_path
                    client_paths.append(path_info.copy())
    
    return client_paths


def _parse_ip_ranges(ip_ranges: Any, arg_name: str = 'ip_ranges') -> List[List[str]]:
    """Parse IP ranges from list pairs or a comma-separated start-end string.

    Accepts:
      - [["1.1.1.1", "1.1.1.10"], ...]
      - "1.1.1.1-1.1.1.10,2.2.2.2-2.2.2.2"
      - "1.1.1.1,2.2.2.2" (single IPs become [ip, ip])
    """
    if ip_ranges is None:
        raise ValueError(f"{arg_name} is required")

    if isinstance(ip_ranges, list):
        parsed: List[List[str]] = []
        for item in ip_ranges:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                start, end = str(item[0]).strip(), str(item[1]).strip()
                if not start or not end:
                    raise ValueError(f"Invalid {arg_name} pair: {item}")
                parsed.append([start, end])
            else:
                raise ValueError(f"Invalid {arg_name} entry: {item}. Expected [start, end] pairs.")
        if not parsed:
            raise ValueError(f"{arg_name} must contain at least one [start, end] pair")
        return parsed

    if isinstance(ip_ranges, str):
        text = ip_ranges.strip()
        if not text:
            raise ValueError(f"{arg_name} is required")
        parsed = []
        for part in text.split(','):
            part = part.strip()
            if not part:
                continue
            if '-' in part:
                start, end = [p.strip() for p in part.split('-', 1)]
            else:
                start = end = part
            if not start or not end:
                raise ValueError(f"Invalid {arg_name} entry: '{part}'")
            parsed.append([start, end])
        if not parsed:
            raise ValueError(f"{arg_name} must contain at least one IP or start-end range")
        return parsed

    raise ValueError(f"{arg_name} must be a list of [start, end] pairs or a comma-separated string")


def _resolve_vippool_ids(client, vip_pools: Any, whitelist) -> List[int]:
    """Resolve comma-separated VIP pool names/ids to integer ids."""
    if vip_pools is None:
        return []
    if isinstance(vip_pools, list):
        tokens = [str(v).strip() for v in vip_pools if str(v).strip()]
    else:
        tokens = [p.strip() for p in str(vip_pools).split(',') if p.strip()]

    ids: List[int] = []
    for token in tokens:
        if token.isdigit():
            ids.append(int(token))
            continue
        pool_id = get_id_by_name(client, 'vippools', token, whitelist=whitelist)
        if not pool_id:
            raise ValueError(f"VIP pool '{token}' not found")
        ids.append(int(pool_id))
    return ids


def create_view(
    cluster: str,
    tenant: str = 'default',
    path: Optional[str] = None,
    hard_quota: Optional[str] = None,
    protocols: Optional[str] = None,
    bucket: Optional[str] = None,
    share: Optional[str] = None,
    policy: Optional[str] = None,
    bucket_owner: Optional[str] = None,
    qos_policy: Optional[str] = None,
    kafka_vip_pools: Optional[str] = None,
    name: Optional[str] = None,
) -> List[Dict[str, str]]:
    """Create a view in a VAST cluster. Provide cluster and path at minimum.

    Args:
        cluster: Cluster address or name. Required.
        tenant: Tenant name that owns the view. Defaults to 'default' if not provided.
        path: View path (e.g., /s3/mybucket, /nfs/myshare). Required.
        protocols: Comma seperated list of protocols to enable (e.g., NFS,S3,SMB,ENDPOINT,DATABASE,KAFKA). if not specified, NFS will be used. when S3 or ENDPOINT is specified, bucket and bucket_owner must be provided. when SMB is specified, share must be provided. DATABASE requires S3. KAFKA requires kafka_vip_pools. (protocols are case-insensitive)
        bucket: Bucket name for S3 protocol. Must be provided if S3 or ENDPOINT protocol is requested.
        bucket_owner: Bucket owner name for S3 protocol. Must be provided if S3 or ENDPOINT protocol is enabled.
        share: Share name for NFS/SMB protocol. Must be provided if SMB is in the protocols list.
        policy: View policy name.
        hard_quota: Hard quota for the view (e.g., 10GB, 100GB, 1TB).
        qos_policy: QoS policy name that can help manage performance for the view.
        kafka_vip_pools: Comma-separated VIP pool names or ids. Required when KAFKA is in protocols.
        name: Optional view name. Defaults to path basename when omitted.
    
    Returns:
        A list of client paths for the new view for the specified protocols. each item in the list will be a dictionary containing protocol and client path.
    """
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    
    # Default tenant to 'default' if not provided
    if not tenant:
        tenant = 'default'

    # Try to use default tenant policy if not specified
    if tenant == 'default' and not policy:
        policy = 'default'
    if tenant != 'default' and not policy:
        policy = tenant + '__default_policy'

    # Validate path structure
    if not path:
        raise ValueError("path is required for view creation")
    validate_path(path)
    
    client = create_vast_client(cluster_address)
    
    # Get API whitelist
    whitelist = get_api_whitelist()
    
    tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant {tenant} not found on cluster {cluster_address}.")
    
    if not protocols:
        protocols = 'NFS'
    proto_list = [p.strip().upper() for p in protocols.split(',') if p and p.strip()]

    if 'DATABASE' in proto_list and 'S3' not in proto_list:
        raise ValueError("The DATABASE protocol can only be provided together with the S3 protocol")
    if 'KAFKA' in proto_list and not kafka_vip_pools:
        raise ValueError("kafka_vip_pools is required when KAFKA protocol is enabled")
    if ('S3' in proto_list or 'ENDPOINT' in proto_list) and (not bucket or not bucket_owner):
        raise ValueError("bucket and bucket_owner are required when S3 or ENDPOINT protocol is enabled")
    # Check if bucket is specified and bucket owner is provided for S3 or ENDPOINT protocols 
    if bucket and (not bucket_owner or ('S3' not in proto_list and 'ENDPOINT' not in proto_list)):
        raise ValueError(f"Bucket {bucket} is specified, but bucket owner or S3 protocol is not provided.")
    if share and 'SMB' not in proto_list:
        raise ValueError(f"Share {share} is specified, but SMB protocol is not provided.")
    
    hard_quota_bytes = None
    if hard_quota:
        hard_quota_bytes = get_size_in_bytes(hard_quota)

    # Get policy id by name
    policy_id = get_id_by_name(client, 'viewpolicies', policy, whitelist=whitelist)
    if not policy_id:
        raise ValueError(f"Policy {policy} not found on cluster {cluster_address}.")
    
    payload: Dict[str, Any] = {
        'tenant_id': tenant_id,
        'path': path,
        'create_dir': True,
        'protocols': proto_list,
        'policy_id': policy_id,
    }
    if name:
        payload['name'] = name
    
    qos_policy_id = get_id_by_name(client, 'qospolicies', qos_policy, whitelist=whitelist) if qos_policy else None
    if qos_policy and not qos_policy_id:
        raise ValueError(f"QoS Policy {qos_policy} not found on cluster {cluster_address}.")
    if qos_policy_id:
        payload['qos_policy_id'] = qos_policy_id
    if share:
        payload['share'] = share
    if bucket:
        payload['bucket'] = bucket
        payload['bucket_owner'] = bucket_owner
    if kafka_vip_pools:
        payload['kafka_vip_pools'] = _resolve_vippool_ids(client, kafka_vip_pools, whitelist)
    
    try:
        logging.info(f"Creating view on cluster={cluster_address} tenant={tenant}, path={path} policy: {policy} {'Hard quota: ' + hard_quota if hard_quota else ''}")

        # Create the view
        created = call_vast_api(
            client=client,
            endpoint='views',
            method='post',
            params=payload,
            whitelist=whitelist
        )
        if hard_quota_bytes:
            logging.info(f"Hard quota set to: {hard_quota}")
            call_vast_api(
                client=client,
                endpoint='quotas',
                method='post',
                params={
                    'path': path,
                    'name': path[1:].replace('/', '_'),
                    'tenant_id': tenant_id,
                    'hard_limit': hard_quota_bytes
                },
                whitelist=whitelist
            )
        logging.info("View created successfully")
    except Exception as e:
        logging.error(f"Failed to create view on {cluster_address}. Error: {e}")
        raise
    
    try:
        client_paths = get_user_paths(cluster=cluster_address, tenant=tenant, view_path=path)
    except Exception as e:
        logging.error(f"Failed to get client paths for the new view {path}. Error: {e}")
        raise
    return client_paths


def create_view_from_template(
    template: str,
    count: int = 1,
    view_template_file: Optional[str] = None
) -> List[Dict[str, str]]:
    """Create a view in a VAST cluster based on a predefined template. templates are defined in the view templates file.

    Args:
        template: Template name defined in the view templates file.
        count: Number of views to create from the template. Defaults to 1.
    
    Returns:
        A list of client paths for the new view for the specified protocols. each item in the list will be a dictionary containing protocol and client path.
    """
    logging.info(f"Creating {count} view(s) from template '{template}'")
    
    # Default template file location
    if not view_template_file:
        view_template_file = VIEW_TEMPLATE_FILE
    
    # Load view templates from file
    try:
        if not os.path.exists(view_template_file):
            raise FileNotFoundError(f"View template file not found: {view_template_file}")
        with open(view_template_file, 'r') as f:
            templates = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load view templates from {view_template_file}. Error: {e}")
        raise

    # Look for the specified template within the loaded templates
    template_info = [t for t in templates if t.get('name') == template]
    if not len(template_info):
        raise ValueError(f"Template {template} not found in {view_template_file}.")
    template_info = template_info[0]

    # Validate template parameters
    required_params = ['cluster', 'tenant', 'path_prefix', 'view_policy', 'hard_quota', 'protocols']
    for param in required_params:
        if param not in template_info:
            raise ValueError(f"Missing required template parameter: {param} for template {template} in file {view_template_file}.")
    
    config = load_config()
    if template_info['cluster'] not in [c['cluster'] for c in config['clusters']]:
        raise ValueError(f"Cluster {template_info['cluster']} in template {template} not found in cluster config file: {CONFIG_FILE}")
    
    # List current views using list_dynamic
    current_views = list_dynamic(
        command_name='views',
        cluster=template_info['cluster'],
        tenant=template_info['tenant'],
        view=template_info['path_prefix'] + '*'
    )
    created_views = []

    # Find the next available index based on existing views
    existing_indices = []
    for v in current_views:
        view_path = v.get('Path') or v.get('path', '')
        match = re.match(rf"{re.escape(template_info['path_prefix'])}(\d+)$", view_path)
        if match:
            existing_indices.append(int(match.group(1)))
    
    start = 1
    if existing_indices:
        start = max(existing_indices) + 1
    
    for i in range(start, count + start):
        # Customize template parameters if needed (e.g., append index to path)
        view_params = template_info.copy()
        view_params['path'] = f"{template_info['path_prefix']}{i}"
        try:
            created_view = create_view(
                cluster=view_params.get('cluster'),
                tenant=view_params.get('tenant') if view_params.get('tenant') else '',
                path=view_params.get('path') if view_params.get('path') else '',
                hard_quota=view_params.get('hard_quota') if view_params.get('hard_quota') else None,
                protocols=view_params.get('protocols') if view_params.get('protocols') else None,
                bucket=f"{view_params.get('bucket_prefix')}{i}" if view_params.get('bucket_prefix') else None,
                share=f"{view_params.get('share_prefix')}{i}" if view_params.get('share_prefix') else None,
                policy=view_params.get('policy') if view_params.get('policy') else None,
                bucket_owner=view_params.get('bucket_owner') if view_params.get('bucket_owner') else None,
                kafka_vip_pools=view_params.get('kafka_vip_pools') if view_params.get('kafka_vip_pools') else None,
                name=view_params.get('name') if view_params.get('name') else None,
            )
            created_views.extend(created_view)
        except Exception as e:
            logging.error(f"Failed to create view from template {template} instance {i}. Error: {e}")
            raise
    return created_views


def create_snapshot(
    cluster: str,
    tenant: str = 'default',
    path: Optional[str] = None,
    snapshot_name: Optional[str] = None,
    expiry_time: Optional[str] = None,
    indestructible: bool = False,
    create_with_timestamp: bool = False
) -> Dict[str, Any]:
    """Create a snapshot for a view in a VAST cluster.

    Args:
        cluster: Cluster address or name. Required.
        tenant: Tenant name that owns the view. Defaults to 'default' if not provided.
        path: View path to snapshot (e.g., /nfs/myshare). Required.
        snapshot_name: Name for the snapshot. Required.
        expiry_time: Expiry time (e.g., 2d, 3w, 1d6h, 30m). Optional.
        indestructible: Whether to make the snapshot indestructible. Defaults to False.
        create_with_timestamp: Whether to append a timestamp to the snapshot name. Defaults to False.
    
    Returns:
        Snapshot creation details including cluster, tenant, path, snapshot name, and expiry information.
    """
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    
    # Default tenant to 'default' if not provided
    if not tenant:
        tenant = 'default'

    # Get API whitelist
    whitelist = get_api_whitelist()

    # Validate required parameters
    if not path:
        raise ValueError("Path is required for snapshot creation")
    if not snapshot_name:
        raise ValueError("Snapshot name is required")

    # Generate timestamped name if requested
    if create_with_timestamp:
        # Get current UTC time and format as _YYYY-MM-DD_HH_MM_SS_UTC
        timestamp = datetime.now(timezone.utc).strftime("_%Y-%m-%d_%H_%M_%S_UTC")
        snapshot_name = f"{snapshot_name}{timestamp}"

    # Validate path structure
    validate_path(path)
    client = create_vast_client(cluster_address)
    tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant {tenant} not found on cluster {cluster_address}.")

    # Calculate expiration time if provided
    expiration_time = None
    if expiry_time:
        try:
            expiry_seconds = parse_time_duration(expiry_time)

            # Calculate expiration timestamp (current time + expiry duration)
            expiration_time = datetime.now(timezone.utc) + timedelta(seconds=expiry_seconds)
            expiration_time = expiration_time.isoformat().replace('+00:00', 'Z')
        except Exception as e:
            raise ValueError(f"Invalid expiry time format '{expiry_time}': {e}")

    # Prepare payload for snapshot creation
    payload: Dict[str, Any] = {
        'path': path,
        'name': snapshot_name,
        'tenant_id': tenant_id
    }
    if expiration_time:
        payload['expiration_time'] = expiration_time
    if indestructible:
        payload['indestructible'] = True
    
    try:
        logging.info(f"Creating snapshot '{snapshot_name}' for path '{path}' on cluster {cluster_address} tenant {tenant}")
        if expiry_time:
            logging.info(f"Snapshot will expire in: {expiry_time} ({format_simple_datetime(expiration_time)})")
        if indestructible:
            logging.info("Snapshot will be created as indestructible")

        # Create the snapshot
        result = call_vast_api(
            client=client,
            endpoint='snapshots',
            method='post',
            params=payload,
            whitelist=whitelist
        )
        logging.info(f"Snapshot '{snapshot_name}' created successfully")
        return {
            'cluster': cluster_address,
            'tenant': tenant,
            'path': path,
            'snapshot_name': snapshot_name,
            'expiry_time': expiry_time,
            'indestructible': indestructible,
            'result': result
        }
    except Exception as e:
        logging.error(f"Failed to create snapshot '{snapshot_name}' on {cluster_address}. Error: {e}")
        raise


def create_clone(
    cluster: str,
    source_tenant: str = 'default',
    source_path: Optional[str] = None,
    source_snapshot: Optional[str] = None,
    destination_tenant: Optional[str] = None,
    destination_path: Optional[str] = None,
    refresh: bool = False
) -> List[Dict[str, str]]:
    """Create a clone from a snapshot in a VAST cluster.

    Args:
        cluster: Cluster address or name. Required.
        source_tenant: Source tenant name. Defaults to 'default' if not provided.
        source_path: Source view path to clone from. Required.
        source_snapshot: Source snapshot name (use * suffix for newest with prefix, when doing this you don't need to look for snapshots before cloning, if you use just * it will give you the newest snapshot). Required.
        destination_tenant: Destination tenant name (defaults to source tenant).
        destination_path: Destination path for the clone. Required.
        refresh: Whether to destroy existing clone before creating new one. Defaults to False, if there is a view configrued at the destination path it will remain and will be linked to the new clone after creation.
    
    Returns:
        when empty list is returned, clone was created successfully but no view exists at the destination path.
        when a list with one item is returned, the item will be a dictionary containing access paths for the view linked to the clone per protocol.
    """
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    
    # Default source tenant to 'default' if not provided
    if not source_tenant:
        source_tenant = 'default'

    # If destination tenant is not specified, use source tenant
    if not destination_tenant:
        destination_tenant = source_tenant

    # Validate required parameters
    if not source_path:
        raise ValueError("Source path is required for clone creation")
    if not source_snapshot:
        raise ValueError("Source snapshot name is required")
    if not destination_path:
        raise ValueError("Destination path is required for clone creation")
    if source_path == '/' or destination_path == '/':
        raise ValueError("Source/Destination path cannot be root '/'")

    # Validate path structures
    validate_path(source_path)
    validate_path(destination_path)
    client = create_vast_client(cluster_address)

    # Get API whitelist
    whitelist = get_api_whitelist()

    # Get tenant IDs
    source_tenant_id = get_id_by_name(client, 'tenants', source_tenant, whitelist=whitelist)
    if not source_tenant_id:
        raise ValueError(f"Source tenant {source_tenant} not found on cluster {cluster_address}.")
    destination_tenant_id = get_id_by_name(client, 'tenants', destination_tenant, whitelist=whitelist)
    if not destination_tenant_id:
        raise ValueError(f"Destination tenant {destination_tenant} not found on cluster {cluster_address}.")

    # Handle snapshot name with wildcard (find newest snapshot with prefix)
    snapshots = list_dynamic(
        command_name='snapshots',
        cluster=cluster_address,
        tenant=source_tenant,
        path=source_path,
        snapshot_name=source_snapshot
    )
    if not len(snapshots):
        raise ValueError(f"No matching snapshot found for path {source_path} on tenant {source_tenant}.")
    
    # Get snapshot name from result (could be 'Snapshot Name' or 'snapshot_name' or 'name')
    actual_snapshot_name = None
    for key in ['Snapshot Name', 'snapshot_name', 'name', 'Name']:
        if key in snapshots[0]:
            actual_snapshot_name = snapshots[0][key]
            break
    
    if not actual_snapshot_name:
        raise ValueError(f"Could not determine snapshot name from results")

    # API expects paths to end with /
    source_path += '/'
    destination_path += '/'

    # Check if destination already exists and handle refresh
    refresh_performed = False
    if refresh:
        try:
            # Check if destination is a clone
            existing_clone = call_vast_api(
                client=client,
                endpoint='globalsnapstreams',
                method='get',
                params={'loanee_root_path': destination_path},
                whitelist=whitelist
            )
            if len(existing_clone):
                existing_clone = existing_clone[0]
                if existing_clone['loanee_tenant']['name'] != destination_tenant:
                    raise ValueError(f"Existing clone at {destination_path} belongs to tenant {existing_clone['loanee_tenant']['name']}. Cannot refresh.")
                if existing_clone['owner_tenant']['name'] != source_tenant or existing_clone['source_path'] != source_path:
                    raise ValueError(f"Existing clone at {destination_path} is based on: {existing_clone['owner_tenant']['name']}:{existing_clone['source_path']}. Cannot refresh.")
                else:
                    if existing_clone['state'] != 'Completed':
                        logging.info(f"Stopping existing clone synchronization to be able to delete it.")
                        # Note: stop.patch() and delete() are sub-endpoints that call_vast_api doesn't support
                        # We validate whitelist manually for these special cases
                        if whitelist is not None:
                            if 'globalsnapstreams' not in whitelist:
                                raise ValueError(
                                    f"Access denied: API endpoint 'globalsnapstreams' is not whitelisted. "
                                    f"Please contact your administrator to add it to the api_whitelist section."
                                )
                            allowed_methods = whitelist.get('globalsnapstreams', [])
                            if allowed_methods and 'patch' not in allowed_methods:
                                raise ValueError(
                                    f"Access denied: HTTP method 'PATCH' is not allowed for endpoint 'globalsnapstreams'."
                                )
                        client.globalsnapstreams[existing_clone['id']].stop.patch()
                    logging.info(f"Deleting existing clone at {destination_path}")
                    if whitelist is not None:
                        allowed_methods = whitelist.get('globalsnapstreams', [])
                        if allowed_methods and 'delete' not in allowed_methods:
                            raise ValueError(
                                f"Access denied: HTTP method 'DELETE' is not allowed for endpoint 'globalsnapstreams'."
                            )
                    client.globalsnapstreams[existing_clone['id']].delete(remove_dir=True)
                    refresh_performed = True
        except Exception as e:
            # If getting clones fails, it might not exist, which is fine
            raise ValueError(f"Delete of {destination_path} path failed. Error: {e}")

    # Get snapshot id for the actual snapshot name
    try:
        snapshot_info = call_vast_api(
            client=client,
            endpoint='snapshots',
            method='get',
            params={
                'name': actual_snapshot_name,
                'path': source_path,
                'tenant_id': source_tenant_id
            },
            whitelist=whitelist
        )
        if not len(snapshot_info):
            raise ValueError(f"Snapshot {actual_snapshot_name} not found on tenant {source_tenant}.")
        snapshot_id = snapshot_info[0]['id']
    except Exception as e:
        logging.error(f"Failed to retrieve snapshot info for {actual_snapshot_name} on {source_tenant}. Error: {e}")
        raise
    
    try:
        logging.info(f"Creating local clone on cluster: {cluster_address} clone from: '{source_tenant}:{source_path}' snapshot: {actual_snapshot_name}' to '{destination_tenant}:{destination_path}'")
        payload = {
            'loanee_root_path': destination_path,
            'loanee_tenant_id': destination_tenant_id,
            'name': f"clone_of_{source_path.replace('/', '_')}{uuid.uuid4().hex[:8]}",
            'enabled': True
        }

        # Create the clone using the filesystem clone API
        # Note: snapshots[snapshot_id].clone.post() is a sub-endpoint that call_vast_api doesn't support
        # We validate whitelist manually for this special case
        if whitelist is not None:
            # Check if snapshots endpoint is whitelisted for POST
            if 'snapshots' not in whitelist:
                raise ValueError(
                    f"Access denied: API endpoint 'snapshots' is not whitelisted. "
                    f"Please contact your administrator to add it to the api_whitelist section "
                    f"in the YAML configuration file."
                )
            allowed_methods = whitelist.get('snapshots', [])
            if allowed_methods and 'post' not in allowed_methods:
                raise ValueError(
                    f"Access denied: HTTP method 'POST' is not allowed for endpoint 'snapshots'. "
                    f"Allowed methods: {allowed_methods}"
                )
        result = client.snapshots[snapshot_id].clone.post(**payload)

        # Check if view exists on the view path
        view_info = list_dynamic(
            command_name='views',
            cluster=cluster_address,
            tenant=destination_tenant,
            view=destination_path.rstrip('/')  # Remove trailing slash for view lookup
        )
        if not len(view_info):
            logging.info(f"Clone created successfully, no view exists on destination path {destination_path}")
            return []
        else:
            client_paths = get_user_paths(cluster=cluster_address, tenant=destination_tenant, view_path=destination_path.rstrip('/'))
            logging.info(f"Clone created successfully, view already exists at {destination_path} and linked to the clone")
            return client_paths
    except Exception as e:
        logging.error(f"Failed to create clone on {cluster_address}. Error: {e}")
        raise


def create_quota(
    cluster: str,
    tenant: str = 'default',
    path: Optional[str] = None,
    hard_limit: Optional[str] = None,
    soft_limit: Optional[str] = None,
    files_hard_limit: Optional[int] = None,
    files_soft_limit: Optional[int] = None,
    grace_period: Optional[int] = None
) -> Dict[str, Any]:
    """Use this tool to create or update quota for a specific path and tenant on a VAST cluster. This operation requires read-write mode.

    Args:
        cluster: Cluster address or name. Required.
        tenant: Tenant name. Defaults to 'default' if not provided.
        path: View path to set quota for. Required.
        hard_limit: Hard quota limit (e.g., '10GB', '1TB'). If not specified, quota is unlimited.
        soft_limit: Soft quota limit (e.g., '8GB', '800GB'). If not specified, quota is unlimited.
        files_hard_limit: Hard limit for number of files. If not specified, unlimited.
        files_soft_limit: Soft limit for number of files. If not specified, unlimited.
        grace_period: Grace period in seconds for soft limit. If not specified, uses default.
    
    Returns:
        A dictionary containing the created/updated quota information including Cluster, Tenant, Path, Name, Hard Limit, Soft Limit, Files Hard Limit, Files Soft Limit, and Grace Period.
    """
    config = load_config()
    
    # Use shared resolution function
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)
    
    # Default tenant to 'default' if not provided
    if not tenant:
        tenant = 'default'
    
    if not path:
        raise ValueError("Path is required to create quota.")

    # Validate path structure
    validate_path(path)
    client = create_vast_client(cluster_address)

    # Get API whitelist
    whitelist = get_api_whitelist()

    # Get tenant ID
    tenant_id = get_id_by_name(client, 'tenants', tenant, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant {tenant} not found on cluster {cluster_address}.")

    # Prepare quota payload
    quota_name = path[1:].replace('/', '_')
    payload = {
        'path': path,
        'name': quota_name,
        'tenant_id': tenant_id
    }

    # Convert size strings to bytes
    if hard_limit:
        payload['hard_limit'] = get_size_in_bytes(hard_limit)
    if soft_limit:
        payload['soft_limit'] = get_size_in_bytes(soft_limit)
    if files_hard_limit is not None:
        payload['inodes_hard_limit'] = files_hard_limit
    if files_soft_limit is not None:
        payload['inodes_soft_limit'] = files_soft_limit
    if grace_period is not None:
        payload['grace_period'] = grace_period
    
    logging.info(f"Creating/updating quota on cluster: {cluster_address}, tenant: {tenant}, path: {path}")
    if hard_limit:
        logging.info(f"Hard limit: {hard_limit}")
    if soft_limit:
        logging.info(f"Soft limit: {soft_limit}")
    if files_hard_limit is not None:
        logging.info(f"Files hard limit: {files_hard_limit}")
    if files_soft_limit is not None:
        logging.info(f"Files soft limit: {files_soft_limit}")
    
    try:
        # Check if quota already exists
        existing_quotas = call_vast_api(
            client=client,
            endpoint='quotas',
            method='get',
            params={'path': path},
            tenant_id=tenant_id,
            whitelist=whitelist
        )
        if existing_quotas:
            # Update existing quota
            quota_id = existing_quotas[0]['id']
            logging.info(f"Updating existing quota (ID: {quota_id})")
            # Note: quotas[quota_id].patch() is a sub-endpoint that call_vast_api doesn't support
            # We validate whitelist manually for this special case
            if whitelist is not None:
                if 'quotas' not in whitelist:
                    raise ValueError(
                        f"Access denied: API endpoint 'quotas' is not whitelisted. "
                        f"Please contact your administrator to add it to the api_whitelist section."
                    )
                allowed_methods = whitelist.get('quotas', [])
                if allowed_methods and 'patch' not in allowed_methods:
                    raise ValueError(
                        f"Access denied: HTTP method 'PATCH' is not allowed for endpoint 'quotas'."
                    )
            updated = client.quotas[quota_id].patch(**payload)
            result = updated[0] if isinstance(updated, list) else updated
        else:
            # Create new quota
            logging.info("Creating new quota")
            created = call_vast_api(
                client=client,
                endpoint='quotas',
                method='post',
                params=payload,
                whitelist=whitelist
            )
            result = created[0] if isinstance(created, list) else created
        
        logging.info(f"Quota {'updated' if existing_quotas else 'created'} successfully")
        
        from .utils import pretty_size
        return {
            'Cluster': cluster_address,
            'Tenant': tenant,
            'Path': path,
            'Name': quota_name,
            'Hard Limit': pretty_size(str(result.get('hard_limit', 0))) if result.get('hard_limit') else 'Unlimited',
            'Soft Limit': pretty_size(str(result.get('soft_limit', 0))) if result.get('soft_limit') else 'Unlimited',
            'Files Hard Limit': result.get('inodes_hard_limit', 'Unlimited') if result.get('inodes_hard_limit') else 'Unlimited',
            'Files Soft Limit': result.get('inodes_soft_limit', 'Unlimited') if result.get('inodes_soft_limit') else 'Unlimited',
            'Grace Period': result.get('grace_period', 'N/A')
        }
    except Exception as e:
        logging.error(f"Failed to create/update quota on {cluster_address}. Error: {e}")
        raise


VALID_SUPPORT_BUNDLE_PRESETS = {
    'standard', 'default', 'debug', 'micro', 'mini', 'management',
    'performance', 'traces_and_metrics', 'nfsv3', 'nfsv4', 'smb', 's3',
    'estore', 'raid', 'hardware', 'permission_issues', 'rca', 'dr',
    'inspect_metadata',
}


def _normalize_timestamp(value: str, target_fmt: str) -> str:
    """Try multiple common timestamp formats and return the value in target_fmt.

    Handles ISO 8601 variants (with T, with Z, with timezone offset) as well as
    the plain "YYYY-MM-DD HH:MM:SS" format the API expects.
    """
    value = value.strip()
    # Strip trailing 'Z' or timezone offset for parsing
    clean = value.replace('Z', '').rstrip()
    # Remove timezone offset like +00:00 or -05:00
    if re.match(r'.*[+-]\d{2}:\d{2}$', clean):
        clean = clean[:-6]

    candidate_formats = [
        target_fmt,               # "2026-03-07 00:00:00"
        "%Y-%m-%dT%H:%M:%S",     # "2026-03-07T00:00:00"
        "%Y-%m-%dT%H:%M:%S.%f",  # "2026-03-07T00:00:00.123456"
        "%Y-%m-%d %H:%M:%S.%f",  # "2026-03-07 00:00:00.123456"
        "%Y-%m-%d %H:%M",        # "2026-03-07 00:00"
        "%Y-%m-%dT%H:%M",        # "2026-03-07T00:00"
        "%Y-%m-%d",              # "2026-03-07"
    ]
    for fmt in candidate_formats:
        try:
            dt = datetime.strptime(clean, fmt)
            return dt.strftime(target_fmt)
        except ValueError:
            continue
    raise ValueError(
        f"Cannot parse timestamp '{value}'. "
        f"Expected format: YYYY-MM-DD HH:MM:SS (e.g., '2026-03-07 00:00:00')"
    )


def _resolve_node_ids(
    client,
    endpoint: str,
    name_filter: str,
    whitelist
) -> tuple:
    """Resolve node IDs from a name filter/pattern by querying the cnodes or dnodes endpoint.

    Args:
        client: VAST client instance.
        endpoint: 'cnodes' or 'dnodes'.
        name_filter: Name prefix or wildcard pattern (fnmatch-style).
        whitelist: API whitelist dict.

    Returns:
        Tuple of (comma-separated IDs string, list of matched node names).
    """
    nodes = call_vast_api(
        client=client,
        endpoint=endpoint,
        method='get',
        params={'page': 1, 'page_size': 5000},
        whitelist=whitelist,
    )

    if '*' not in name_filter and '?' not in name_filter:
        name_filter = name_filter + '*'

    matched = [
        (str(n['id']), n.get('display_name', n.get('name', '')))
        for n in nodes
        if fnmatch.fnmatch(n.get('display_name', n.get('name', '')), name_filter)
    ]

    if not matched:
        raise ValueError(
            f"No {endpoint} matched the filter '{name_filter}'. "
            f"Available names: {', '.join(n.get('display_name', n.get('name', '')) for n in nodes[:20])}"
        )

    matched_ids = [m[0] for m in matched]
    matched_names = [m[1] for m in matched]
    logging.info(f"Resolved {len(matched)} {endpoint} from filter '{name_filter}': {', '.join(matched_names)}")
    return ','.join(matched_ids), matched_names


def create_support_bundle(
    cluster: str,
    prefix: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    duration: Optional[str] = None,
    preset: str = 'standard',
    aggregated: bool = False,
    text: bool = False,
    obfuscated: bool = False,
    cnodes_only: bool = False,
    dnodes_only: bool = False,
    send_now: bool = False,
    cnode_ids: Optional[str] = None,
    dnode_ids: Optional[str] = None,
    cnode_filter: Optional[str] = None,
    dnode_filter: Optional[str] = None,
    luna_args: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a support bundle on a VAST cluster.

    Args:
        cluster: Cluster address or name. Required.
        prefix: Bundle name/prefix. Required.
        start_time: Start time in "YYYY-MM-DD HH:MM:SS" format. Optional.
        end_time: End time in "YYYY-MM-DD HH:MM:SS" format. Optional.
        duration: Duration string (e.g. "5m", "10m", "1h"). When provided alone, means "last N minutes" (end=now, start=now-duration). Can also combine with start_time or end_time.
        preset: Bundle preset type. Defaults to 'standard'.
        aggregated: Whether to aggregate the bundle. Defaults to False.
        text: Whether to include text output. Defaults to False.
        obfuscated: Whether to encrypt private data. Defaults to False.
        cnodes_only: Include only cnode data. Defaults to False.
        dnodes_only: Include only dnode data. Defaults to False.
        send_now: Upload bundle to VAST support immediately. Defaults to False.
        cnode_ids: Comma-separated cnode IDs to include.
        dnode_ids: Comma-separated dnode IDs to include.
        cnode_filter: Name prefix/pattern to resolve cnode IDs (e.g. "cnode-128*").
        dnode_filter: Name prefix/pattern to resolve dnode IDs (e.g. "dnode-5*").
        luna_args: Luna arguments string (e.g. "perf_overview").

    Returns:
        Summary dict with bundle creation details.
    """
    config = load_config()
    cluster_address, cluster_config, _ = resolve_cluster_identifier(cluster, config)

    preset = preset.lower().strip()
    if preset not in VALID_SUPPORT_BUNDLE_PRESETS:
        raise ValueError(
            f"Invalid preset '{preset}'. Must be one of: {', '.join(sorted(VALID_SUPPORT_BUNDLE_PRESETS))}"
        )

    # --- Normalize timestamps to "YYYY-MM-DD HH:MM:SS" ---
    api_time_fmt = "%Y-%m-%d %H:%M:%S"
    start_time = _normalize_timestamp(start_time, api_time_fmt) if start_time else None
    end_time = _normalize_timestamp(end_time, api_time_fmt) if end_time else None

    # --- Time resolution ---
    duration_seconds = parse_time_duration(duration) if duration else None

    if start_time and end_time:
        pass  # both provided explicitly
    elif start_time and not end_time:
        if not duration_seconds:
            raise ValueError("Either end_time or duration must be provided when start_time is given.")
        dt_start = datetime.strptime(start_time, api_time_fmt)
        dt_end = dt_start + timedelta(seconds=duration_seconds)
        end_time = dt_end.strftime(api_time_fmt)
    elif end_time and not start_time:
        if not duration_seconds:
            raise ValueError("Either start_time or duration must be provided when end_time is given.")
        dt_end = datetime.strptime(end_time, api_time_fmt)
        dt_start = dt_end - timedelta(seconds=duration_seconds)
        start_time = dt_start.strftime(api_time_fmt)
    elif duration_seconds and not start_time and not end_time:
        dt_end = datetime.now(timezone.utc).replace(tzinfo=None)
        dt_start = dt_end - timedelta(seconds=duration_seconds)
        start_time = dt_start.strftime(api_time_fmt)
        end_time = dt_end.strftime(api_time_fmt)
        logging.info(f"Relative time: using last {duration} -> {start_time} to {end_time}")
    else:
        raise ValueError(
            "Time must be specified. Provide one of: "
            "(1) duration alone for 'last N minutes', "
            "(2) start_time + duration, "
            "(3) end_time + duration, or "
            "(4) start_time + end_time."
        )

    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()

    # --- Resolve node IDs from filters ---
    resolved_cnode_names: List[str] = []
    resolved_dnode_names: List[str] = []
    if cnode_filter and not cnode_ids:
        cnode_ids, resolved_cnode_names = _resolve_node_ids(client, 'cnodes', cnode_filter, whitelist)
    if dnode_filter and not dnode_ids:
        dnode_ids, resolved_dnode_names = _resolve_node_ids(client, 'dnodes', dnode_filter, whitelist)

    # --- Build payload ---
    payload: Dict[str, Any] = {
        'prefix': prefix,
        'aggregated': aggregated,
        'preset': preset,
        'text': text,
        'obfuscated': obfuscated,
        'cnodes_only': cnodes_only,
        'dnodes_only': dnodes_only,
        'send_now': send_now,
        'start_time': start_time,
        'end_time': end_time,
    }
    if cnode_ids:
        payload['cnode_ids'] = cnode_ids
    if dnode_ids:
        payload['dnode_ids'] = dnode_ids
    if luna_args:
        payload['luna_args'] = luna_args

    try:
        logging.info(
            f"Creating support bundle on cluster={cluster_address} "
            f"prefix='{prefix}' preset={preset} "
            f"time={start_time} -> {end_time} "
            f"send_now={send_now} obfuscated={obfuscated}"
        )

        result = call_vast_api(
            client=client,
            endpoint='supportbundles',
            method='post',
            params=payload,
            whitelist=whitelist,
        )

        bundle = result[0] if isinstance(result, list) and result else result
        logging.info(f"Support bundle created successfully: id={bundle.get('id')} state={bundle.get('state')}")

        summary: Dict[str, Any] = {
            'Cluster': cluster_address,
            'ID': bundle.get('id'),
            'Name': bundle.get('name'),
            'State': bundle.get('state'),
            'Preset': bundle.get('preset'),
            'Start Time': bundle.get('start_time'),
            'End Time': bundle.get('end_time'),
            'Bundle File': bundle.get('bundle_file'),
            'Bundle URL': bundle.get('bundle_url'),
            'Send Now': send_now,
            'Obfuscated': obfuscated,
            'CNodes Only': cnodes_only,
            'DNodes Only': dnodes_only,
            'CNode IDs': bundle.get('cnode_ids', ''),
            'DNode IDs': bundle.get('dnode_ids', ''),
            'Luna Args': bundle.get('luna_args', ''),
            'Position in Queue': bundle.get('position_in_queue'),
        }
        if resolved_cnode_names:
            summary['CNode Names'] = ', '.join(resolved_cnode_names)
        if resolved_dnode_names:
            summary['DNode Names'] = ', '.join(resolved_dnode_names)
        return summary
    except Exception as e:
        logging.error(f"Failed to create support bundle on {cluster_address}. Error: {e}")
        raise


def create_tenant(
    cluster: str,
    name: str,
    client_ip_ranges: Any,
) -> Dict[str, Any]:
    """Create a tenant with client IP ranges.

    Args:
        cluster: Cluster address or name. Required.
        name: Tenant name. Required.
        client_ip_ranges: List of [start, end] pairs, or comma-separated
            "start-end" / single-IP strings.
    """
    if not name or not str(name).strip():
        raise ValueError("name is required for tenant creation")

    ranges = _parse_ip_ranges(client_ip_ranges, arg_name='client_ip_ranges')
    config = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(cluster, config)
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()

    payload = {
        'name': str(name).strip(),
        'client_ip_ranges': ranges,
    }
    logging.info(f"Creating tenant name={payload['name']} on cluster={cluster_address}")
    created = call_vast_api(
        client=client,
        endpoint='tenants',
        method='post',
        params=payload,
        whitelist=whitelist,
    )
    tenant = created[0] if created else {}
    return {
        'Cluster': cluster_address,
        'ID': tenant.get('id'),
        'Name': tenant.get('name', payload['name']),
        'Client IP Ranges': tenant.get('client_ip_ranges', ranges),
        'Client IP Ranges Summary': tenant.get('client_ip_ranges_summary'),
    }


def create_vippool(
    cluster: str,
    name: str,
    ip_ranges: Any,
    tenant: Optional[str] = None,
    role: str = 'PROTOCOLS',
    subnet_cidr: int = 16,
    vlan: int = 0,
    gw_ip: str = '',
    port_membership: str = 'ALL',
    cnode_ids: Optional[Any] = None,
    enabled: bool = True,
) -> Dict[str, Any]:
    """Create a VIP pool.

    Always sends tenant_id explicitly. Pass tenant=None/''/'none'/'null' for a
    tenantless (shared) pool — omitting tenant_id silently binds to default on VMS.
    """
    if not name or not str(name).strip():
        raise ValueError("name is required for VIP pool creation")

    ranges = _parse_ip_ranges(ip_ranges, arg_name='ip_ranges')
    config = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(cluster, config)
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()

    tenant_token = None if tenant is None else str(tenant).strip()
    tenantless_tokens = {'', 'none', 'null', 'tenantless'}
    if tenant_token is None or tenant_token.lower() in tenantless_tokens:
        tenant_id = None
        tenant_label = 'tenantless'
    else:
        tenant_id = get_id_by_name(client, 'tenants', tenant_token, whitelist=whitelist)
        if not tenant_id:
            raise ValueError(f"Tenant {tenant_token} not found on cluster {cluster_address}.")
        tenant_label = tenant_token

    resolved_cnode_ids: List[int] = []
    if cnode_ids:
        if isinstance(cnode_ids, list):
            tokens = [str(v).strip() for v in cnode_ids if str(v).strip()]
        else:
            tokens = [p.strip() for p in str(cnode_ids).split(',') if p.strip()]
        for token in tokens:
            if token.isdigit():
                resolved_cnode_ids.append(int(token))
            else:
                cnode_id = get_id_by_name(client, 'cnodes', token, whitelist=whitelist)
                if not cnode_id:
                    raise ValueError(f"CNode '{token}' not found on cluster {cluster_address}.")
                resolved_cnode_ids.append(int(cnode_id))

    payload: Dict[str, Any] = {
        'name': str(name).strip(),
        'role': role or 'PROTOCOLS',
        'tenant_id': tenant_id,  # explicit null for tenantless pools
        'ip_ranges': ranges,
        'subnet_cidr': int(subnet_cidr),
        'vlan': int(vlan),
        'gw_ip': gw_ip if gw_ip is not None else '',
        'port_membership': port_membership or 'ALL',
        'cnode_ids': resolved_cnode_ids,
        'enabled': bool(enabled),
    }
    logging.info(
        f"Creating vippool name={payload['name']} tenant={tenant_label} on cluster={cluster_address}"
    )
    created = call_vast_api(
        client=client,
        endpoint='vippools',
        method='post',
        params=payload,
        whitelist=whitelist,
    )
    pool = created[0] if created else {}
    return {
        'Cluster': cluster_address,
        'ID': pool.get('id'),
        'Name': pool.get('name', payload['name']),
        'Tenant': tenant_label,
        'Tenant ID': pool.get('tenant_id', tenant_id),
        'Role': pool.get('role', payload['role']),
        'IP Ranges': pool.get('ip_ranges', ranges),
        'Enabled': pool.get('enabled', enabled),
    }


def create_user(
    cluster: str,
    name: str,
    uid: int,
    local_provider: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a local user.

    Users have no tenant field; tenant scope comes from the local provider.
    """
    if not name or not str(name).strip():
        raise ValueError("name is required for user creation")
    if uid is None:
        raise ValueError("uid is required for user creation")

    config = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(cluster, config)
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()

    provider_token = None if local_provider is None else str(local_provider).strip()
    if not provider_token:
        local_provider_id = 1
        provider_label = '1'
    elif provider_token.isdigit():
        local_provider_id = int(provider_token)
        provider_label = provider_token
    else:
        local_provider_id = get_id_by_name(client, 'localproviders', provider_token, whitelist=whitelist)
        if not local_provider_id:
            raise ValueError(f"Local provider '{provider_token}' not found on cluster {cluster_address}.")
        provider_label = provider_token

    payload = {
        'name': str(name).strip(),
        'uid': int(uid),
        'local_provider_id': int(local_provider_id),
    }
    logging.info(f"Creating user name={payload['name']} uid={payload['uid']} on cluster={cluster_address}")
    created = call_vast_api(
        client=client,
        endpoint='users',
        method='post',
        params=payload,
        whitelist=whitelist,
    )
    user = created[0] if created else {}
    return {
        'Cluster': cluster_address,
        'ID': user.get('id'),
        'Name': user.get('name', payload['name']),
        'UID': user.get('uid', payload['uid']),
        'Local Provider': provider_label,
        'Local Provider ID': user.get('local_provider_id', local_provider_id),
    }


def create_viewpolicy(
    cluster: str,
    name: str,
    tenant: str,
    flavor: str = 'S3_NATIVE',
    auth_source: str = 'PROVIDERS',
    access_flavor: str = 'ALL',
    path_length: str = 'LCD',
    allowed_characters: str = 'LCD',
    nfs_read_write: Optional[Any] = None,
    s3_read_write: Optional[Any] = None,
) -> Dict[str, Any]:
    """Create a view policy.

    Do not send legacy read_write together with nfs_read_write.
    """
    if not name or not str(name).strip():
        raise ValueError("name is required for view policy creation")
    if not tenant or not str(tenant).strip():
        raise ValueError("tenant is required for view policy creation")

    def _as_list(value: Any, default: List[str]) -> List[str]:
        if value is None:
            return list(default)
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        return [p.strip() for p in str(value).split(',') if p.strip()]

    config = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(cluster, config)
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()

    tenant_name = str(tenant).strip()
    tenant_id = get_id_by_name(client, 'tenants', tenant_name, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant {tenant_name} not found on cluster {cluster_address}.")

    payload: Dict[str, Any] = {
        'name': str(name).strip(),
        'tenant_id': tenant_id,
        'flavor': flavor or 'S3_NATIVE',
        'auth_source': auth_source or 'PROVIDERS',
        'access_flavor': access_flavor or 'ALL',
        'path_length': path_length or 'LCD',
        'allowed_characters': allowed_characters or 'LCD',
        'nfs_read_write': _as_list(nfs_read_write, ['*']),
        's3_read_write': _as_list(s3_read_write, ['*']),
    }
    logging.info(
        f"Creating viewpolicy name={payload['name']} tenant={tenant_name} on cluster={cluster_address}"
    )
    created = call_vast_api(
        client=client,
        endpoint='viewpolicies',
        method='post',
        params=payload,
        whitelist=whitelist,
    )
    policy = created[0] if created else {}
    return {
        'Cluster': cluster_address,
        'ID': policy.get('id'),
        'Name': policy.get('name', payload['name']),
        'Tenant': tenant_name,
        'Tenant ID': policy.get('tenant_id', tenant_id),
        'Flavor': policy.get('flavor', payload['flavor']),
        'Auth Source': policy.get('auth_source', payload['auth_source']),
    }


def create_s3_key(
    cluster: str,
    user: str,
    tenant: Optional[str] = None,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a local S3 access key for a user.

    Locates the user through query_users (tenant-scoped), validates the user has
    fewer than 2 local S3 keys, then POSTs to users/{id}/access_keys/.

    Args:
        cluster: Cluster address or name. Required.
        user: User name / login name. Required.
        tenant: Tenant name. Optional; defaults to 'default' and is noted in the output.
        provider: Local provider name or id. Optional; when omitted, uses the local
            provider assigned to the tenant.
    """
    if not cluster or not str(cluster).strip():
        raise ValueError("cluster is required for S3 key creation")
    if not user or not str(user).strip():
        raise ValueError("user is required for S3 key creation")

    user_name = str(user).strip()
    tenant_was_defaulted = not (tenant and str(tenant).strip())
    tenant_name = 'default' if tenant_was_defaulted else str(tenant).strip()

    # Locate user details via existing query_users functionality
    queried = query_users(cluster=cluster, tenant=tenant_name, prefix=user_name, top=50)
    matched_user = _exact_query_user_match(queried, user_name)

    existing_key_count = _count_enabled_access_keys(matched_user)
    if existing_key_count >= MAX_LOCAL_S3_KEYS_PER_USER:
        raise ValueError(
            f"User '{user_name}' already has {existing_key_count} local S3 keys "
            f"(maximum {MAX_LOCAL_S3_KEYS_PER_USER}). Delete an existing key before creating another."
        )

    config = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(cluster, config)
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()

    tenant_id = get_id_by_name(client, 'tenants', tenant_name, whitelist=whitelist)
    if not tenant_id:
        raise ValueError(f"Tenant '{tenant_name}' not found on cluster {cluster_address}.")

    provider_obj = _resolve_provider(client, provider, tenant_name, whitelist)
    provider_id = provider_obj.get('id')
    provider_name = provider_obj.get('name')

    # Resolve VMS user object id under the local provider (needed for access_keys POST)
    user_params: Dict[str, Any] = {'name': user_name}
    if provider_id is not None:
        user_params['local_provider_id'] = provider_id
    local_users = call_vast_api(
        client=client,
        endpoint='users',
        method='get',
        params=user_params,
        whitelist=whitelist,
    )
    if not local_users:
        # Retry without provider filter in case the filter is unsupported
        local_users = call_vast_api(
            client=client,
            endpoint='users',
            method='get',
            params={'name': user_name},
            whitelist=whitelist,
        )
        if provider_id is not None:
            local_users = [
                u for u in local_users
                if (u.get('local_provider') or {}).get('id') == provider_id
                or u.get('local_provider_id') == provider_id
            ]
    if not local_users:
        raise ValueError(
            f"Local user object for '{user_name}' not found"
            + (f" under provider '{provider_name}'" if provider_name else "")
        )
    if len(local_users) > 1:
        raise ValueError(
            f"Ambiguous local user '{user_name}': found {len(local_users)} user objects"
        )

    user_obj = local_users[0]
    user_id = user_obj.get('id')
    if user_id is None:
        raise ValueError(f"Local user '{user_name}' has no id; cannot create access key")

    # Whitelist: access_keys is a sub-endpoint of users; require users POST
    if 'users' not in whitelist:
        raise ValueError(
            "Access denied: API endpoint 'users' is not whitelisted for S3 key creation."
        )
    allowed_methods = whitelist.get('users', [])
    if 'post' not in allowed_methods:
        raise ValueError(
            "Access denied: HTTP method 'POST' is not allowed for endpoint 'users' "
            "(required for users/{id}/access_keys)."
        )

    logging.info(
        f"Creating S3 key for user={user_name} id={user_id} tenant={tenant_name} "
        f"provider={provider_name} on cluster={cluster_address}"
    )
    try:
        created = client.users[user_id].access_keys.post(tenant_id=tenant_id)
    except Exception as e:
        logging.error(f"Failed to create S3 key for user '{user_name}': {e}")
        raise

    if isinstance(created, list):
        created = created[0] if created else {}
    if not isinstance(created, dict):
        created = {}

    result: Dict[str, Any] = {
        'Cluster': cluster_address,
        'User': matched_user.get('name') or user_name,
        'Login Name': matched_user.get('login_name') or user_obj.get('name') or user_name,
        'User ID': user_id,
        'UID': matched_user.get('uid') or user_obj.get('uid'),
        'Tenant': tenant_name,
        'Tenant ID': tenant_id,
        'Tenant Defaulted': tenant_was_defaulted,
        'Provider': provider_name,
        'Provider ID': provider_id,
        'Existing Keys Before Create': existing_key_count,
        'Access Key': created.get('access_key'),
        'Secret Key': created.get('secret_key'),
        'S3 Info': matched_user.get('s3 info') or matched_user.get('s3_info') or '',
        'Identity Policies': matched_user.get('identity policies') or '',
        'Origins': matched_user.get('origins') or '',
    }
    if tenant_was_defaulted:
        result['Note'] = "Tenant was not provided; used default tenant."
    return result

