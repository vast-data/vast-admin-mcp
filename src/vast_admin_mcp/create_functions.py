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
from .functions import list_dynamic
from .template_parser import TemplateParser
from .config import TEMPLATE_MODIFICATIONS_FILE, get_default_template_path



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
    qos_policy: Optional[str] = None
) -> List[Dict[str, str]]:
    """Create a view in a VAST cluster. Provide cluster and path at minimum.

    Args:
        cluster: Cluster address or name. Required.
        tenant: Tenant name that owns the view. Defaults to 'default' if not provided.
        path: View path (e.g., /s3/mybucket, /nfs/myshare). Required.
        protocols: Comma seperated list of protocols to enable (e.g., NFS,S3,SMB,ENDPOINT). if not specified, NFS will be used. when S3 or ENDPOINT is specified, bucket and bucket_owner must be provided. when SMB is specified, share must be provided. (protocols are case-insensitive)
        bucket: Bucket name for S3 protocol. Must be provided if S3 or ENDPOINT protocol is requested.
        bucket_owner: Bucket owner name for S3 protocol. Must be provided if S3 or ENDPOINT protocol is enabled.
        share: Share name for NFS/SMB protocol. Must be provided if SMB is in the protocols list.
        policy: View policy name.
        hard_quota: Hard quota for the view (e.g., 10GB, 100GB, 1TB).
        qos_policy: QoS policy name that can help manage performance for the view.
    
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
                bucket_owner=view_params.get('bucket_owner') if view_params.get('bucket_owner') else None
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

