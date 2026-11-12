"""YAML-driven delete operations for vast-admin-mcp."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .client import call_vast_api, create_vast_client, resolve_cluster_identifier
from .config import TEMPLATE_MODIFICATIONS_FILE, get_default_template_path, load_config
from .template_parser import TemplateParser
from .utils import get_api_whitelist, parse_filter_value


def _get_template_parser() -> TemplateParser:
    default_template_path = get_default_template_path()
    return TemplateParser(TEMPLATE_MODIFICATIONS_FILE, default_template_path=default_template_path)


def _build_lookup_params(args_config: List[Dict[str, Any]], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Build GET lookup query params from delete_cmds filter arguments."""
    params: Dict[str, Any] = {}

    for arg in args_config:
        name = arg.get('name')
        if not name or name in ('cluster', 'clusters'):
            continue
        if not arg.get('filter', False):
            continue

        python_name = name.replace(' ', '_')
        raw_value = kwargs.get(python_name, kwargs.get(name))
        if raw_value is None or (isinstance(raw_value, str) and not raw_value.strip()):
            if arg.get('mandatory', False):
                raise ValueError(f"Missing required argument '{name}' for delete lookup")
            continue

        value_str = str(raw_value).strip()
        arg_type = arg.get('type', 'str')
        _, filter_value, api_suffix = parse_filter_value(value_str, arg_type)

        api_field = arg.get('_api_field') or name
        params[f"{api_field}{api_suffix}"] = filter_value

    return params


def _validate_delete_whitelist(endpoint: str, whitelist: Dict[str, List[str]]) -> None:
    if endpoint not in whitelist:
        raise ValueError(
            f"Access denied: API endpoint '{endpoint}' is not whitelisted. "
            f"Please contact your administrator to add it to the api_whitelist section "
            f"in the YAML configuration file."
        )
    allowed_methods = whitelist.get(endpoint, [])
    if 'delete' not in allowed_methods:
        raise ValueError(
            f"Access denied: HTTP method 'DELETE' is not allowed for endpoint '{endpoint}'. "
            f"Allowed methods: [{', '.join(allowed_methods)}]"
        )


def delete_dynamic(command_name: str, **kwargs) -> str:
    """Delete a VAST object defined by delete_cmds.<command_name>.

    Looks up a single object using filter arguments from the YAML template,
    then deletes it by id. Requires exactly one match.
    """
    parser = _get_template_parser()
    template = parser.get_delete_template(command_name)
    if not template:
        raise ValueError(f"Unknown delete command '{command_name}'. Available: {parser.get_all_delete_commands()}")

    endpoint = template.get('api_endpoint')
    if not endpoint:
        raise ValueError(f"delete_cmds.{command_name} is missing api_endpoint")

    cluster = kwargs.get('cluster') or kwargs.get('clusters')
    if not cluster or (isinstance(cluster, str) and not cluster.strip()):
        raise ValueError("cluster is required for delete operations")
    if isinstance(cluster, str) and ',' in cluster:
        raise ValueError("delete operations require a single cluster name, not a comma-separated list")

    config = load_config()
    cluster_address, _, _ = resolve_cluster_identifier(str(cluster).strip(), config)
    client = create_vast_client(cluster_address)
    whitelist = get_api_whitelist()
    _validate_delete_whitelist(endpoint, whitelist)

    args_config = parser.get_delete_arguments(command_name)
    lookup_params = _build_lookup_params(args_config, kwargs)
    if not lookup_params:
        raise ValueError(f"No lookup filters provided for delete command '{command_name}'")

    matches = call_vast_api(
        client=client,
        endpoint=endpoint,
        method='get',
        params=lookup_params,
        whitelist=whitelist,
    )

    if not matches:
        raise ValueError(
            f"No {command_name} found matching {lookup_params} on cluster {cluster_address}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"Ambiguous delete for {command_name}: found {len(matches)} matches for "
            f"{lookup_params} on cluster {cluster_address}. Narrow the identity filters."
        )

    obj = matches[0]
    object_id = obj.get('id')
    if object_id is None:
        raise ValueError(f"Matched {command_name} has no id; cannot delete")

    logging.info(
        f"Deleting {command_name} id={object_id} on cluster={cluster_address} filters={lookup_params}"
    )

    try:
        endpoint_obj = getattr(client, endpoint)
        endpoint_obj[object_id].delete()
    except Exception as e:
        logging.error(f"Failed to delete {command_name} id={object_id} on {cluster_address}: {e}")
        raise

    identity_bits = [f"{k}={v}" for k, v in lookup_params.items()]
    identity = ", ".join(identity_bits) if identity_bits else f"id={object_id}"
    return f"Deleted {command_name} id={object_id} ({identity}) on cluster={cluster_address}"
