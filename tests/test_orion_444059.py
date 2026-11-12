"""Unit tests for ORION-444059 Event Broker MCP gaps."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from vast_admin_mcp.config import TEMPLATE_MODIFICATIONS_FILE, get_default_template_path
from vast_admin_mcp.create_functions import (
    _parse_ip_ranges,
    create_view,
    create_vippool,
)
from vast_admin_mcp.delete_functions import delete_dynamic
from vast_admin_mcp.template_parser import TemplateParser


def _parser() -> TemplateParser:
    return TemplateParser(
        TEMPLATE_MODIFICATIONS_FILE,
        default_template_path=get_default_template_path(),
    )


class TestListTemplateOrion444059(unittest.TestCase):
    def test_views_protocol_filter_maps_to_protocols_contains(self):
        parser = _parser()
        protocol_args = [a for a in parser.get_arguments('views') if a.get('name') == 'protocol']
        self.assertEqual(len(protocol_args), 1)
        self.assertEqual(protocol_args[0].get('_api_field'), 'protocols__contains')
        self.assertTrue(protocol_args[0].get('filter'))
        # Display protocols field must remain non-filterable
        display = [
            f for f in parser.get_fields('views')
            if f.get('name') == 'protocols' and not f.get('argument')
        ]
        self.assertTrue(display)

    def test_tenants_expose_client_ip_ranges(self):
        parser = _parser()
        field_names = [f.get('name') for f in parser.get_fields('tenants')]
        self.assertIn('client ip ranges', field_names)
        self.assertIn('client ip ranges summary', field_names)

    def test_delete_cmds_loaded_without_tenants(self):
        parser = _parser()
        cmds = parser.get_all_delete_commands()
        self.assertEqual(sorted(cmds), ['s3_keys', 'users', 'viewpolicies', 'views', 'vippools'])
        self.assertNotIn('tenants', cmds)

    def test_whitelist_methods(self):
        wl = _parser().get_api_whitelist()
        self.assertEqual(wl['tenants'], ['get', 'post'])
        self.assertIn('delete', wl['vippools'])
        self.assertIn('delete', wl['users'])
        self.assertIn('delete', wl['viewpolicies'])
        self.assertIn('delete', wl['views'])
        self.assertIn('post', wl['views'])


class TestCreateValidationOrion444059(unittest.TestCase):
    def test_parse_ip_ranges_string_and_list(self):
        self.assertEqual(
            _parse_ip_ranges('10.0.0.1,10.0.0.10-10.0.0.20'),
            [['10.0.0.1', '10.0.0.1'], ['10.0.0.10', '10.0.0.20']],
        )
        self.assertEqual(
            _parse_ip_ranges([['1.1.1.1', '1.1.1.2']]),
            [['1.1.1.1', '1.1.1.2']],
        )

    def test_create_view_database_requires_s3(self):
        with self.assertRaises(ValueError) as ctx:
            with patch('vast_admin_mcp.create_functions.load_config', return_value={'clusters': []}):
                with patch(
                    'vast_admin_mcp.create_functions.resolve_cluster_identifier',
                    return_value=('cluster', {}, None),
                ):
                    with patch('vast_admin_mcp.create_functions.create_vast_client'):
                        with patch(
                            'vast_admin_mcp.create_functions.get_api_whitelist',
                            return_value={'tenants': ['get'], 'viewpolicies': ['get'], 'views': ['get', 'post']},
                        ):
                            with patch(
                                'vast_admin_mcp.create_functions.get_id_by_name',
                                side_effect=[1, 2],
                            ):
                                create_view(
                                    cluster='c1',
                                    tenant='t1',
                                    path='/eb',
                                    protocols='DATABASE,KAFKA',
                                    bucket='b',
                                    bucket_owner='owner',
                                    kafka_vip_pools='pool1',
                                )
        self.assertIn('DATABASE', str(ctx.exception))

    def test_create_view_kafka_requires_vip_pools(self):
        with self.assertRaises(ValueError) as ctx:
            with patch('vast_admin_mcp.create_functions.load_config', return_value={'clusters': []}):
                with patch(
                    'vast_admin_mcp.create_functions.resolve_cluster_identifier',
                    return_value=('cluster', {}, None),
                ):
                    with patch('vast_admin_mcp.create_functions.create_vast_client'):
                        with patch(
                            'vast_admin_mcp.create_functions.get_api_whitelist',
                            return_value={'tenants': ['get'], 'viewpolicies': ['get'], 'views': ['get', 'post']},
                        ):
                            with patch(
                                'vast_admin_mcp.create_functions.get_id_by_name',
                                side_effect=[1, 2],
                            ):
                                create_view(
                                    cluster='c1',
                                    tenant='t1',
                                    path='/eb',
                                    protocols='S3,DATABASE,KAFKA',
                                    bucket='b',
                                    bucket_owner='owner',
                                )
        self.assertIn('kafka_vip_pools', str(ctx.exception))

    def test_create_vippool_always_sends_tenant_id_null(self):
        captured = {}

        def fake_call_vast_api(**kwargs):
            captured['params'] = kwargs['params']
            return [{'id': 9, 'name': kwargs['params']['name'], 'tenant_id': None}]

        with patch('vast_admin_mcp.create_functions.load_config', return_value={'clusters': []}):
            with patch(
                'vast_admin_mcp.create_functions.resolve_cluster_identifier',
                return_value=('cluster', {}, None),
            ):
                with patch('vast_admin_mcp.create_functions.create_vast_client'):
                    with patch(
                        'vast_admin_mcp.create_functions.get_api_whitelist',
                        return_value={'vippools': ['get', 'post'], 'tenants': ['get']},
                    ):
                        with patch(
                            'vast_admin_mcp.create_functions.call_vast_api',
                            side_effect=fake_call_vast_api,
                        ):
                            create_vippool(
                                cluster='c1',
                                name='eb-pool',
                                ip_ranges='10.0.1.1-10.0.1.2',
                                tenant='none',
                            )
        self.assertIn('tenant_id', captured['params'])
        self.assertIsNone(captured['params']['tenant_id'])


class TestDeleteDynamicOrion444059(unittest.TestCase):
    def test_delete_rejects_missing_delete_whitelist(self):
        with self.assertRaises(ValueError) as ctx:
            with patch('vast_admin_mcp.delete_functions.load_config', return_value={'clusters': []}):
                with patch(
                    'vast_admin_mcp.delete_functions.resolve_cluster_identifier',
                    return_value=('cluster', {}, None),
                ):
                    with patch('vast_admin_mcp.delete_functions.create_vast_client'):
                        with patch(
                            'vast_admin_mcp.delete_functions.get_api_whitelist',
                            return_value={'vippools': ['get', 'post']},
                        ):
                            delete_dynamic('vippools', cluster='c1', name='pool1')
        self.assertIn('DELETE', str(ctx.exception))

    def test_delete_requires_exactly_one_match(self):
        with self.assertRaises(ValueError) as ctx:
            with patch('vast_admin_mcp.delete_functions.load_config', return_value={'clusters': []}):
                with patch(
                    'vast_admin_mcp.delete_functions.resolve_cluster_identifier',
                    return_value=('cluster', {}, None),
                ):
                    with patch('vast_admin_mcp.delete_functions.create_vast_client'):
                        with patch(
                            'vast_admin_mcp.delete_functions.get_api_whitelist',
                            return_value={'vippools': ['get', 'post', 'delete']},
                        ):
                            with patch(
                                'vast_admin_mcp.delete_functions.call_vast_api',
                                return_value=[{'id': 1}, {'id': 2}],
                            ):
                                delete_dynamic('vippools', cluster='c1', name='pool1')
        self.assertIn('Ambiguous', str(ctx.exception))

    def test_delete_zero_matches(self):
        with self.assertRaises(ValueError) as ctx:
            with patch('vast_admin_mcp.delete_functions.load_config', return_value={'clusters': []}):
                with patch(
                    'vast_admin_mcp.delete_functions.resolve_cluster_identifier',
                    return_value=('cluster', {}, None),
                ):
                    with patch('vast_admin_mcp.delete_functions.create_vast_client'):
                        with patch(
                            'vast_admin_mcp.delete_functions.get_api_whitelist',
                            return_value={'vippools': ['get', 'post', 'delete']},
                        ):
                            with patch(
                                'vast_admin_mcp.delete_functions.call_vast_api',
                                return_value=[],
                            ):
                                delete_dynamic('vippools', cluster='c1', name='missing')
        self.assertIn('No vippools found', str(ctx.exception))


class TestReadWriteToolRegistration(unittest.TestCase):
    """Create/delete MCP tools must only register when --read-write is set."""

    def _collect_registered_tools(self, *, read_write: bool):
        from vast_admin_mcp import mcp_server

        registered = []

        class FakeFastMCP:
            def __init__(self, *args, **kwargs):
                pass

            def custom_route(self, *args, **kwargs):
                return lambda func: func

            def tool(self, *args, **kwargs):
                name = kwargs.get('name')
                if name is None and args:
                    name = args[0]
                registered.append(name)

                def decorator(func):
                    return func

                return decorator

            def run(self, **kwargs):
                return None

        with patch.object(mcp_server, 'FastMCP', FakeFastMCP):
            mcp_server.start_mcp(read_write=read_write, transport='stdio')
        return registered

    def test_readonly_omits_create_and_delete_tools(self):
        names = self._collect_registered_tools(read_write=False)
        create_or_delete = [n for n in names if n and (
            n.startswith('create_') or n.startswith('delete_')
        )]
        self.assertEqual(
            create_or_delete,
            [],
            f'readonly mode exposed write tools: {create_or_delete}',
        )
        self.assertIn('list_clusters_vast', names)

    def test_read_write_registers_create_and_delete_tools(self):
        names = self._collect_registered_tools(read_write=True)
        self.assertIn('create_view_vast', names)
        self.assertIn('create_tenant_vast', names)
        self.assertIn('delete_vippools_vast', names)
        self.assertIn('delete_views_vast', names)


if __name__ == '__main__':
    unittest.main()
