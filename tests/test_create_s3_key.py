"""Unit tests for create_s3_key_vast / S3 key delete wiring."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from vast_admin_mcp.config import TEMPLATE_MODIFICATIONS_FILE, get_default_template_path
from vast_admin_mcp.create_functions import (
    MAX_LOCAL_S3_KEYS_PER_USER,
    _count_enabled_access_keys,
    _exact_query_user_match,
    create_s3_key,
)
from vast_admin_mcp.template_parser import TemplateParser


def _parser() -> TemplateParser:
    return TemplateParser(
        TEMPLATE_MODIFICATIONS_FILE,
        default_template_path=get_default_template_path(),
    )


class TestS3KeyHelpers(unittest.TestCase):
    def test_exact_query_user_match(self):
        users = [
            {'name': 'andy', 'login_name': 'andy', 'access_keys': 'AAA'},
            {'name': 'andrew', 'login_name': 'andrew', 'access_keys': ''},
        ]
        matched = _exact_query_user_match(users, 'andy')
        self.assertEqual(matched['name'], 'andy')

    def test_exact_query_user_match_missing(self):
        with self.assertRaises(ValueError):
            _exact_query_user_match([{'name': 'bob', 'login_name': 'bob'}], 'andy')

    def test_count_enabled_access_keys(self):
        self.assertEqual(_count_enabled_access_keys({'access_keys': 'A,B'}), 2)
        self.assertEqual(_count_enabled_access_keys({'access_keys': ''}), 0)
        self.assertEqual(
            _count_enabled_access_keys({
                'access_keys': [
                    {'access_key': 'A', 'enabled': True},
                    {'access_key': 'B', 'enabled': False},
                ]
            }),
            1,
        )


class TestCreateS3Key(unittest.TestCase):
    def test_rejects_when_user_already_has_two_keys(self):
        queried = [{
            'name': 'andy',
            'login_name': 'andy',
            'access_keys': 'KEY1,KEY2',
            'uid': 1,
        }]
        with patch('vast_admin_mcp.create_functions.query_users', return_value=queried):
            with self.assertRaises(ValueError) as ctx:
                create_s3_key(cluster='c1', user='andy', tenant='default')
        self.assertIn(str(MAX_LOCAL_S3_KEYS_PER_USER), str(ctx.exception))

    def test_creates_key_and_notes_default_tenant(self):
        queried = [{
            'name': 'bob',
            'login_name': 'bob',
            'access_keys': 'KEY1',
            'uid': 42,
            's3 info': 'allow create bucket',
            'identity policies': 'pol1',
            'origins': 'uid:LOCAL',
        }]
        provider = {'id': 1, 'name': 'default', 'assigned_tenants_preview': 'default'}
        local_user = {'id': 9, 'name': 'bob', 'uid': 42, 'local_provider': provider}
        access_keys_ep = MagicMock()
        access_keys_ep.post.return_value = {
            'access_key': 'NEWACCESS',
            'secret_key': 'NEWSECRET',
        }
        client = MagicMock()
        client.users = MagicMock()
        client.users.__getitem__.return_value.access_keys = access_keys_ep

        with patch('vast_admin_mcp.create_functions.query_users', return_value=queried):
            with patch('vast_admin_mcp.create_functions.load_config', return_value={'clusters': []}):
                with patch(
                    'vast_admin_mcp.create_functions.resolve_cluster_identifier',
                    return_value=('cluster', {}, None),
                ):
                    with patch(
                        'vast_admin_mcp.create_functions.create_vast_client',
                        return_value=client,
                    ):
                        with patch(
                            'vast_admin_mcp.create_functions.get_api_whitelist',
                            return_value={
                                'users': ['get', 'post'],
                                'tenants': ['get', 'post'],
                                'localproviders': ['get'],
                            },
                        ):
                            with patch(
                                'vast_admin_mcp.create_functions.get_id_by_name',
                                return_value=1,
                            ):
                                with patch(
                                    'vast_admin_mcp.create_functions.call_vast_api',
                                    side_effect=[
                                        [provider],  # localproviders for resolve
                                        [local_user],  # users by name
                                    ],
                                ):
                                    result = create_s3_key(cluster='c1', user='bob')

        self.assertEqual(result['Access Key'], 'NEWACCESS')
        self.assertEqual(result['Secret Key'], 'NEWSECRET')
        self.assertEqual(result['Tenant'], 'default')
        self.assertTrue(result['Tenant Defaulted'])
        self.assertIn('default tenant', result['Note'].lower())
        access_keys_ep.post.assert_called_once_with(tenant_id=1)


class TestS3KeyDeleteCmd(unittest.TestCase):
    def test_delete_cmds_includes_s3_keys(self):
        parser = _parser()
        self.assertIn('s3_keys', parser.get_all_delete_commands())
        wl = parser.get_api_whitelist()
        self.assertIn('delete', wl['locals3keys'])


class TestCreateS3KeyMcpFlag(unittest.TestCase):
    def test_mcp_flag_prints_code_without_executing(self):
        from types import SimpleNamespace
        from vast_admin_mcp.cli import _cli_module as cli

        args = SimpleNamespace(
            cluster='dddd',
            user='ffff',
            tenant=None,
            provider=None,
            debug=False,
            mcp=True,
            format='table',
            output=None,
        )
        with patch('vast_admin_mcp.create_functions.create_s3_key') as create_mock:
            with patch('builtins.print') as print_mock:
                cli.handle_create_s3_key_command(args)
        create_mock.assert_not_called()
        printed = ''.join(str(c.args[0]) for c in print_mock.call_args_list if c.args)
        self.assertIn('create_s3_key_vast', printed)
        self.assertIn('async def create_s3_key_mcp', printed)


if __name__ == '__main__':
    unittest.main()
