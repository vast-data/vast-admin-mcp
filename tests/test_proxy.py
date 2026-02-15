#!/usr/bin/env python3
"""Unit tests for proxy detection and pool manager creation in client.py."""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add parent directory to path to import vast_admin_mcp
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp.client import _get_proxy_url, _create_pool_manager


class TestGetProxyUrl(unittest.TestCase):
    """Tests for _get_proxy_url() environment variable detection."""

    def _clean_env(self):
        """Return a dict with all proxy env vars removed."""
        keys = [
            'HTTPS_PROXY', 'https_proxy',
            'HTTP_PROXY', 'http_proxy',
            'ALL_PROXY', 'all_proxy',
            'NO_PROXY', 'no_proxy',
        ]
        return {k: v for k, v in os.environ.items() if k not in keys}

    # -- Basic detection ---------------------------------------------------

    @patch.dict(os.environ, {}, clear=True)
    def test_no_proxy_vars_returns_none(self):
        """With no proxy env vars set, should return None."""
        self.assertIsNone(_get_proxy_url("vast.example.com"))

    @patch.dict(os.environ, {'HTTPS_PROXY': 'http://proxy:8080'}, clear=True)
    def test_https_proxy_uppercase(self):
        self.assertEqual(_get_proxy_url("vast.example.com"), "http://proxy:8080")

    @patch.dict(os.environ, {'https_proxy': 'http://proxy:8080'}, clear=True)
    def test_https_proxy_lowercase(self):
        self.assertEqual(_get_proxy_url("vast.example.com"), "http://proxy:8080")

    @patch.dict(os.environ, {'HTTP_PROXY': 'http://proxy:3128'}, clear=True)
    def test_http_proxy_uppercase(self):
        self.assertEqual(_get_proxy_url("vast.example.com"), "http://proxy:3128")

    @patch.dict(os.environ, {'ALL_PROXY': 'socks5h://proxy:1080'}, clear=True)
    def test_all_proxy(self):
        self.assertEqual(_get_proxy_url("vast.example.com"), "socks5h://proxy:1080")

    # -- Precedence --------------------------------------------------------

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://https-proxy:8080',
        'HTTP_PROXY': 'http://http-proxy:3128',
        'ALL_PROXY': 'socks5://all-proxy:1080',
    }, clear=True)
    def test_https_proxy_takes_precedence(self):
        self.assertEqual(_get_proxy_url("vast.example.com"), "http://https-proxy:8080")

    @patch.dict(os.environ, {
        'HTTP_PROXY': 'http://http-proxy:3128',
        'ALL_PROXY': 'socks5://all-proxy:1080',
    }, clear=True)
    def test_http_proxy_before_all_proxy(self):
        self.assertEqual(_get_proxy_url("vast.example.com"), "http://http-proxy:3128")

    # -- NO_PROXY ----------------------------------------------------------

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'NO_PROXY': 'vast.example.com',
    }, clear=True)
    def test_no_proxy_exact_match(self):
        self.assertIsNone(_get_proxy_url("vast.example.com"))

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'NO_PROXY': 'vast.example.com',
    }, clear=True)
    def test_no_proxy_no_match_still_returns_proxy(self):
        self.assertEqual(_get_proxy_url("other.example.com"), "http://proxy:8080")

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'NO_PROXY': '.example.com',
    }, clear=True)
    def test_no_proxy_domain_suffix_with_dot(self):
        self.assertIsNone(_get_proxy_url("vast.example.com"))

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'NO_PROXY': 'example.com',
    }, clear=True)
    def test_no_proxy_domain_suffix_without_dot(self):
        self.assertIsNone(_get_proxy_url("vast.example.com"))

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'NO_PROXY': '*',
    }, clear=True)
    def test_no_proxy_wildcard(self):
        self.assertIsNone(_get_proxy_url("vast.example.com"))

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'NO_PROXY': 'internal.local, vast.example.com , other.host',
    }, clear=True)
    def test_no_proxy_comma_separated_list(self):
        self.assertIsNone(_get_proxy_url("vast.example.com"))
        self.assertIsNone(_get_proxy_url("internal.local"))
        self.assertEqual(_get_proxy_url("external.host"), "http://proxy:8080")

    @patch.dict(os.environ, {
        'HTTPS_PROXY': 'http://proxy:8080',
        'no_proxy': 'vast.example.com',
    }, clear=True)
    def test_no_proxy_lowercase_env_var(self):
        self.assertIsNone(_get_proxy_url("vast.example.com"))


class TestCreatePoolManager(unittest.TestCase):
    """Tests for _create_pool_manager() factory function."""

    def test_no_proxy_returns_pool_manager(self):
        """When proxy_url is None, should return a PoolManager."""
        import urllib3
        pm = _create_pool_manager(None, cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, urllib3.PoolManager)

    def test_http_proxy_returns_proxy_manager(self):
        """HTTP proxy URL should produce a ProxyManager."""
        import urllib3
        pm = _create_pool_manager("http://proxy:8080", cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, urllib3.ProxyManager)

    def test_https_proxy_returns_proxy_manager(self):
        """HTTPS proxy URL should produce a ProxyManager."""
        import urllib3
        pm = _create_pool_manager("https://proxy:8080", cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, urllib3.ProxyManager)

    def test_socks_proxy_without_pysocks_raises(self):
        """SOCKS proxy without PySocks should raise ImportError."""
        with patch.dict('sys.modules', {'urllib3.contrib.socks': None}):
            # Force re-import failure
            with patch('builtins.__import__', side_effect=_import_blocker('urllib3.contrib.socks')):
                with self.assertRaises(ImportError) as ctx:
                    _create_pool_manager("socks5://proxy:1080", cert_reqs='CERT_NONE')
                self.assertIn("PySocks", str(ctx.exception))

    def test_socks5_proxy_returns_socks_manager(self):
        """SOCKS5 proxy URL should produce SOCKSProxyManager (if PySocks available)."""
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError:
            self.skipTest("PySocks not installed — skipping SOCKS manager test")
        pm = _create_pool_manager("socks5://proxy:1080", cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, SOCKSProxyManager)

    def test_socks5h_proxy_returns_socks_manager(self):
        """SOCKS5h proxy URL should produce SOCKSProxyManager."""
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError:
            self.skipTest("PySocks not installed — skipping SOCKS manager test")
        pm = _create_pool_manager("socks5h://proxy:1080", cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, SOCKSProxyManager)

    def test_socks4_proxy_returns_socks_manager(self):
        """SOCKS4 proxy URL should produce SOCKSProxyManager."""
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError:
            self.skipTest("PySocks not installed — skipping SOCKS manager test")
        pm = _create_pool_manager("socks4://proxy:1080", cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, SOCKSProxyManager)

    def test_socks4a_proxy_returns_socks_manager(self):
        """SOCKS4a proxy URL should produce SOCKSProxyManager."""
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError:
            self.skipTest("PySocks not installed — skipping SOCKS manager test")
        pm = _create_pool_manager("socks4a://proxy:1080", cert_reqs='CERT_NONE')
        self.assertIsInstance(pm, SOCKSProxyManager)


def _import_blocker(blocked_module):
    """Return an __import__ replacement that blocks a specific module."""
    real_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

    def _blocked_import(name, *args, **kwargs):
        if name == blocked_module:
            raise ImportError(f"Mocked: {name} not available")
        return real_import(name, *args, **kwargs)

    return _blocked_import


if __name__ == '__main__':
    unittest.main()
