#!/usr/bin/env python3
"""Unit tests for VAST API token authentication."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp import client as client_module
# Alias must not be `setup_module` — pytest treats that name as an xUnit hook.
from vast_admin_mcp import setup as vast_setup


class TestApiTokenClientCreation(unittest.TestCase):
    def setUp(self):
        client_module.clear_client_cache()

    def test_create_vast_client_uses_api_token(self):
        captured = {}

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        config = {
            "clusters": [{
                "cluster": "vms.example.com",
                "auth_method": "api_token",
                "api_token": "env:VMS_API_TOKEN",
                "tenant": "default",
                "user_type": "TENANT_ADMIN",
                "vast_version": "5.3.0",
            }]
        }

        with patch.object(client_module, "load_config", return_value=config), \
                patch.object(client_module, "retrieve_password_secure", return_value="secret-token"), \
                patch.object(client_module, "VASTClient", FakeClient):
            client_module.create_vast_client("vms.example.com", use_cache=False)

        self.assertEqual(captured["address"], "vms.example.com")
        self.assertEqual(captured["token"], "secret-token")
        self.assertEqual(captured["tenant"], "default")
        self.assertNotIn("version", captured)
        self.assertNotIn("user", captured)
        self.assertNotIn("password", captured)

    def test_cluster_name_resolution_uses_api_token_for_temporary_client(self):
        captured = {}

        class FakeClusters:
            def get(self, **kwargs):
                return {"results": [{"name": "prod"}]}

        class FakeClient:
            def __init__(self, **kwargs):
                captured.update(kwargs)
                self.clusters = FakeClusters()

        config = {
            "clusters": [{
                "cluster": "prod-vms",
                "auth_method": "api_token",
                "api_token": "env:VMS_API_TOKEN",
                "tenant": "default",
                "user_type": "TENANT_ADMIN",
                "vast_version": "5.3.0",
            }]
        }

        with patch.object(client_module, "retrieve_password_secure", return_value="secret-token"), \
                patch.object(client_module, "VASTClient", FakeClient):
            address, cluster_config, cluster_name = client_module.resolve_cluster_identifier("prod", config)

        self.assertEqual(address, "prod-vms")
        self.assertEqual(cluster_config["cluster"], "prod-vms")
        self.assertEqual(cluster_name, "prod")
        self.assertEqual(captured["token"], "secret-token")
        self.assertEqual(captured["tenant"], "default")
        self.assertNotIn("version", captured)


class TestApiTokenSetup(unittest.TestCase):
    def test_validate_cluster_stores_api_token_reference(self):
        class FailingLoginEndpoint:
            def get(self, **kwargs):
                raise AssertionError("api/login should not be called for API token validation")

        class FakeEndpoint:
            def __init__(self, result):
                self._result = result

            def get(self, **kwargs):
                return self._result

        class FakeClient:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.login = FailingLoginEndpoint()
                self.clusters = FakeEndpoint({
                    "results": [{"name": "prod", "sw_version": "5.3.0.12"}]
                })

        with patch.object(vast_setup, "VASTClient", FakeClient), \
                patch.object(vast_setup, "store_password_secure", return_value="encrypted:token-ref") as store:
            result = vast_setup.validate_cluster(
                cluster="https://vms.example.com:443",
                tenant="default",
                api_token="secret-token",
                auth_method="api_token",
            )

        store.assert_called_once_with("vms.example.com:443", vast_setup.API_TOKEN_USERNAME, "secret-token")
        self.assertEqual(result["cluster"], "vms.example.com:443")
        self.assertEqual(result["auth_method"], "api_token")
        self.assertEqual(result["api_token"], "encrypted:token-ref")
        self.assertEqual(result["username"], vast_setup.API_TOKEN_USERNAME)
        self.assertNotIn("password", result)

    def test_validate_cluster_rejects_api_token_when_clusters_endpoint_fails(self):
        class FakeEndpoint:
            def __init__(self, result=None, error=None):
                self._result = result
                self._error = error

            def get(self, **kwargs):
                if self._error:
                    raise self._error
                return self._result

        class FakeClient:
            def __init__(self, **kwargs):
                self.login = FakeEndpoint({"user_type": "TENANT_ADMIN"})
                self.clusters = FakeEndpoint(error=RuntimeError("Invalid Api Token"))
                self.dashboard = MagicMock()
                self.dashboard.status = FakeEndpoint({"clusters": [{"build": "5.3.0.12"}]})

        with patch.object(vast_setup, "VASTClient", FakeClient), \
                patch.object(vast_setup, "store_password_secure") as store:
            result = vast_setup.validate_cluster(
                cluster="vms.example.com",
                tenant="default",
                api_token="bad-token",
                auth_method="api_token",
            )

        self.assertEqual(result, {})
        store.assert_not_called()


class TestApiTokenRequestHeader(unittest.TestCase):
    def test_patched_request_uses_api_token_authorization_header(self):
        captured = {}

        class FakeResponse:
            status = 200
            data = b'{"ok": true}'
            headers = {"Content-Type": "application/json"}

        class FakePoolManager:
            def request(self, method, url, **kwargs):
                captured.update(kwargs)
                return FakeResponse()

        class FakeSelf:
            _token = "secret-token"
            _user = None
            _password = None
            _tenant = None
            _version = "latest"
            _address = "vms.example.com"
            _url = "api"
            _cert_file = None
            _cert_server_name = None

        with patch.object(client_module, "_create_pool_manager", return_value=FakePoolManager()), \
                patch.object(client_module, "_get_proxy_url", return_value=None):
            result = client_module.VASTClient.request(FakeSelf(), "GET")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(captured["headers"]["authorization"], "Api-Token secret-token")


if __name__ == "__main__":
    unittest.main()
