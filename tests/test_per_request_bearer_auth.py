#!/usr/bin/env python3
"""Tests for per-request VAST credentials (auth_method "per_request", header X-VAST-Bearer).

Also pins the existing behaviour for clusters with stored credentials, which must not change:
Basic auth for username/password, Api-Token for API tokens, the 'client' cache keyed by address
with no TTL, and the X-VAST-Bearer header ignored for them.
"""

import base64
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from vast_admin_mcp import client as client_module
from vast_admin_mcp import request_auth
from vast_admin_mcp.request_auth import PerRequestAuthRequired, parse_bearer_header, uses_per_request_auth

PASSWORD_CLUSTER = {
    "cluster": "pw.example.com",
    "cluster_name": "pw-cluster",
    "username": "admin",
    "password": "base64:" + base64.b64encode(b"secret").decode(),
    "user_type": "SUPER_ADMIN",
    "tenant": "",
}
TOKEN_CLUSTER = {
    "cluster": "tok.example.com",
    "cluster_name": "tok-cluster",
    "auth_method": "api_token",
    "api_token": "base64:" + base64.b64encode(b"stored-api-token").decode(),
    "user_type": "SUPER_ADMIN",
    "tenant": "",
}
PER_REQUEST_CLUSTER = {
    "cluster": "pr.example.com",
    "cluster_name": "cluster-one",
    "auth_method": "per_request",
    "user_type": "SUPER_ADMIN",
    "tenant": "",
}
CONFIG = {"clusters": [PASSWORD_CLUSTER, TOKEN_CLUSTER, PER_REQUEST_CLUSTER]}


class FakeResponse:
    def __init__(self, status=200, data=b"[]"):
        self.status = status
        self.data = data
        self.headers = {"Content-Type": "application/json"}


class FakePoolManager:
    def __init__(self, sink, response=None):
        self.sink = sink
        self.response = response

    def request(self, method, url, headers=None, fields=None, body=None):
        self.sink.append({"method": method, "url": url, "headers": dict(headers or {})})
        return self.response or FakeResponse()


def with_headers(headers):
    return patch.object(request_auth, "_request_headers", return_value={k.lower(): v for k, v in headers.items()})


class Base(unittest.TestCase):
    def setUp(self):
        client_module.clear_client_cache()
        self.sent = []
        self.response = None
        self.patches = [
            patch.object(client_module, "load_config", return_value=CONFIG),
            patch.object(client_module, "_create_pool_manager", side_effect=lambda *a, **k: FakePoolManager(self.sent, self.response)),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        client_module.clear_client_cache()

    def auth_of_get(self, client):
        client.clusters.get()
        return self.sent[-1]["headers"].get("authorization")


class TestHeaderParsing(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(parse_bearer_header(None), {})
        self.assertEqual(parse_bearer_header(""), {})
        self.assertEqual(parse_bearer_header("Cluster-One=abc.def.ghi"), {"cluster-one": "abc.def.ghi"})
        self.assertEqual(parse_bearer_header(" a=1 , b = 2 ,junk, =x, c="), {"a": "1", "b": "2"})

    def test_bearer_prefix_is_ignored(self):
        # Gateways such as LiteLLM (on-behalf-of, upstream_token_header) put "Bearer " before the value.
        self.assertEqual(parse_bearer_header("Bearer c1=tok"), {"c1": "tok"})
        self.assertEqual(parse_bearer_header("bearer c1=tok, c2=t2"), {"c1": "tok", "c2": "t2"})
        self.assertEqual(parse_bearer_header("c1=Bearer tok"), {"c1": "tok"})
        self.assertEqual(parse_bearer_header("Bearer none"), {})

    def test_opt_in_only(self):
        self.assertTrue(uses_per_request_auth(PER_REQUEST_CLUSTER))
        self.assertTrue(uses_per_request_auth({"auth_method": " Per_Request "}))
        self.assertFalse(uses_per_request_auth(PASSWORD_CLUSTER))
        self.assertFalse(uses_per_request_auth(TOKEN_CLUSTER))

    def test_no_http_request_means_no_headers(self):
        # Outside an MCP HTTP request (stdio, CLI) there is nothing to read.
        self.assertEqual(request_auth._request_headers(), {})


class TestStoredCredentialsUnchanged(Base):
    def test_password_cluster_basic_auth_and_header_ignored(self):
        with with_headers({"X-VAST-Bearer": "pw-cluster=should-not-be-used"}):
            c = client_module.create_vast_client("pw.example.com")
            self.assertIs(type(c), client_module.VASTClient)
            self.assertEqual(self.auth_of_get(c), "Basic " + base64.b64encode(b"admin:secret").decode())

    def test_api_token_cluster_keeps_api_token_scheme(self):
        with with_headers({"X-VAST-Bearer": "tok-cluster=should-not-be-used"}):
            c = client_module.create_vast_client("tok.example.com")
            self.assertEqual(self.auth_of_get(c), "Api-Token stored-api-token")

    def test_client_cache_keyed_by_address_without_ttl(self):
        a = client_module.get_or_create_client("pw.example.com")
        b = client_module.get_or_create_client("pw-cluster")
        self.assertIs(a, b)
        cm = client_module._cache_manager
        self.assertIs(cm.get("client", "pw.example.com"), a)
        self.assertIsNone(cm._cache_ttls.get("client"), "the shared client cache keeps no TTL")


class TestPerRequest(Base):
    def test_without_token_refuses_and_never_falls_back(self):
        with with_headers({}):
            with self.assertRaises(PerRequestAuthRequired) as ctx:
                client_module.create_vast_client("cluster-one")
        self.assertIn("X-VAST-Bearer", str(ctx.exception))
        self.assertEqual(self.sent, [], "nothing was sent to the cluster")

    def test_token_for_another_cluster_is_not_used(self):
        with with_headers({"X-VAST-Bearer": "pw-cluster=tok-a"}):
            with self.assertRaises(PerRequestAuthRequired):
                client_module.create_vast_client("pr.example.com")

    def test_bearer_scheme_on_every_endpoint(self):
        with with_headers({"X-VAST-Bearer": "cluster-one=tok-a"}):
            c = client_module.create_vast_client("cluster-one")
            self.assertIsInstance(c, client_module.BearerVASTClient)
            self.assertEqual(self.auth_of_get(c), "Bearer tok-a")
            c.views.get()
            self.assertEqual(self.sent[-1]["headers"]["authorization"], "Bearer tok-a")
            self.assertTrue(self.sent[-1]["url"].startswith("https://pr.example.com/api/"))

    def test_matches_address_too(self):
        with with_headers({"X-VAST-Bearer": "PR.example.com=tok-b"}):
            c = client_module.create_vast_client("pr.example.com")
            self.assertEqual(self.auth_of_get(c), "Bearer tok-b")

    def test_one_client_per_token(self):
        with with_headers({"X-VAST-Bearer": "cluster-one=tok-a"}):
            a1 = client_module.get_or_create_client("cluster-one")
            a2 = client_module.create_vast_client("cluster-one")
        with with_headers({"X-VAST-Bearer": "cluster-one=tok-b"}):
            b = client_module.get_or_create_client("cluster-one")
            self.assertEqual(self.auth_of_get(b), "Bearer tok-b")
        self.assertIs(a1, a2)
        self.assertIsNot(a1, b)
        cm = client_module._cache_manager
        self.assertIsNone(cm.get("client", "pr.example.com"), "never cached under the plain address")
        self.assertEqual(cm._cache_ttls.get("bearer_client"), client_module.PER_REQUEST_CLIENT_TTL_SECONDS)

    def test_name_lookup_never_logs_in_to_per_request_clusters(self):
        with with_headers({}):
            with self.assertRaises(ValueError):
                client_module.resolve_cluster_identifier("var-201", {"clusters": [PER_REQUEST_CLUSTER]})
        self.assertEqual(self.sent, [])


class TestTokenRejection(Base):
    BAD_TOKEN = b'{"detail":"Given token not valid for any token type","code":"token_not_valid"}'

    def test_rejected_bearer_token_raises_clearly(self):
        from vast_admin_mcp.request_auth import PerRequestTokenRejected
        self.response = FakeResponse(403, self.BAD_TOKEN)
        with with_headers({"X-VAST-Bearer": "cluster-one=expired"}):
            c = client_module.create_vast_client("cluster-one")
            with self.assertRaises(PerRequestTokenRejected):
                c.views.get()
        self.response = FakeResponse(401, b"{}")
        with with_headers({"X-VAST-Bearer": "cluster-one=expired2"}):
            with self.assertRaises(PerRequestTokenRejected):
                client_module.create_vast_client("cluster-one").views.get()

    def test_permission_403_with_valid_token_is_a_normal_failure(self):
        from vastpy import RESTFailure
        self.response = FakeResponse(403, b'{"detail":"no permissions","code":"permission_denied"}')
        with with_headers({"X-VAST-Bearer": "cluster-one=ok"}):
            with self.assertRaises(RESTFailure):
                client_module.create_vast_client("cluster-one").activedirectory.get()

    def test_stored_credentials_403_unchanged(self):
        from vastpy import RESTFailure
        self.response = FakeResponse(403, self.BAD_TOKEN)
        with self.assertRaises(RESTFailure):
            client_module.create_vast_client("tok.example.com").views.get()

    def test_executor_reraises_instead_of_empty_list(self):
        from vast_admin_mcp.command_executor import CommandExecutor
        from vast_admin_mcp.request_auth import PerRequestTokenRejected
        self.response = FakeResponse(403, self.BAD_TOKEN)
        with with_headers({"X-VAST-Bearer": "cluster-one=expired"}):
            ex = CommandExecutor.__new__(CommandExecutor)
            ex.client = client_module.create_vast_client("cluster-one")
            ex.template_parser = type("TP", (), {"get_api_whitelist": lambda self: {"views": ["get"]}})()
            with self.assertRaises(PerRequestTokenRejected):
                ex._execute_api_calls("views", {"views": {}})


if __name__ == "__main__":
    unittest.main()
