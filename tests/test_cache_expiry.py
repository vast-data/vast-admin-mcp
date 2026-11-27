"""CacheManager must not keep expired entries whose key is never read again."""

import unittest
from unittest import mock

from vast_admin_mcp.cache import CacheManager


class TestCacheExpiry(unittest.TestCase):
    def test_expired_entries_are_evicted_on_write(self):
        cm = CacheManager()
        with mock.patch("vast_admin_mcp.cache.time.time", return_value=1000.0):
            for i in range(50):
                cm.set("bearer_client", f"cluster#token{i}", object(), ttl=3600)
        # One hour later every old token has rotated; a new client is cached for the new token.
        with mock.patch("vast_admin_mcp.cache.time.time", return_value=1000.0 + 3601):
            cm.set("bearer_client", "cluster#token-new", object(), ttl=3600)
        self.assertEqual(list(cm._caches["bearer_client"]), ["cluster#token-new"])
        self.assertEqual(list(cm._cache_timestamps["bearer_client"]), ["cluster#token-new"])

    def test_live_entries_are_kept(self):
        cm = CacheManager()
        with mock.patch("vast_admin_mcp.cache.time.time", return_value=1000.0):
            cm.set("bearer_client", "a", 1, ttl=3600)
        with mock.patch("vast_admin_mcp.cache.time.time", return_value=1000.0 + 10):
            cm.set("bearer_client", "b", 2, ttl=3600)
            self.assertEqual(cm.get("bearer_client", "a"), 1)
            self.assertEqual(cm.get("bearer_client", "b"), 2)

    def test_cache_without_ttl_is_untouched(self):
        cm = CacheManager()
        with mock.patch("vast_admin_mcp.cache.time.time", return_value=1000.0):
            cm.set("client", "a", 1)
        with mock.patch("vast_admin_mcp.cache.time.time", return_value=10**9):
            cm.set("client", "b", 2)
            self.assertEqual(cm.get("client", "a"), 1)


if __name__ == "__main__":
    unittest.main()
