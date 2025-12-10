"""Cache management for vast-admin-mcp.

Provides a unified caching mechanism to replace global cache variables
with a proper class-based approach that supports TTL and invalidation.
"""

import logging
import time
from typing import Dict, Any, Optional, Callable
from threading import Lock


class CacheManager:
    """Manages in-memory caches with optional TTL and invalidation support.
    
    This class replaces global cache variables with a thread-safe,
    manageable caching system.
    """
    
    def __init__(self):
        """Initialize the cache manager."""
        self._caches: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, Dict[str, float]] = {}
        self._cache_ttls: Dict[str, Optional[float]] = {}
        self._lock = Lock()
    
    def get(self, cache_name: str, key: str, default: Any = None) -> Any:
        """Get a value from a named cache.
        
        Args:
            cache_name: Name of the cache (e.g., 'config', 'whitelist', 'client')
            key: Cache key
            default: Default value if key not found or expired
            
        Returns:
            Cached value or default
        """
        with self._lock:
            if cache_name not in self._caches:
                return default
            
            cache = self._caches[cache_name]
            if key not in cache:
                return default
            
            # Check TTL if set
            ttl = self._cache_ttls.get(cache_name)
            if ttl is not None:
                timestamp = self._cache_timestamps.get(cache_name, {}).get(key, 0)
                if time.time() - timestamp > ttl:
                    # Expired, remove and return default
                    del cache[key]
                    if cache_name in self._cache_timestamps and key in self._cache_timestamps[cache_name]:
                        del self._cache_timestamps[cache_name][key]
                    return default
            
            return cache[key]
    
    def set(self, cache_name: str, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set a value in a named cache.
        
        Args:
            cache_name: Name of the cache
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds (None for no expiration)
        """
        with self._lock:
            if cache_name not in self._caches:
                self._caches[cache_name] = {}
                self._cache_timestamps[cache_name] = {}
            
            self._caches[cache_name][key] = value
            self._cache_timestamps[cache_name][key] = time.time()
            
            # Set TTL for this cache if provided
            if ttl is not None:
                self._cache_ttls[cache_name] = ttl
    
    def clear(self, cache_name: Optional[str] = None) -> None:
        """Clear cache(s).
        
        Args:
            cache_name: Name of cache to clear, or None to clear all caches
        """
        with self._lock:
            if cache_name is None:
                self._caches.clear()
                self._cache_timestamps.clear()
                self._cache_ttls.clear()
            else:
                if cache_name in self._caches:
                    del self._caches[cache_name]
                if cache_name in self._cache_timestamps:
                    del self._cache_timestamps[cache_name]
                if cache_name in self._cache_ttls:
                    del self._cache_ttls[cache_name]
    
    def get_or_set(self, cache_name: str, key: str, factory: Callable[[], Any], ttl: Optional[float] = None) -> Any:
        """Get a value from cache, or set it using a factory function if not present.
        
        This is a common pattern: check cache, if not found, compute value and cache it.
        
        Args:
            cache_name: Name of the cache
            key: Cache key
            factory: Function to call if value not in cache
            ttl: Time-to-live in seconds (None for no expiration)
            
        Returns:
            Cached or newly computed value
        """
        value = self.get(cache_name, key)
        if value is None:
            try:
                value = factory()
                self.set(cache_name, key, value, ttl)
            except Exception as e:
                logging.warning(f"Error computing cache value for {cache_name}.{key}: {e}")
                raise
        return value


# Global cache manager instance
_cache_manager = CacheManager()


def get_cache_manager() -> CacheManager:
    """Get the global cache manager instance.
    
    Returns:
        Global CacheManager instance
    """
    return _cache_manager

