"""
Cache Manager Module
Manages caching for performance optimization
"""
import polars as pl
from typing import Dict, Any, Optional
import os
import json
import hashlib
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages data caching"""

    def __init__(self, cache_dir: Optional[str] = None, ttl_hours: int = 24):
        if cache_dir:
            self.cache_dir = cache_dir
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.cache_dir = os.path.join(
                os.path.dirname(os.path.dirname(script_dir)),
                '.cache'
            )

        os.makedirs(self.cache_dir, exist_ok=True)
        self.ttl_hours = ttl_hours
        self._memory_cache: Dict[str, Any] = {}

    def _get_cache_key(self, identifier: str) -> str:
        """Generate a cache key from an identifier"""
        return hashlib.md5(identifier.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> str:
        """Get the file path for a cache entry"""
        return os.path.join(self.cache_dir, f"{cache_key}.cache")

    def _get_metadata_path(self, cache_key: str) -> str:
        """Get the metadata path for a cache entry"""
        return os.path.join(self.cache_dir, f"{cache_key}.meta.json")

    def get(
        self,
        identifier: str,
        use_memory: bool = True
    ) -> Optional[Any]:
        """
        Get a cached item.

        Args:
            identifier: Cache identifier
            use_memory: Check memory cache first

        Returns:
            Cached value or None
        """
        try:
            cache_key = self._get_cache_key(identifier)

            # Check memory cache first
            if use_memory and cache_key in self._memory_cache:
                entry = self._memory_cache[cache_key]
                if datetime.now() < entry['expires_at']:
                    return entry['value']
                else:
                    del self._memory_cache[cache_key]

            # Check disk cache
            cache_path = self._get_cache_path(cache_key)
            metadata_path = self._get_metadata_path(cache_key)

            if not os.path.exists(cache_path) or not os.path.exists(metadata_path):
                return None

            # Check expiration
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)

            expires_at = datetime.fromisoformat(metadata['expires_at'])
            if datetime.now() > expires_at:
                self._remove_cache_files(cache_key)
                return None

            # Load cached value
            cache_type = metadata.get('type', 'json')

            if cache_type == 'dataframe':
                value = pl.read_parquet(cache_path)
            else:
                with open(cache_path, 'r') as f:
                    value = json.load(f)

            # Store in memory cache
            if use_memory:
                self._memory_cache[cache_key] = {
                    'value': value,
                    'expires_at': expires_at
                }

            return value

        except Exception as e:
            logger.warning(f"Error getting from cache: {e}")
            return None

    def set(
        self,
        identifier: str,
        value: Any,
        ttl_hours: Optional[int] = None,
        use_memory: bool = True
    ) -> Dict[str, Any]:
        """
        Set a cached item.

        Args:
            identifier: Cache identifier
            value: Value to cache
            ttl_hours: Custom TTL in hours
            use_memory: Also store in memory cache

        Returns:
            Cache result
        """
        try:
            cache_key = self._get_cache_key(identifier)
            cache_path = self._get_cache_path(cache_key)
            metadata_path = self._get_metadata_path(cache_key)

            ttl = ttl_hours or self.ttl_hours
            expires_at = datetime.now() + timedelta(hours=ttl)

            # Determine type and save
            if isinstance(value, pl.DataFrame):
                cache_type = 'dataframe'
                value.write_parquet(cache_path)
            else:
                cache_type = 'json'
                with open(cache_path, 'w') as f:
                    json.dump(value, f)

            # Save metadata
            metadata = {
                'identifier': identifier,
                'type': cache_type,
                'created_at': datetime.now().isoformat(),
                'expires_at': expires_at.isoformat(),
                'ttl_hours': ttl
            }

            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)

            # Store in memory cache
            if use_memory:
                self._memory_cache[cache_key] = {
                    'value': value,
                    'expires_at': expires_at
                }

            return {
                'success': True,
                'cache_key': cache_key,
                'expires_at': expires_at.isoformat()
            }

        except Exception as e:
            logger.error(f"Error setting cache: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def invalidate(self, identifier: str) -> Dict[str, Any]:
        """Invalidate a cache entry"""
        try:
            cache_key = self._get_cache_key(identifier)

            # Remove from memory cache
            if cache_key in self._memory_cache:
                del self._memory_cache[cache_key]

            # Remove disk cache
            self._remove_cache_files(cache_key)

            return {
                'success': True,
                'invalidated': identifier
            }

        except Exception as e:
            logger.error(f"Error invalidating cache: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def clear(self) -> Dict[str, Any]:
        """Clear all cache entries"""
        try:
            # Clear memory cache
            self._memory_cache.clear()

            # Clear disk cache
            count = 0
            for f in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, f)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    count += 1

            return {
                'success': True,
                'files_removed': count
            }

        except Exception as e:
            logger.error(f"Error clearing cache: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            memory_entries = len(self._memory_cache)

            disk_entries = 0
            total_size = 0
            expired_count = 0

            for f in os.listdir(self.cache_dir):
                if f.endswith('.meta.json'):
                    disk_entries += 1
                    metadata_path = os.path.join(self.cache_dir, f)

                    with open(metadata_path, 'r') as mf:
                        metadata = json.load(mf)

                    expires_at = datetime.fromisoformat(metadata['expires_at'])
                    if datetime.now() > expires_at:
                        expired_count += 1

                if f.endswith('.cache'):
                    total_size += os.path.getsize(os.path.join(self.cache_dir, f))

            return {
                'success': True,
                'memory_entries': memory_entries,
                'disk_entries': disk_entries,
                'expired_entries': expired_count,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'cache_dir': self.cache_dir
            }

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def cleanup_expired(self) -> Dict[str, Any]:
        """Remove expired cache entries"""
        try:
            removed = 0

            for f in os.listdir(self.cache_dir):
                if f.endswith('.meta.json'):
                    metadata_path = os.path.join(self.cache_dir, f)

                    with open(metadata_path, 'r') as mf:
                        metadata = json.load(mf)

                    expires_at = datetime.fromisoformat(metadata['expires_at'])
                    if datetime.now() > expires_at:
                        cache_key = f.replace('.meta.json', '')
                        self._remove_cache_files(cache_key)
                        removed += 1

            return {
                'success': True,
                'entries_removed': removed
            }

        except Exception as e:
            logger.error(f"Error cleaning up cache: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _remove_cache_files(self, cache_key: str):
        """Remove cache files for a key"""
        cache_path = self._get_cache_path(cache_key)
        metadata_path = self._get_metadata_path(cache_key)

        if os.path.exists(cache_path):
            os.remove(cache_path)
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
