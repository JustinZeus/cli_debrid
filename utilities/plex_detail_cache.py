import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Any


class PlexDetailCache:
    def __init__(
        self, cache_file: str, max_age_days: int = 30, max_entries: int = 10000
    ):
        self.cache_file = cache_file
        self.max_age = timedelta(days=max_age_days)
        self.max_entries = max_entries
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict:
        """Load cache from disk, return empty dict if file doesn't exist or is corrupted."""
        if not os.path.exists(self.cache_file):
            return {}

        try:
            with open(self.cache_file, "r") as f:
                cache = json.load(f)
                # Validate cache structure
                if isinstance(cache, dict):
                    return cache
                return {}
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Cache file corrupted or unreadable, starting fresh: {e}")
            return {}

    def _save_cache(self):
        """Save cache to disk."""
        try:
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
            with open(self.cache_file, "w") as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logging.error(f"Failed to save cache: {e}")

    def _make_cache_key(self, item) -> Optional[str]:
        """Generate a unique cache key from a Plex item."""
        try:
            # Use guid if available, otherwise use title + type as fallback
            if hasattr(item, "guid") and item.guid:
                return item.guid
            elif hasattr(item, "title") and hasattr(item, "type"):
                return f"{item.type}:{item.title}"
            return None
        except Exception:
            return None

    def get(self, item) -> Optional[Dict]:
        """Get cached details for an item if they exist and aren't expired."""
        cache_key = self._make_cache_key(item)
        if not cache_key:
            return None

        if cache_key in self.cache:
            entry = self.cache[cache_key]
            # Check if entry is expired
            if "cached_at" in entry:
                cached_time = datetime.fromisoformat(entry["cached_at"])
                if datetime.now() - cached_time < self.max_age:
                    return entry
            # Expired or no timestamp, remove it
            del self.cache[cache_key]
        return None

    def set(self, item, details: Dict):
        """Cache details for an item."""
        cache_key = self._make_cache_key(item)
        if not cache_key:
            return

        # Enforce max entries limit (remove oldest entries if needed)
        if len(self.cache) >= self.max_entries:
            # Remove 10% of oldest entries
            entries_to_remove = max(1, self.max_entries // 10)
            sorted_keys = sorted(
                self.cache.keys(),
                key=lambda k: self.cache[k].get("cached_at", ""),
            )
            for key in sorted_keys[:entries_to_remove]:
                del self.cache[key]

        # Add timestamp to details
        details["cached_at"] = datetime.now().isoformat()
        self.cache[cache_key] = details

    def commit(self):
        """Save cache to disk."""
        self._save_cache()

    def stats(self) -> Dict:
        """Return cache statistics."""
        return {"total_entries": len(self.cache), "cache_file": self.cache_file}
