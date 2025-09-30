"""
Coordinates the fetching and caching of Plex watchlist item details.

Manages the orchestration between cache lookups and async API fetches.
"""

import logging
import asyncio
import time
from typing import List, Dict, Any, Tuple

from .cache import PlexDetailCache
from .fetcher import run_async_fetches


logger = logging.getLogger(__name__)


class WatchlistFetchCoordinator:
    """Coordinates cached and fresh fetches of Plex watchlist item details."""

    def __init__(self, cache: PlexDetailCache):
        """
        Initialize the fetch coordinator.

        Args:
            cache: PlexDetailCache instance to use for caching
        """
        self.cache = cache

    def fetch_all_item_details(
        self, watchlist_items: List[Any], plex_token: str
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        Fetch details for all watchlist items, using cache when possible.

        Args:
            watchlist_items: List of Plex item objects from watchlist
            plex_token: Plex authentication token

        Returns:
            Tuple of (all_items_with_details, stats_dict) where stats contains
            cache_hits, fetched_count, and errors
        """
        items_to_fetch = []
        cached_items = []

        # Separate cached from items needing fetch
        for item_obj in watchlist_items:
            cached_details = self.cache.get(item_obj)

            if cached_details:
                cached_items.append(
                    self._reconstruct_item_from_cache(item_obj, cached_details)
                )
            else:
                fetch_item = self._prepare_item_for_fetch(item_obj)
                if fetch_item:
                    items_to_fetch.append(fetch_item)

        logger.info(
            f"Cache hit for {len(cached_items)} items, "
            f"need to fetch {len(items_to_fetch)} items"
        )

        # Fetch uncached items
        fetched_items = []
        if items_to_fetch:
            fetched_items = self._fetch_and_cache_items(items_to_fetch, plex_token)

        # Combine results
        all_items = cached_items + fetched_items

        stats = {
            "cache_hits": len(cached_items),
            "fetched_count": len(fetched_items),
            "total_items": len(all_items),
        }

        return all_items, stats

    def _reconstruct_item_from_cache(
        self, item_obj: Any, cached_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Reconstruct item details dictionary from cache."""
        return {
            "imdb_id": cached_details.get("imdb_id"),
            "tmdb_id": cached_details.get("tmdb_id"),
            "media_type": cached_details.get("media_type"),
            "original_plex_item": item_obj,
            "error": cached_details.get("error"),
        }

    def _prepare_item_for_fetch(self, item_obj: Any) -> Dict[str, Any] | None:
        """
        Prepare a Plex item for async fetching.

        Returns None if item cannot be fetched (missing required attributes).
        """
        if not (
            hasattr(item_obj, "key") and item_obj.key and hasattr(item_obj, "_server")
        ):
            logger.warning(
                f"Skipping item {getattr(item_obj, 'title', 'Unknown Title')} "
                f"due to missing key or _server attribute"
            )
            return None

        try:
            details_url = item_obj._server.url(item_obj.key)
            return {
                "title": item_obj.title,
                "url": details_url,
                "original_plex_item": item_obj,
            }
        except Exception as e:
            logger.error(f"Error constructing details URL for {item_obj.title}: {e}")
            return None

    def _fetch_and_cache_items(
        self, items_to_fetch: List[Dict[str, Any]], plex_token: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch items via async requests and update cache.

        Args:
            items_to_fetch: List of item dicts prepared for fetching
            plex_token: Plex authentication token

        Returns:
            List of fetched item details
        """
        start_time = time.time()

        fetched_data_list = asyncio.run(run_async_fetches(items_to_fetch, plex_token))

        elapsed = time.time() - start_time
        logger.info(
            f"Async fetching of {len(items_to_fetch)} item details "
            f"took {elapsed:.4f} seconds"
        )

        # Update cache with newly fetched items
        for item_details in fetched_data_list:
            original_item = item_details["original_plex_item"]
            cache_entry = {
                "imdb_id": item_details.get("imdb_id"),
                "tmdb_id": item_details.get("tmdb_id"),
                "media_type": item_details.get("media_type"),
                "error": item_details.get("error"),
            }
            self.cache.set(original_item, cache_entry)

        # Save cache to disk
        self.cache.commit()
        logger.info(
            f"Updated cache with {len(fetched_data_list)} new entries. "
            f"Total cache size: {self.cache.stats()['total_entries']} entries"
        )

        return fetched_data_list
