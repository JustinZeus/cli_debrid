"""
Process watchlist items and determine which should be added to wanted list.

Handles ID validation, conversion, state checking, and watchlist removal logic.
"""

import logging
import time
from typing import Dict, Any, List, Optional

from database.database_reading import get_media_item_presence
from utilities.settings import get_setting
from cli_battery.app.trakt_metadata import TraktMetadata
from .id_conversion import tmdb_to_imdb


logger = logging.getLogger(__name__)


class WatchlistItemProcessor:
    """Processes fetched watchlist items into wanted items list."""

    def __init__(self, account, username: str):
        """
        Initialize the processor.

        Args:
            account: MyPlexAccount instance for watchlist removal
            username: Username for logging purposes
        """
        self.account = account
        self.username = username
        self.removal_enabled = get_setting("Debug", "plex_watchlist_removal", False)
        self.keep_series = get_setting("Debug", "plex_watchlist_keep_series", False)

    def process_items(
        self, fetched_items: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
        """Process fetched items into wanted items list."""
        wanted_items = []
        stats = {"skipped": 0, "removed": 0, "collected_kept": 0, "processed": 0}

        start_time = time.time()

        # Extract all IMDB IDs upfront
        imdb_ids = [
            item["imdb_id"]
            for item in fetched_items
            if item.get("imdb_id") and not item.get("error")
        ]

        # Single batch DB query for all presence checks
        presence_map = self._get_batch_presence(imdb_ids)

        for item_details in fetched_items:
            result = self._process_single_item(item_details, stats, presence_map)
            if result:
                wanted_items.append(result)

        elapsed = time.time() - start_time
        logger.info(
            f"Processing loop for {len(fetched_items)} items took {elapsed:.4f} seconds"
        )

        stats["processed"] = len(wanted_items)
        return wanted_items, stats

    def _process_single_item(
        self,
        item_details: Dict[str, Any],
        stats: Dict[str, int],
        presence_map: Dict[str, str],
    ) -> Optional[Dict[str, Any]]:
        """Process a single item and return wanted item dict or None."""
        original_plex_item = item_details["original_plex_item"]
        title = original_plex_item.title

        # Skip items with fetch errors
        if item_details.get("error"):
            logger.warning(
                f"Skipping item '{title}' due to error: {item_details['error']}"
            )
            stats["skipped"] += 1
            return None

        # Get or convert IMDB ID
        imdb_id = item_details["imdb_id"]
        tmdb_id = item_details["tmdb_id"]
        media_type = item_details["media_type"] or original_plex_item.type

        if not imdb_id and tmdb_id and media_type:
            converted_imdb_id, _ = tmdb_to_imdb(tmdb_id, media_type, title)
            if converted_imdb_id:
                imdb_id = converted_imdb_id

        if not imdb_id:
            stats["skipped"] += 1
            logger.debug(
                f"Skipping item '{title}' - no IMDB ID found after conversion attempts"
            )
            return None

        # Normalize media type
        media_type = self._normalize_media_type(media_type)

        # Check if item should be removed from watchlist
        item_state = presence_map.get(imdb_id, "Not Found")
        logger.debug(f"Item '{title}' (IMDB: {imdb_id}) - Presence: {item_state}")

        if self._should_remove_item(item_state, media_type, imdb_id, title):
            if self._remove_from_watchlist(original_plex_item, title, imdb_id):
                stats["removed"] += 1
                return None
        elif item_state == "Collected" and self.removal_enabled:
            stats["collected_kept"] += 1

        # Build wanted item
        logger.debug(
            f"Added '{title}' (IMDB: {imdb_id}, Type: {media_type}) "
            f"to processed items from source: {self.username}"
        )

        return {
            "imdb_id": imdb_id,
            "media_type": media_type,
            "content_source_detail": self.username,
        }

    @staticmethod
    def _normalize_media_type(media_type: str) -> str:
        """Normalize media type to standard format."""
        return "tv" if media_type == "show" else media_type

    def _should_remove_item(
        self, item_state: str, media_type: str, imdb_id: str, title: str
    ) -> bool:
        """Determine if a collected item should be removed from watchlist."""
        if item_state != "Collected" or not self.removal_enabled:
            return False

        if media_type == "tv":
            if self.keep_series:
                logger.debug(
                    f"Keeping collected TV series: '{title}' (IMDB: {imdb_id}) - "
                    "keep_series is enabled"
                )
                return False

            show_status = self._get_show_status(imdb_id)
            if show_status != "ended":
                logger.debug(
                    f"Keeping collected ongoing TV series: '{title}' "
                    f"(IMDB: {imdb_id}) - status: {show_status}"
                )
                return False

            logger.debug(
                f"Identified collected and ended TV series for removal: '{title}' "
                f"(IMDB: {imdb_id}) - status: {show_status}"
            )
            return True
        else:
            logger.debug(
                f"Identified collected movie for removal: '{title}' (IMDB: {imdb_id})"
            )
            return True

    @staticmethod
    def _get_show_status(imdb_id: str) -> str:
        """
        Get the status of a TV show from Trakt.

        Returns:
            Status string ('ended', 'returning series', etc.) or empty string on error
        """
        start_time = time.time()
        try:
            trakt = TraktMetadata()
            search_result = trakt._search_by_imdb(imdb_id)
            if search_result and search_result["type"] == "show":
                show = search_result["show"]
                slug = show["ids"]["slug"]

                url = f"{trakt.base_url}/shows/{slug}?extended=full"
                response = trakt._make_request(url)
                if response and response.status_code == 200:
                    show_data = response.json()
                    status = show_data.get("status", "").lower()
                    logger.debug(
                        f"Getting show status for {imdb_id} took "
                        f"{time.time() - start_time:.4f} seconds. Status: {status}"
                    )
                    return "ended" if status == "canceled" else status
        except Exception as e:
            logger.error(f"Error getting show status for {imdb_id}: {str(e)}")
            logger.debug(
                f"Getting show status for {imdb_id} took "
                f"{time.time() - start_time:.4f} seconds before error."
            )
        return ""

    def _get_batch_presence(self, imdb_ids: List[str]) -> Dict[str, str]:
        """Batch query for media item presence."""
        from database.database_reading import get_media_items_presence_batch

        start = time.time()
        try:
            result = get_media_items_presence_batch(imdb_ids)
            elapsed = time.time() - start
            logger.info(
                f"Batch presence check for {len(imdb_ids)} items completed in {elapsed:.3f}s"
            )
            return result
        except Exception as e:
            logger.error(f"Batch presence check failed: {e}")
            return {}

    def _remove_from_watchlist(self, plex_item: Any, title: str, imdb_id: str) -> bool:
        """
        Attempt to remove item from watchlist.

        Returns:
            True if removal succeeded, False otherwise
        """
        try:
            start_time = time.time()
            self.account.removeFromWatchlist([plex_item])
            elapsed = time.time() - start_time
            logger.info(
                f"Successfully removed '{title}' (IMDB: {imdb_id}) "
                f"from watchlist. Took {elapsed:.4f}s"
            )
            return True
        except Exception as e:
            logger.error(
                f"Failed to remove '{title}' (IMDB: {imdb_id}) from watchlist: {e}"
            )
            return False
