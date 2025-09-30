import logging
import time
from typing import List, Dict, Any, Tuple, Optional
from utilities.settings import get_setting
from database.database_reading import get_media_item_presence
from cli_battery.app.trakt_metadata import TraktMetadata
from cli_battery.app.direct_api import DirectAPI


class WatchlistProcessor:
    """Handles processing of Plex watchlist items."""

    def __init__(self, account, token: str, username: Optional[str] = None):
        self.account = account
        self.token = token
        self.username = username or (account.username if account else None)

    def process_watchlist(
        self, all_fetched_items: List[Dict[str, Any]], versions: Dict[str, bool]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, bool]]:
        """Process all fetched watchlist items.

        Args:
            all_fetched_items: List of items with their fetched details
            versions: Version configuration

        Returns:
            Tuple of (processed_items, versions)
        """
        should_remove = get_setting("Debug", "plex_watchlist_removal", False)
        keep_series = get_setting("Debug", "plex_watchlist_keep_series", False)

        processed_items = []
        stats = {
            "skipped_count": 0,
            "removed_count": 0,
            "collected_skipped": 0,
            "total": len(all_fetched_items),
        }

        processing_start_time = time.time()

        for item_details in all_fetched_items:
            result = self._process_single_item(item_details, should_remove, keep_series)

            if result is None:
                continue

            if result == "skipped":
                stats["skipped_count"] += 1
            elif result == "removed":
                stats["removed_count"] += 1
            elif result == "collected_skipped":
                stats["collected_skipped"] += 1
            else:  # It's a wanted item
                processed_items.append(result)

        self._log_stats(stats, processing_start_time, processed_items)
        return processed_items, versions

    def _process_single_item(
        self, item_details: Dict[str, Any], should_remove: bool, keep_series: bool
    ) -> Optional[Any]:
        """Process a single watchlist item.

        Returns:
            - Dict for wanted items
            - "skipped", "removed", or "collected_skipped" for special cases
            - None for errors
        """
        original_plex_item = item_details["original_plex_item"]
        title = original_plex_item.title

        # Handle fetch errors
        if item_details.get("error"):
            logging.warning(
                f"Skipping item '{title}' due to error: {item_details['error']}"
            )
            return "skipped"

        # Extract IDs and media type
        imdb_id = item_details["imdb_id"]
        tmdb_id = item_details["tmdb_id"]
        media_type = (
            item_details["media_type"]
            if item_details["media_type"]
            else original_plex_item.type
        )

        # Try TMDB to IMDB conversion if needed
        if not imdb_id and tmdb_id and media_type:
            imdb_id = self._convert_tmdb_to_imdb(tmdb_id, media_type, title)

        if not imdb_id:
            logging.debug(f"Skipping '{title}' - no IMDB ID found")
            return "skipped"

        # Normalize media type
        if media_type == "show":
            media_type = "tv"

        # Check if item should be removed
        if should_remove:
            removal_result = self._handle_removal(
                original_plex_item, imdb_id, media_type, title, keep_series
            )
            if removal_result:
                return removal_result

        # Create wanted item
        return {
            "imdb_id": imdb_id,
            "media_type": media_type,
            "content_source_detail": self.username,
        }

    def _convert_tmdb_to_imdb(
        self, tmdb_id: str, media_type: str, title: str
    ) -> Optional[str]:
        """Convert TMDB ID to IMDB ID."""
        logging.info(
            f"No IMDB ID for '{title}', attempting TMDB ({tmdb_id}) conversion"
        )

        try:
            api = DirectAPI()
            converted_id, source = api.tmdb_to_imdb(tmdb_id, media_type=media_type)

            if converted_id:
                logging.info(
                    f"Converted TMDB {tmdb_id} to IMDB {converted_id} via {source}"
                )
                return converted_id
            else:
                logging.warning(f"TMDB to IMDB conversion failed for '{title}'")

        except Exception as e:
            logging.error(f"Error converting TMDB to IMDB for '{title}': {e}")

        return None

    def _handle_removal(
        self, plex_item, imdb_id: str, media_type: str, title: str, keep_series: bool
    ) -> Optional[str]:
        """Handle removal logic for collected items."""
        item_state = get_media_item_presence(imdb_id=imdb_id)

        if item_state != "Collected":
            return None

        # Check if we should keep this item
        if media_type == "tv":
            if keep_series:
                logging.debug(
                    f"Keeping collected series '{title}' - keep_series enabled"
                )
                return "collected_skipped"

            show_status = self._get_show_status(imdb_id)
            if show_status != "ended":
                logging.debug(
                    f"Keeping ongoing series '{title}' - status: {show_status}"
                )
                return "collected_skipped"

        # Try to remove from watchlist
        try:
            self.account.removeFromWatchlist([plex_item])
            logging.info(f"Removed '{title}' (IMDB: {imdb_id}) from watchlist")
            return "removed"
        except Exception as e:
            logging.error(f"Failed to remove '{title}' from watchlist: {e}")
            return None

    def _get_show_status(self, imdb_id: str) -> str:
        """Get TV show status from Trakt."""
        try:
            trakt = TraktMetadata()
            search_result = trakt._search_by_imdb(imdb_id)

            if search_result and search_result["type"] == "show":
                show = search_result["show"]
                slug = show["ids"]["slug"]
                url = f"{trakt.base_url}/shows/{slug}?extended=full"
                response = trakt._make_request(url)

                if response and response.status_code == 200:
                    status = response.json().get("status", "").lower()
                    return "ended" if status == "canceled" else status

        except Exception as e:
            logging.error(f"Error getting show status for {imdb_id}: {e}")

        return ""

    def _log_stats(self, stats: Dict, start_time: float, processed_items: List):
        """Log processing statistics."""
        prefix = (
            f"User {self.username}: " if self.username != self.account.username else ""
        )

        logging.info(f"{prefix}Processing took {time.time() - start_time:.4f} seconds")
        logging.info(
            f"{prefix}Successfully processed: "
            f"{stats['total'] - stats['skipped_count'] - stats['collected_skipped'] - stats['removed_count']}"
        )
        logging.info(f"{prefix}Skipped (no IMDB): {stats['skipped_count']}")
        logging.info(f"{prefix}Removed: {stats['removed_count']}")
        logging.info(f"{prefix}Kept (collected): {stats['collected_skipped']}")
        logging.info(f"{prefix}Added to wanted: {len(processed_items)}")
