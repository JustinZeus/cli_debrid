import logging
import os
import time
from typing import Any, List, Dict, Tuple

from utilities.plex_api_patches import setup_plex_api_patches

setup_plex_api_patches()

from plexapi.myplex import MyPlexAccount
from utilities.settings import get_setting
from .plex_token_manager import update_token_status

from utilities.watchlist import (
    PlexDetailCache,
    WatchlistFetchCoordinator,
    WatchlistItemProcessor,
)

DB_CONTENT_DIR = os.environ.get("USER_DB_CONTENT", "/user/db_content")
DETAIL_CACHE_FILE = os.path.join(DB_CONTENT_DIR, "plex_detail_cache.json")


def get_plex_client() -> tuple[Any, str] | tuple[None, None]:
    """
    Get Plex client connection and token.

    Returns:
        Tuple of (account, token_string) or (None, None) on failure
    """
    plex_token = get_setting("Plex", "token")
    if not plex_token or not isinstance(plex_token, str):
        plex_token = get_setting("File Management", "plex_token_for_symlink")

    if not plex_token or not isinstance(plex_token, str):
        logging.error("Plex token not configured in settings")
        return None, None

    try:
        account = MyPlexAccount(token=plex_token)
        logging.info(f"Connected to Plex.tv as user: {account.username}")
        return account, plex_token
    except Exception as e:
        logging.error(f"Failed to connect to Plex.tv: {e}")
        return None, None


def get_wanted_from_plex_watchlist(
    versions: Dict[str, bool],
) -> List[Tuple[List[Dict[str, Any]], Dict[str, bool]]]:
    """
    Retrieve and process the main Plex user's watchlist.

    Args:
        versions: Dictionary of version configurations

    Returns:
        List of tuples containing (wanted_items, versions)
    """
    logging.info("Starting Plex watchlist retrieval")

    account, plex_token_str = get_plex_client()
    if not account or not plex_token_str:
        return [([], versions)]

    try:
        initial_watchlist = account.watchlist()
        logging.info(f"Found {len(initial_watchlist)} items in watchlist")

        if not initial_watchlist:
            return [([], versions)]

        # Fetch item details with caching
        detail_cache = PlexDetailCache(DETAIL_CACHE_FILE, max_age_days=30)
        coordinator = WatchlistFetchCoordinator(detail_cache)
        all_fetched_items, fetch_stats = coordinator.fetch_all_item_details(
            initial_watchlist, plex_token_str
        )

        logging.info(
            f"Retrieved {fetch_stats['total_items']} items "
            f"({fetch_stats['cache_hits']} cached, {fetch_stats['fetched_count']} fetched)"
        )

        # Process items into wanted list
        processor = WatchlistItemProcessor(account, account.username)
        wanted_items, process_stats = processor.process_items(all_fetched_items)

        logging.info(
            f"Processed {process_stats['processed']} items "
            f"(skipped: {process_stats['skipped']}, "
            f"removed: {process_stats['removed']}, "
            f"kept: {process_stats['collected_kept']})"
        )

        return [(wanted_items, versions)]

    except Exception as e:
        logging.error(f"Error processing Plex watchlist: {e}", exc_info=True)
        return [([], versions)]


def get_wanted_from_other_plex_watchlist(
    username: str, token: str, versions: Dict[str, bool]
) -> List[Tuple[List[Dict[str, Any]], Dict[str, bool]]]:
    """
    Retrieve and process another Plex user's watchlist.

    Args:
        username: Plex username to fetch watchlist for
        token: Authentication token for the user
        versions: Dictionary of version configurations

    Returns:
        List of tuples containing (wanted_items, versions)
    """
    logging.info(f"Starting watchlist retrieval for user: {username}")

    try:
        account = MyPlexAccount(token=token)

        if account.username != username:
            logging.error(
                f"Token mismatch: expected {username}, got {account.username}"
            )
            return [([], versions)]

        initial_watchlist = account.watchlist()
        logging.info(
            f"User {username}: Found {len(initial_watchlist)} items in watchlist"
        )

        if not initial_watchlist:
            return [([], versions)]

        # Fetch item details with user-specific cache
        cache_file = os.path.join(DB_CONTENT_DIR, f"plex_detail_cache_{username}.json")
        detail_cache = PlexDetailCache(cache_file, max_age_days=30)
        coordinator = WatchlistFetchCoordinator(detail_cache)
        all_fetched_items, fetch_stats = coordinator.fetch_all_item_details(
            initial_watchlist, token
        )

        # Process items into wanted list
        processor = WatchlistItemProcessor(account, account.username)
        wanted_items, process_stats = processor.process_items(all_fetched_items)

        logging.info(
            f"User {username}: Processed {process_stats['processed']} items "
            f"(skipped: {process_stats['skipped']})"
        )

        return [(wanted_items, versions)]

    except Exception as e:
        logging.error(f"Error fetching {username}'s watchlist: {e}", exc_info=True)
        return [([], versions)]


def _validate_single_token(token: str, username: str | None = None) -> Dict[str, Any]:
    """
    Validate a single Plex token.

    Args:
        token: Plex authentication token
        username: Optional expected username for validation

    Returns:
        Dictionary with validation results
    """
    try:
        account = MyPlexAccount(token=token)
        account.ping()

        return {
            "valid": True,
            "expires_at": getattr(account, "rememberExpiresAt", None),
            "username": account.username,
        }
    except Exception as e:
        logging.error(f"Token validation failed for {username or 'main'}: {e}")
        return {"valid": False, "expires_at": None, "username": None}


def validate_plex_tokens() -> Dict[str, Dict[str, Any]]:
    """Validate all configured Plex tokens and return their status."""
    # Import here to avoid circular import
    from queues.config_manager import load_config

    token_status = {}

    # Validate main user's token
    plex_token = get_setting("Plex", "token")
    if plex_token and isinstance(plex_token, str):
        result = _validate_single_token(plex_token)
        token_status["main"] = result
        update_token_status(
            "main",
            result["valid"],
            expires_at=result["expires_at"],
            plex_username=result["username"],
        )
        logging.info(f"Main token validation: {result['valid']}")

    # Validate other users' tokens
    config = load_config()
    content_sources = config.get("Content Sources", {})

    for source_id, source in content_sources.items():
        if source.get("type") == "Other Plex Watchlist":
            username = source.get("username")
            token = source.get("token")

            if username and token and isinstance(token, str):
                result = _validate_single_token(token, username)
                token_status[username] = result
                update_token_status(
                    username,
                    result["valid"],
                    expires_at=result["expires_at"],
                    plex_username=result["username"],
                )
                logging.info(f"Token validation for {username}: {result['valid']}")

    return token_status
