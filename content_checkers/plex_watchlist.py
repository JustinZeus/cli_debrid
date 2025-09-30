import logging
import os
import time
from typing import List, Dict, Any, Tuple
from plexapi.myplex import MyPlexAccount
from utilities.settings import get_setting
from queues.config_manager import load_config
from .plex_token_manager import update_token_status
from utilities.plex_watchlist import PlexDetailCache, apply_patches
from utilities.plex_watchlist.processor import WatchlistProcessor
from utilities.plex_watchlist.fetcher import run_async_fetches

# Apply API patches at module load
apply_patches()

# Get db_content directory from environment variable with fallback
DB_CONTENT_DIR = os.environ.get("USER_DB_CONTENT", "/user/db_content")
DETAIL_CACHE_FILE = os.path.join(DB_CONTENT_DIR, "plex_detail_cache.json")


def get_plex_client():
    """Get authenticated Plex client and token."""
    start_time = time.time()

    plex_token = get_setting("Plex", "token")
    if not plex_token:
        plex_token = get_setting("File Management", "plex_token_for_symlink")

    if not plex_token:
        logging.error("Plex token not configured")
        return None, None

    try:
        logging.info("Connecting to Plex.tv cloud service")
        account = MyPlexAccount(token=plex_token)
        logging.info(f"Connected as user: {account.username}")
        logging.info(f"Connection took {time.time() - start_time:.4f} seconds")
        return account, plex_token
    except Exception as e:
        logging.error(f"Error connecting to Plex: {e}")
        return None, None


def get_wanted_from_plex_watchlist(
    versions: Dict[str, bool],
) -> List[Tuple[List[Dict[str, Any]], Dict[str, bool]]]:
    """Get wanted items from main Plex watchlist."""
    overall_start_time = time.time()

    logging.info("Starting Plex watchlist retrieval")

    account, plex_token_str = get_plex_client()
    if not account or not plex_token_str:
        logging.error("Failed to get Plex client or token")
        return [([], versions)]

    try:
        # Get watchlist
        initial_watchlist = account.watchlist()
        logging.info(f"Found {len(initial_watchlist)} items in watchlist")

        if not initial_watchlist:
            return [([], versions)]

        # Initialize cache
        detail_cache = PlexDetailCache(DETAIL_CACHE_FILE, max_age_days=30)

        items_to_process_async = []
        cached_items = []

        # Check cache for each item
        for item_obj in initial_watchlist:
            cached_details = detail_cache.get(item_obj)
            if cached_details:
                cached_items.append(
                    {
                        "imdb_id": cached_details.get("imdb_id"),
                        "tmdb_id": cached_details.get("tmdb_id"),
                        "media_type": cached_details.get("media_type"),
                        "original_plex_item": item_obj,
                        "error": cached_details.get("error"),
                    }
                )
            else:
                if (
                    hasattr(item_obj, "key")
                    and item_obj.key
                    and hasattr(item_obj, "_server")
                ):
                    try:
                        details_url = item_obj._server.url(item_obj.key)
                        items_to_process_async.append(
                            {
                                "title": item_obj.title,
                                "url": details_url,
                                "original_plex_item": item_obj,
                            }
                        )
                    except Exception as e:
                        logging.error(
                            f"Error constructing URL for {item_obj.title}: {e}"
                        )

        logging.info(
            f"Cache hit for {len(cached_items)} items, "
            f"need to fetch {len(items_to_process_async)} items"
        )

        # Fetch uncached items
        fetched_data_list = []
        if items_to_process_async:
            fetched_data_list = run_async_fetches(
                items_to_process_async, plex_token_str
            )

            # Update cache
            for item_details in fetched_data_list:
                cache_entry = {
                    "imdb_id": item_details.get("imdb_id"),
                    "tmdb_id": item_details.get("tmdb_id"),
                    "media_type": item_details.get("media_type"),
                    "error": item_details.get("error"),
                }
                detail_cache.set(item_details["original_plex_item"], cache_entry)

            detail_cache.commit()
            logging.info(f"Updated cache with {len(fetched_data_list)} new entries")

        # Process all items
        all_fetched_items = cached_items + fetched_data_list

        processor = WatchlistProcessor(account, plex_token_str)
        processed_items, versions = processor.process_watchlist(
            all_fetched_items, versions
        )

        logging.info(
            f"Watchlist processing complete in {time.time() - overall_start_time:.4f} seconds"
        )

        return [(processed_items, versions)]

    except Exception as e:
        logging.error(f"Error processing Plex watchlist: {e}", exc_info=True)
        return [([], versions)]


def get_wanted_from_other_plex_watchlist(
    username: str, token: str, versions: Dict[str, bool]
) -> List[Tuple[List[Dict[str, Any]], Dict[str, bool]]]:
    """Get wanted items from another user's Plex watchlist."""
    overall_start_time = time.time()

    logging.info(f"Starting watchlist retrieval for user: {username}")

    try:
        account = MyPlexAccount(token=token)

        if account.username != username:
            logging.error(
                f"Token mismatch: expected {username}, got {account.username}"
            )
            return [([], versions)]

        initial_watchlist = account.watchlist()
        logging.info(f"Found {len(initial_watchlist)} items for {username}")

        if not initial_watchlist:
            return [([], versions)]

        # Use separate cache file per user
        cache_file = os.path.join(DB_CONTENT_DIR, f"plex_detail_cache_{username}.json")
        detail_cache = PlexDetailCache(cache_file, max_age_days=30)

        items_to_process_async = []
        cached_items = []

        # Check cache
        for item_obj in initial_watchlist:
            cached_details = detail_cache.get(item_obj)
            if cached_details:
                cached_items.append(
                    {
                        "imdb_id": cached_details.get("imdb_id"),
                        "tmdb_id": cached_details.get("tmdb_id"),
                        "media_type": cached_details.get("media_type"),
                        "original_plex_item": item_obj,
                        "error": cached_details.get("error"),
                    }
                )
            else:
                if (
                    hasattr(item_obj, "key")
                    and item_obj.key
                    and hasattr(item_obj, "_server")
                ):
                    try:
                        details_url = item_obj._server.url(item_obj.key)
                        items_to_process_async.append(
                            {
                                "title": item_obj.title,
                                "url": details_url,
                                "original_plex_item": item_obj,
                            }
                        )
                    except Exception as e:
                        logging.error(f"User {username}: Error constructing URL: {e}")

        logging.info(
            f"User {username}: Cache hit {len(cached_items)}, "
            f"fetching {len(items_to_process_async)}"
        )

        # Fetch uncached items
        fetched_data_list = []
        if items_to_process_async:
            fetched_data_list = run_async_fetches(items_to_process_async, token)

            # Update cache
            for item_details in fetched_data_list:
                cache_entry = {
                    "imdb_id": item_details.get("imdb_id"),
                    "tmdb_id": item_details.get("tmdb_id"),
                    "media_type": item_details.get("media_type"),
                    "error": item_details.get("error"),
                }
                detail_cache.set(item_details["original_plex_item"], cache_entry)

            detail_cache.commit()

        # Process all items
        all_fetched_items = cached_items + fetched_data_list

        processor = WatchlistProcessor(account, token, username)
        processed_items, versions = processor.process_watchlist(
            all_fetched_items, versions
        )

        logging.info(
            f"User {username} processing complete in "
            f"{time.time() - overall_start_time:.4f} seconds"
        )

        return [(processed_items, versions)]

    except Exception as e:
        logging.error(f"Error processing {username}'s watchlist: {e}", exc_info=True)
        return [([], versions)]


def validate_plex_tokens():
    """Validate all Plex tokens and return their status."""
    overall_start_time = time.time()
    token_status = {}

    # Validate main token
    try:
        plex_token = get_setting("Plex", "token")
        if plex_token:
            account = MyPlexAccount(token=plex_token)
            account.ping()

            token_status["main"] = {
                "valid": True,
                "expires_at": getattr(account, "rememberExpiresAt", None),
                "username": account.username,
            }

            update_token_status(
                "main",
                True,
                expires_at=getattr(account, "rememberExpiresAt", None),
                plex_username=account.username,
            )

            logging.info(f"Main token valid for user: {account.username}")

    except Exception as e:
        logging.error(f"Error validating main token: {e}")
        token_status["main"] = {"valid": False, "expires_at": None, "username": None}
        update_token_status("main", False)

    # Validate other users' tokens
    config = load_config()
    content_sources = config.get("Content Sources", {})

    for source_id, source in content_sources.items():
        if source.get("type") == "Other Plex Watchlist":
            username = source.get("username")
            token = source.get("token")

            if username and token:
                try:
                    account = MyPlexAccount(token=token)
                    account.ping()

                    token_status[username] = {
                        "valid": True,
                        "expires_at": getattr(account, "rememberExpiresAt", None),
                        "username": account.username,
                    }

                    update_token_status(
                        username,
                        True,
                        expires_at=getattr(account, "rememberExpiresAt", None),
                        plex_username=account.username,
                    )

                    logging.info(f"Token valid for user {username}")

                except Exception as e:
                    logging.error(f"Error validating token for {username}: {e}")
                    token_status[username] = {
                        "valid": False,
                        "expires_at": None,
                        "username": None,
                    }
                    update_token_status(username, False)

    logging.info(
        f"Token validation complete in {time.time() - overall_start_time:.4f} seconds"
    )
    return token_status
