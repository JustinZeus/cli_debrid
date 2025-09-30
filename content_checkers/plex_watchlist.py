import logging
import os

# --- Temporary logging setup for debugging the patch ---
LOG_FLAG_PATCH = "[PLEX_PATCH_DEBUG]"
patch_logger = logging.getLogger("plex_patch_debugger")
patch_logger.setLevel(logging.INFO)
log_dir_debug = os.environ.get("USER_LOGS", "/user/logs")
os.makedirs(log_dir_debug, exist_ok=True)
debug_log_path = os.path.join(log_dir_debug, "patch_debug.log")
fh = logging.FileHandler(debug_log_path)
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
patch_logger.addHandler(fh)
patch_logger.propagate = False

patch_logger.info(f"{LOG_FLAG_PATCH} Logger initialized in plex_watchlist.py")
# --- End temporary logging setup ---

patch_logger.info(
    f"{LOG_FLAG_PATCH} Attempting to patch plexapi query methods for logging..."
)
try:
    import plexapi.myplex

    # Patch PlexServer.query
    original_plexserver_query = plexapi.myplex.PlexServer.query

    def patched_plexserver_query(
        self, key, method=None, headers=None, params=None, timeout=None, **kwargs
    ):
        # Fix the endpoint URL if it's using the old metadata.provider.plex.tv
        if "metadata.provider.plex.tv" in str(key):
            fixed_key = str(key).replace(
                "metadata.provider.plex.tv", "discover.provider.plex.tv"
            )
            patch_logger.info(
                f"{LOG_FLAG_PATCH} PLEXSERVER QUERY: FIXED ENDPOINT - Original: {key} -> Fixed: {fixed_key}"
            )
            patch_logger.info(
                f"{LOG_FLAG_PATCH} PLEXSERVER QUERY: method={method}, params={params}, kwargs={kwargs}"
            )
            return original_plexserver_query(
                self, fixed_key, method, headers, params, timeout, **kwargs
            )
        else:
            patch_logger.info(
                f"{LOG_FLAG_PATCH} PLEXSERVER QUERY: key={key}, method={method}, params={params}, kwargs={kwargs}"
            )
            return original_plexserver_query(
                self, key, method, headers, params, timeout, **kwargs
            )

    plexapi.myplex.PlexServer.query = patched_plexserver_query
    patch_logger.info(
        f"{LOG_FLAG_PATCH} SUCCESS: Patched plexapi.myplex.PlexServer.query to log requests."
    )

    # Patch MyPlexAccount.query
    original_myplex_query = plexapi.myplex.MyPlexAccount.query

    def patched_myplex_query(
        self, url, method=None, headers=None, timeout=None, **kwargs
    ):
        # Fix the endpoint URL if it's using the old metadata.provider.plex.tv
        if "metadata.provider.plex.tv" in url:
            fixed_url = url.replace(
                "metadata.provider.plex.tv", "discover.provider.plex.tv"
            )
            patch_logger.info(
                f"{LOG_FLAG_PATCH} MYPLEX QUERY: FIXED ENDPOINT - Original: {url} -> Fixed: {fixed_url}"
            )
            patch_logger.info(
                f"{LOG_FLAG_PATCH} MYPLEX QUERY: method={method}, kwargs={kwargs}"
            )
            return original_myplex_query(
                self, fixed_url, method, headers, timeout, **kwargs
            )
        else:
            patch_logger.info(
                f"{LOG_FLAG_PATCH} MYPLEX QUERY: url={url}, method={method}, kwargs={kwargs}"
            )
            return original_myplex_query(self, url, method, headers, timeout, **kwargs)

    plexapi.myplex.MyPlexAccount.query = patched_myplex_query
    patch_logger.info(
        f"{LOG_FLAG_PATCH} SUCCESS: Patched plexapi.myplex.MyPlexAccount.query to log requests."
    )

except (ImportError, AttributeError) as e:
    patch_logger.error(
        f"{LOG_FLAG_PATCH} FAILED: Could not patch plexapi query methods. Error: {e}",
        exc_info=True,
    )

patch_logger.info(f"{LOG_FLAG_PATCH} plex_watchlist.py module execution continues...")

from plexapi.myplex import MyPlexAccount

patch_logger.info(f"{LOG_FLAG_PATCH} Imported MyPlexAccount from plexapi.myplex.")

from typing import List, Dict, Any, Tuple
from utilities.settings import get_setting
from database.database_reading import get_media_item_presence
from queues.config_manager import load_config
from cli_battery.app.trakt_metadata import TraktMetadata
import os
import pickle
from datetime import datetime, timedelta
from .plex_token_manager import update_token_status, get_token_status
import time
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from utilities.watchlist import (
    PlexDetailCache,
    fetch_item_details_and_extract_ids,
    run_async_fetches,
    tmdb_to_imdb,
    WatchlistFetchCoordinator,
    WatchlistItemProcessor,
)

# Get db_content directory from environment variable with fallback
DB_CONTENT_DIR = os.environ.get("USER_DB_CONTENT", "/user/db_content")
PLEX_WATCHLIST_CACHE_FILE = os.path.join(DB_CONTENT_DIR, "plex_watchlist_cache.pkl")
OTHER_PLEX_WATCHLIST_CACHE_FILE = os.path.join(
    DB_CONTENT_DIR, "other_plex_watchlist_cache.pkl"
)
DETAIL_CACHE_FILE = os.path.join(DB_CONTENT_DIR, "plex_detail_cache.json")
CACHE_EXPIRY_DAYS = 7


def load_plex_cache(cache_file):
    try:
        if os.path.exists(cache_file):
            with open(cache_file, "rb") as f:
                return pickle.load(f)
    except (EOFError, pickle.UnpicklingError, FileNotFoundError) as e:
        logging.warning(
            f"Error loading Plex watchlist cache: {e}. Creating a new cache."
        )
    return {}


def save_plex_cache(cache, cache_file):
    try:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, "wb") as f:
            pickle.dump(cache, f)
    except Exception as e:
        logging.error(f"Error saving Plex watchlist cache: {e}")


def get_plex_client() -> tuple[Any, str] | tuple[None, None]:
    """
    Get Plex client connection and token.

    Returns:
        Tuple of (account, token_string) or (None, None) on failure
    """
    start_time = time.time()

    # Get tokens with explicit type casting
    plex_token = get_setting("Plex", "token")
    if not plex_token or not isinstance(plex_token, str):
        plex_token = get_setting("File Management", "plex_token_for_symlink")

    # Type guard - ensure we have a string token or None
    if not plex_token or not isinstance(plex_token, str):
        logging.error(
            "Plex token not configured. Please add Plex token in settings "
            "(Plex:token or File Management:plex_token_for_symlink)."
        )
        return None, None

    # At this point, type checker knows plex_token is str
    try:
        logging.info("Connecting to Plex.tv cloud service using token authentication")
        account = MyPlexAccount(token=plex_token)
        logging.info(f"Successfully connected to Plex.tv as user: {account.username}")
        logging.debug(
            f"Account details - Username: {account.username}, Email: {account.email}"
        )
        logging.debug(
            f"Connection details - Using Plex.tv API, endpoint: {account._server}"
        )
        logging.info(
            f"Plex client connection took {time.time() - start_time:.4f} seconds"
        )
        return account, plex_token
    except Exception as e:
        logging.error(f"Error connecting to Plex.tv cloud service: {e}")
        logging.error(
            f"Plex client connection attempt took {time.time() - start_time:.4f} "
            "seconds before failing"
        )
        return None, None


def get_show_status(imdb_id: str) -> str:
    """Get the status of a TV show from Trakt."""
    start_time = time.time()
    try:
        trakt = TraktMetadata()
        search_result = trakt._search_by_imdb(imdb_id)
        if search_result and search_result["type"] == "show":
            show = search_result["show"]
            slug = show["ids"]["slug"]

            # Get the full show data using the slug
            url = f"{trakt.base_url}/shows/{slug}?extended=full"
            response = trakt._make_request(url)
            if response and response.status_code == 200:
                show_data = response.json()
                status = show_data.get("status", "").lower()
                logging.debug(
                    f"Getting show status for {imdb_id} took {time.time() - start_time:.4f} seconds. Status: {status}"
                )
                if status == "canceled":
                    return "ended"
                return status
    except Exception as e:
        logging.error(f"Error getting show status for {imdb_id}: {str(e)}")
        logging.debug(
            f"Getting show status for {imdb_id} took {time.time() - start_time:.4f} seconds before error."
        )
    return ""


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
    overall_start_time = time.time()

    logging.info("Starting Plex.tv cloud watchlist retrieval")

    # Connect to Plex
    account, plex_token_str = get_plex_client()
    if not account or not plex_token_str:
        logging.error("Failed to get Plex client or token")
        return [([], versions)]

    logging.info(f"Using Plex account: {account.username}")

    try:
        # Fetch watchlist
        logging.info("Fetching initial watchlist from Plex.tv cloud service")
        fetch_start = time.time()
        initial_watchlist = account.watchlist()
        logging.info(
            f"Fetching watchlist took {time.time() - fetch_start:.4f} seconds. "
            f"Found {len(initial_watchlist)} items"
        )

        if not initial_watchlist:
            logging.info("Plex watchlist is empty")
            return [([], versions)]

        # Fetch item details with caching
        detail_cache = PlexDetailCache(DETAIL_CACHE_FILE, max_age_days=30)
        coordinator = WatchlistFetchCoordinator(detail_cache)
        all_fetched_items, fetch_stats = coordinator.fetch_all_item_details(
            initial_watchlist, plex_token_str
        )

        logging.info(
            f"Fetched details for {fetch_stats['total_items']} items "
            f"({fetch_stats['cache_hits']} from cache, "
            f"{fetch_stats['fetched_count']} fetched)"
        )

        # Process items into wanted list
        processor = WatchlistItemProcessor(account, account.username)
        wanted_items, process_stats = processor.process_items(all_fetched_items)

        # Log summary
        logging.info("Plex.tv cloud watchlist processing complete:")
        logging.info(f"  Total items in watchlist: {len(initial_watchlist)}")
        logging.info(f"  Items successfully processed: {process_stats['processed']}")
        logging.info(
            f"  Items skipped (no IMDB ID or error): {process_stats['skipped']}"
        )
        logging.info(f"  Items removed from watchlist: {process_stats['removed']}")
        logging.info(
            f"  Items kept (collected but ongoing): {process_stats['collected_kept']}"
        )

        elapsed = time.time() - overall_start_time
        logging.info(
            f"get_wanted_from_plex_watchlist completed in {elapsed:.4f} seconds"
        )

        return [(wanted_items, versions)]

    except Exception as e:
        elapsed = time.time() - overall_start_time
        logging.error(
            f"Error processing Plex watchlist after {elapsed:.4f} seconds: {e}",
            exc_info=True,
        )
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
    overall_start_time = time.time()

    logging.info(f"Starting watchlist retrieval for other Plex user: {username}")

    try:
        # Connect to Plex
        logging.info(f"Connecting to Plex.tv cloud service for user {username}")
        connect_start = time.time()
        account = MyPlexAccount(token=token)
        logging.info(f"Connection took {time.time() - connect_start:.4f} seconds")

        if not account:
            logging.error(
                f"Could not connect to Plex.tv cloud service for user {username}"
            )
            return [([], versions)]

        # Verify username matches
        if account.username != username:
            logging.error(
                f"Token for user {username} belongs to {account.username}. Aborting"
            )
            return [([], versions)]

        # Fetch watchlist
        logging.info(f"Fetching watchlist for user {username}")
        fetch_start = time.time()
        initial_watchlist = account.watchlist()
        logging.info(
            f"Fetching watchlist took {time.time() - fetch_start:.4f} seconds. "
            f"Found {len(initial_watchlist)} items"
        )

        if not initial_watchlist:
            logging.info(f"Plex watchlist for user {username} is empty")
            return [([], versions)]

        # Fetch item details with user-specific cache
        cache_file = os.path.join(DB_CONTENT_DIR, f"plex_detail_cache_{username}.json")
        detail_cache = PlexDetailCache(cache_file, max_age_days=30)
        coordinator = WatchlistFetchCoordinator(detail_cache)
        all_fetched_items, fetch_stats = coordinator.fetch_all_item_details(
            initial_watchlist, token
        )

        logging.info(
            f"User {username}: Fetched details for {fetch_stats['total_items']} items "
            f"({fetch_stats['cache_hits']} from cache, "
            f"{fetch_stats['fetched_count']} fetched)"
        )

        # Process items into wanted list
        processor = WatchlistItemProcessor(account, account.username)
        wanted_items, process_stats = processor.process_items(all_fetched_items)

        logging.info(
            f"User {username}: Retrieved {process_stats['processed']} wanted items. "
            f"Skipped {process_stats['skipped']} (no IMDB ID or error)"
        )

        elapsed = time.time() - overall_start_time
        logging.info(
            f"get_wanted_from_other_plex_watchlist for user {username} "
            f"completed in {elapsed:.4f} seconds"
        )

        return [(wanted_items, versions)]

    except Exception as e:
        elapsed = time.time() - overall_start_time
        logging.error(
            f"Error fetching {username}'s watchlist after {elapsed:.4f} seconds: {e}",
            exc_info=True,
        )
        return [([], versions)]


def validate_plex_tokens():
    """Validate all Plex tokens and return their status."""
    overall_start_time = time.time()
    token_status = {}

    # Validate main user's token
    try:
        plex_token_validation_start_time = time.time()
        plex_token = get_setting("Plex", "token")
        if plex_token:
            account = MyPlexAccount(token=plex_token)
            # Ping to refresh the auth token
            ping_start_time = time.time()
            account.ping()
            logging.debug(
                f"Main token ping took {time.time() - ping_start_time:.4f} seconds."
            )
            # The expiration is stored in the account object directly
            token_status["main"] = {
                "valid": True,
                "expires_at": (
                    account.rememberExpiresAt
                    if hasattr(account, "rememberExpiresAt")
                    else None
                ),
                "username": account.username,
            }
            update_token_status(
                "main",
                True,
                expires_at=(
                    account.rememberExpiresAt
                    if hasattr(account, "rememberExpiresAt")
                    else None
                ),
                plex_username=account.username,
            )
            logging.info(
                f"Main Plex token validation took {time.time() - plex_token_validation_start_time:.4f} seconds. Valid: True, User: {account.username}"
            )
    except Exception as e:
        logging.error(f"Error validating main Plex token: {e}")
        token_status["main"] = {"valid": False, "expires_at": None, "username": None}
        update_token_status("main", False)
        logging.info(
            f"Main Plex token validation took {time.time() - plex_token_validation_start_time:.4f} seconds. Valid: False"
        )

    # Validate other users' tokens
    config = load_config()
    content_sources = config.get("Content Sources", {})

    for source_id, source in content_sources.items():
        if source.get("type") == "Other Plex Watchlist":
            username = source.get("username")
            token = source.get("token")

            if username and token:
                other_token_validation_start_time = time.time()
                try:
                    account = MyPlexAccount(token=token)
                    # Ping to refresh the auth token
                    ping_start_time = time.time()
                    account.ping()
                    logging.debug(
                        f"Other token ping for user {username} took {time.time() - ping_start_time:.4f} seconds."
                    )
                    token_status[username] = {
                        "valid": True,
                        "expires_at": (
                            account.rememberExpiresAt
                            if hasattr(account, "rememberExpiresAt")
                            else None
                        ),
                        "username": account.username,
                    }
                    update_token_status(
                        username,
                        True,
                        expires_at=(
                            account.rememberExpiresAt
                            if hasattr(account, "rememberExpiresAt")
                            else None
                        ),
                        plex_username=account.username,
                    )
                    logging.info(
                        f"Plex token validation for user {username} took {time.time() - other_token_validation_start_time:.4f} seconds. Valid: True, User: {account.username}"
                    )
                except Exception as e:
                    logging.error(
                        f"Error validating Plex token for user {username}: {e}"
                    )
                    token_status[username] = {
                        "valid": False,
                        "expires_at": None,
                        "username": None,
                    }
                    update_token_status(username, False)
                    logging.info(
                        f"Plex token validation for user {username} took {time.time() - other_token_validation_start_time:.4f} seconds. Valid: False"
                    )

    logging.info(
        f"validate_plex_tokens completed in {time.time() - overall_start_time:.4f} seconds."
    )
    return token_status
