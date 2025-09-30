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


def get_plex_client():
    start_time = time.time()
    # Prefer main Plex token, fallback to symlink token if primary not set
    plex_token = get_setting("Plex", "token")
    if not plex_token:
        plex_token = get_setting("File Management", "plex_token_for_symlink")

    if not plex_token:
        logging.error(
            "Plex token not configured. Please add Plex token in settings (Plex:token or File Management:plex_token_for_symlink)."
        )
        return None, None  # Return None for account and token string

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
        return account, plex_token  # Return account object and token string
    except Exception as e:
        logging.error(f"Error connecting to Plex.tv cloud service: {e}")
        logging.error(
            f"Plex client connection attempt took {time.time() - start_time:.4f} seconds before failing"
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
    overall_start_time = time.time()
    all_wanted_items = []
    processed_items_for_current_run = []

    logging.info("Starting Plex.tv cloud watchlist retrieval")

    client_start_time = time.time()
    account, plex_token_str = get_plex_client()
    client_end_time = time.time()
    logging.info(
        f"get_plex_client() call took {client_end_time - client_start_time:.4f} seconds"
    )

    if not account or not plex_token_str:
        logging.error(
            "Failed to get Plex client or token - no account available or token missing"
        )
        return [([], versions)]

    logging.info(f"Using Plex account: {account.username}")

    try:
        should_remove = get_setting("Debug", "plex_watchlist_removal", False)
        keep_series = get_setting("Debug", "plex_watchlist_keep_series", False)

        logging.info("Fetching initial watchlist from Plex.tv cloud service")
        fetch_watchlist_start_time = time.time()
        initial_watchlist = account.watchlist()
        fetch_watchlist_end_time = time.time()
        logging.info(
            f"Fetching initial watchlist from Plex API took {fetch_watchlist_end_time - fetch_watchlist_start_time:.4f} seconds. Found {len(initial_watchlist)} items."
        )

        if not initial_watchlist:
            logging.info("Plex watchlist is empty.")
            return [([], versions)]

        # Initialize the detail cache
        detail_cache = PlexDetailCache(DETAIL_CACHE_FILE, max_age_days=30)
        coordinator = WatchlistFetchCoordinator(detail_cache)

        # Fetch all item details using cache when possible
        all_fetched_items, fetch_stats = coordinator.fetch_all_item_details(
            initial_watchlist, plex_token_str  # or just 'token' in the other function
        )

        logging.info(
            f"Fetched details for {fetch_stats['total_items']} items "
            f"({fetch_stats['cache_hits']} from cache, {fetch_stats['fetched_count']} fetched)"
        )

        total_items_from_async = len(all_fetched_items)
        skipped_count = 0
        removed_count = 0
        collected_skipped = 0

        processing_loop_start_time = time.time()
        for item_details in all_fetched_items:
            original_plex_item = item_details["original_plex_item"]
            title = original_plex_item.title

            if item_details.get("error"):
                logging.warning(
                    f"Skipping item '{title}' due to error during async fetch: {item_details['error']}"
                )
                skipped_count += 1
                continue

            imdb_id = item_details["imdb_id"]
            tmdb_id = item_details["tmdb_id"]
            media_type = (
                item_details["media_type"]
                if item_details["media_type"]
                else original_plex_item.type
            )

            if not imdb_id and tmdb_id and media_type:
                converted_imdb_id, _ = tmdb_to_imdb(tmdb_id, media_type, title)
                if converted_imdb_id:
                    imdb_id = converted_imdb_id

            if not imdb_id:
                skipped_count += 1
                logging.debug(
                    f"Skipping item '{title}' - no IMDB ID found after async fetch and potential conversion."
                )
                continue

            if media_type == "show":
                media_type = "tv"

            item_state = get_media_item_presence(imdb_id=imdb_id)
            logging.debug(f"Item '{title}' (IMDB: {imdb_id}) - Presence: {item_state}")

            if item_state == "Collected" and should_remove:
                if media_type == "tv":
                    if keep_series:
                        logging.debug(
                            f"Keeping collected TV series: '{title}' (IMDB: {imdb_id}) - keep_series is enabled."
                        )
                        collected_skipped += 1
                        continue
                    else:
                        show_status = get_show_status(imdb_id)
                        if show_status != "ended":
                            logging.debug(
                                f"Keeping collected ongoing TV series: '{title}' (IMDB: {imdb_id}) - status: {show_status}."
                            )
                            collected_skipped += 1
                            continue
                        logging.debug(
                            f"Identified collected and ended TV series for removal: '{title}' (IMDB: {imdb_id}) - status: {show_status}."
                        )
                else:
                    logging.debug(
                        f"Identified collected movie for removal: '{title}' (IMDB: {imdb_id})."
                    )

                try:
                    remove_item_start_time = time.time()
                    account.removeFromWatchlist([original_plex_item])
                    removed_count += 1
                    logging.info(
                        f"Successfully removed '{title}' (IMDB: {imdb_id}) from watchlist. Took {time.time() - remove_item_start_time:.4f}s."
                    )
                    continue
                except Exception as e_remove:
                    logging.error(
                        f"Failed to remove '{title}' (IMDB: {imdb_id}) from watchlist: {e_remove}"
                    )

            processed_items_for_current_run.append(
                {
                    "imdb_id": imdb_id,
                    "media_type": media_type,
                    "content_source_detail": account.username,
                }
            )
            logging.debug(
                f"Added '{title}' (IMDB: {imdb_id}, Type: {media_type}) to processed items from source: {account.username}"
            )

        processing_loop_end_time = time.time()
        logging.info(
            f"Main processing loop for {total_items_from_async} fetched items took {processing_loop_end_time - processing_loop_start_time:.4f} seconds."
        )

        logging.info(f"Plex.tv cloud watchlist processing complete:")
        logging.info(f"Total items in initial watchlist: {len(initial_watchlist)}")
        logging.info(f"Items prepared for async fetch: {len(items_to_process_async)}")
        logging.info(
            f"Items successfully processed from async results: {len(all_fetched_items) - skipped_count - collected_skipped - removed_count}"
        )
        logging.info(f"Items skipped (no IMDB ID or fetch error): {skipped_count}")
        logging.info(f"Items removed from watchlist: {removed_count}")
        logging.info(f"Items skipped (already collected and kept): {collected_skipped}")
        logging.info(
            f"New items added to wanted list: {len(processed_items_for_current_run)}"
        )

        all_wanted_items.append((processed_items_for_current_run, versions))

        overall_end_time = time.time()
        logging.info(
            f"get_wanted_from_plex_watchlist completed in {overall_end_time - overall_start_time:.4f} seconds."
        )
        return all_wanted_items

    except Exception as e:
        logging.error(f"Error processing Plex watchlist: {e}", exc_info=True)
        overall_end_time = time.time()
        logging.error(
            f"get_wanted_from_plex_watchlist failed after {overall_end_time - overall_start_time:.4f} seconds due to: {e}"
        )
        return [([], versions)]


def get_wanted_from_other_plex_watchlist(
    username: str, token: str, versions: Dict[str, bool]
) -> List[Tuple[List[Dict[str, Any]], Dict[str, bool]]]:
    overall_start_time = time.time()
    all_wanted_items = []
    processed_items_for_current_run = []

    logging.info(f"Starting watchlist retrieval for other Plex user: {username}")

    try:
        logging.info(f"Connecting to Plex.tv cloud service for user {username}")
        client_start_time = time.time()
        account = MyPlexAccount(token=token)
        client_end_time = time.time()
        logging.info(
            f"Plex client connection for user {username} took {client_end_time - client_start_time:.4f} seconds."
        )

        if not account:
            logging.error(
                f"Could not connect to Plex.tv cloud service with provided token for user {username}"
            )
            return [([], versions)]

        if account.username != username:
            logging.error(
                f"Plex.tv cloud token for user {username} seems to belong to {account.username} (expected {username}). Aborting."
            )
            return [([], versions)]

        logging.info(
            f"Fetching initial watchlist for user {username} from Plex.tv cloud service"
        )
        fetch_watchlist_start_time = time.time()
        initial_watchlist = account.watchlist()
        fetch_watchlist_end_time = time.time()
        logging.info(
            f"Fetching initial watchlist for {username} from Plex API took {fetch_watchlist_end_time - fetch_watchlist_start_time:.4f} seconds. Found {len(initial_watchlist)} items."
        )

        if not initial_watchlist:
            logging.info(f"Plex watchlist for user {username} is empty.")
            return [([], versions)]

        # Initialize the detail cache for this specific user
        other_detail_cache_file = os.path.join(
            DB_CONTENT_DIR, f"plex_detail_cache_{username}.json"
        )
        # Initialize the detail cache
        detail_cache = PlexDetailCache(DETAIL_CACHE_FILE, max_age_days=30)
        coordinator = WatchlistFetchCoordinator(detail_cache)

        # Fetch all item details using cache when possible
        all_fetched_items, fetch_stats = coordinator.fetch_all_item_details(
            initial_watchlist, token
        )

        logging.info(
            f"Fetched details for {fetch_stats['total_items']} items "
            f"({fetch_stats['cache_hits']} from cache, {fetch_stats['fetched_count']} fetched)"
        )

        items_processed_count = 0
        items_skipped_no_imdb = 0

        for item_details in all_fetched_items:
            original_plex_item = item_details["original_plex_item"]
            title = original_plex_item.title

            if item_details.get("error"):
                logging.warning(
                    f"User {username}: Skipping item '{title}' due to error during async fetch: {item_details['error']}"
                )
                items_skipped_no_imdb += 1
                continue

            imdb_id = item_details["imdb_id"]

            if not imdb_id:
                items_skipped_no_imdb += 1
                logging.debug(
                    f"User {username}: Skipping item '{title}' - no IMDB ID found after async fetch."
                )
                continue

            media_type = (
                item_details["media_type"]
                if item_details["media_type"]
                else original_plex_item.type
            )
            if media_type == "show":
                media_type = "tv"

            wanted_item = {
                "imdb_id": imdb_id,
                "media_type": media_type,
                "content_source_detail": account.username,
            }
            processed_items_for_current_run.append(wanted_item)
            items_processed_count += 1
            logging.debug(
                f"User {username}: Added '{title}' (IMDB: {imdb_id}, Type: {media_type}) to processed items."
            )

        logging.info(
            f"User {username}: Retrieved {items_processed_count} wanted items from watchlist. Skipped {items_skipped_no_imdb} (no IMDB or fetch error)."
        )

    except Exception as e:
        logging.error(
            f"Error fetching {username}'s Plex watchlist: {str(e)}", exc_info=True
        )
        return [([], versions)]

    all_wanted_items.append((processed_items_for_current_run, versions))
    logging.info(
        f"get_wanted_from_other_plex_watchlist for user {username} completed in {time.time() - overall_start_time:.4f} seconds."
    )
    return all_wanted_items


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
