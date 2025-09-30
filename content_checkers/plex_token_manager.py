import os
import json
import logging
from datetime import datetime
from queues.config_manager import CONFIG_DIR

TOKEN_STATUS_FILE = os.path.join(CONFIG_DIR, "plex_token_status.json")


def load_token_status():
    """Load the token status from the JSON file."""
    try:
        if os.path.exists(TOKEN_STATUS_FILE):
            with open(TOKEN_STATUS_FILE, "r") as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error loading token status: {e}")
    return {}


def save_token_status(status):
    """Save the token status to the JSON file."""
    try:
        with open(TOKEN_STATUS_FILE, "w") as f:
            json.dump(status, f, indent=4, default=str)
    except Exception as e:
        logging.error(f"Error saving token status: {e}")


def update_token_status(username, valid, expires_at=None, plex_username=None):
    """Update the status for a specific token."""
    status = load_token_status()
    status[username] = {
        "valid": valid,
        "last_checked": datetime.now().isoformat(),
        "expires_at": expires_at.isoformat() if expires_at else None,
        "username": plex_username,
    }
    save_token_status(status)


def get_token_status():
    """Get the current status of all tokens."""
    return load_token_status()


def validate_plex_tokens():
    """Validate all Plex tokens and return their status."""
    from queues.config_manager import (
        load_config,
    )  # Local import to avoid circular dependency

    print("test validate")
    from plexapi.myplex import MyPlexAccount
    import logging
    import time
    from utilities.settings import get_setting

    overall_start_time = time.time()
    token_status = {}

    # Validate main token
    try:
        plex_token_validation_start_time = time.time()
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
