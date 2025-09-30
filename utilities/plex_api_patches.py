"""
Monkey patches for the plexapi library.

Fixes endpoint URLs and adds request logging for debugging purposes.
This module should be imported early in the application lifecycle.
"""

import logging
import os


def setup_plex_api_patches():
    """
    Apply patches to plexapi library methods.

    This function:
    1. Fixes deprecated metadata.provider.plex.tv endpoints to use discover.provider.plex.tv
    2. Adds debug logging for all Plex API requests
    """
    # Set up dedicated logger for patches
    patch_logger = logging.getLogger("plex_api_patches")
    patch_logger.setLevel(logging.INFO)

    log_dir = os.environ.get("USER_LOGS", "/user/logs")
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, "plex_api_patches.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    patch_logger.addHandler(file_handler)
    patch_logger.propagate = False

    try:
        import plexapi.myplex

        # Patch PlexServer.query
        original_plexserver_query = plexapi.myplex.PlexServer.query

        def patched_plexserver_query(
            self, key, method=None, headers=None, params=None, timeout=None, **kwargs
        ):
            # Fix deprecated endpoint URL
            if "metadata.provider.plex.tv" in str(key):
                fixed_key = str(key).replace(
                    "metadata.provider.plex.tv", "discover.provider.plex.tv"
                )
                patch_logger.info(f"Fixed endpoint URL: {key} -> {fixed_key}")
                key = fixed_key

            patch_logger.debug(
                f"PlexServer.query: key={key}, method={method}, params={params}"
            )

            return original_plexserver_query(
                self, key, method, headers, params, timeout, **kwargs
            )

        plexapi.myplex.PlexServer.query = patched_plexserver_query
        patch_logger.info("Successfully patched plexapi.myplex.PlexServer.query")

        # Patch MyPlexAccount.query
        original_myplex_query = plexapi.myplex.MyPlexAccount.query

        def patched_myplex_query(
            self, url, method=None, headers=None, timeout=None, **kwargs
        ):
            # Fix deprecated endpoint URL
            if "metadata.provider.plex.tv" in url:
                fixed_url = url.replace(
                    "metadata.provider.plex.tv", "discover.provider.plex.tv"
                )
                patch_logger.info(f"Fixed endpoint URL: {url} -> {fixed_url}")
                url = fixed_url

            patch_logger.debug(f"MyPlexAccount.query: url={url}, method={method}")

            return original_myplex_query(self, url, method, headers, timeout, **kwargs)

        plexapi.myplex.MyPlexAccount.query = patched_myplex_query
        patch_logger.info("Successfully patched plexapi.myplex.MyPlexAccount.query")

    except (ImportError, AttributeError) as e:
        patch_logger.error(f"Failed to patch plexapi query methods: {e}", exc_info=True)
        raise


# Apply patches when module is imported
setup_plex_api_patches()
