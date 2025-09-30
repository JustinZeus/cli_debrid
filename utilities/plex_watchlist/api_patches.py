import logging
import os

# Store originals for potential restoration
_original_plexserver_query = None
_original_myplexaccount_query = None
_patches_applied = False


def apply_patches():
    """Apply monkey patches to fix Plex API endpoint issues."""
    global _patches_applied, _original_plexserver_query, _original_myplexaccount_query

    if _patches_applied:
        return True

    if os.environ.get("DISABLE_PLEX_PATCHES") == "true":
        logging.info("Plex API patches disabled by environment variable")
        return False

    try:
        from plexapi.myplex import MyPlexAccount
        from plexapi.server import PlexServer

        # Store originals
        _original_myplexaccount_query = MyPlexAccount.query
        _original_plexserver_query = PlexServer.query

        # Create patched versions
        def patched_plexserver_query(
            self, key, method=None, headers=None, params=None, timeout=None, **kwargs
        ):
            if "metadata.provider.plex.tv" in str(key):
                fixed_key = str(key).replace(
                    "metadata.provider.plex.tv", "discover.provider.plex.tv"
                )
                logging.debug(f"Fixed PlexServer endpoint: {key} -> {fixed_key}")
                return _original_plexserver_query(
                    self, fixed_key, method, headers, params, timeout, **kwargs
                )
            return _original_plexserver_query(
                self, key, method, headers, params, timeout, **kwargs
            )

        def patched_myplexaccount_query(
            self, url, method=None, headers=None, timeout=None, **kwargs
        ):
            if "metadata.provider.plex.tv" in url:
                fixed_url = url.replace(
                    "metadata.provider.plex.tv", "discover.provider.plex.tv"
                )
                logging.debug(f"Fixed MyPlexAccount endpoint: {url} -> {fixed_url}")
                return _original_myplexaccount_query(
                    self, fixed_url, method, headers, timeout, **kwargs
                )
            return _original_myplexaccount_query(
                self, url, method, headers, timeout, **kwargs
            )

        # Apply patches
        PlexServer.query = patched_plexserver_query
        MyPlexAccount.query = patched_myplexaccount_query

        _patches_applied = True
        logging.info("Successfully applied Plex API endpoint patches")
        return True

    except ImportError as e:
        logging.error(f"Failed to apply Plex patches - plexapi not available: {e}")
        return False
    except Exception as e:
        logging.error(f"Failed to apply Plex patches: {e}")
        return False
