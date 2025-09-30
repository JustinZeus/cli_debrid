"""
Identifier conversion utilities for media metadata.

Handles conversion between different metadata provider IDs (TMDB, IMDB, etc.)
"""

import logging
from typing import Optional, Tuple

from cli_battery.app.direct_api import DirectAPI


logger = logging.getLogger(__name__)


def tmdb_to_imdb(
    tmdb_id: str, media_type: str, title: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Convert TMDB ID to IMDB ID using the DirectAPI.

    Args:
        tmdb_id: TMDB identifier
        media_type: Type of media ('movie', 'tv', 'show')
        title: Optional title for logging purposes

    Returns:
        Tuple of (imdb_id, source_name) where source_name indicates which API was used
    """
    if not tmdb_id or not media_type:
        logger.warning(
            f"Missing required parameters for TMDB conversion: "
            f"tmdb_id={tmdb_id}, media_type={media_type}"
        )
        return None, None

    try:
        api = DirectAPI()
        imdb_id, source = api.tmdb_to_imdb(tmdb_id, media_type=media_type)

        if imdb_id:
            title_info = f" for '{title}'" if title else ""
            logger.info(
                f"Converted TMDB ID {tmdb_id} to IMDB ID {imdb_id}{title_info} "
                f"via {source}"
            )
            return imdb_id, source
        else:
            title_info = f" for '{title}'" if title else ""
            logger.warning(
                f"TMDB to IMDB conversion failed{title_info} (TMDB: {tmdb_id})"
            )
            return None, None

    except Exception as e:
        title_info = f" for '{title}'" if title else ""
        logger.error(
            f"Error during TMDB to IMDB conversion{title_info}: {e}", exc_info=True
        )
        return None, None
