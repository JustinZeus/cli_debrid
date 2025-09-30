"""
Async fetching utilities for Plex watchlist items.

This module handles concurrent fetching of detailed metadata from Plex API endpoints,
extracting IMDB/TMDB identifiers and media types from XML responses.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional
import xml.etree.ElementTree as ET

import aiohttp


logger = logging.getLogger(__name__)


class PlexItemDetails:
    """Container for Plex item metadata extraction results."""

    def __init__(
        self,
        imdb_id: Optional[str] = None,
        tmdb_id: Optional[str] = None,
        media_type: Optional[str] = None,
        original_plex_item: Any = None,
        error: Optional[str] = None,
    ):
        self.imdb_id = imdb_id
        self.tmdb_id = tmdb_id
        self.media_type = media_type
        self.original_plex_item = original_plex_item
        self.error = error

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "imdb_id": self.imdb_id,
            "tmdb_id": self.tmdb_id,
            "media_type": self.media_type,
            "original_plex_item": self.original_plex_item,
            "error": self.error,
        }


class PlexMetadataExtractor:
    """Extracts IMDB/TMDB identifiers from Plex XML metadata."""

    IMDB_PREFIX = "imdb://"
    TMDB_PREFIX = "tmdb://"

    @staticmethod
    def extract_ids_from_xml(
        xml_text: str,
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Parse Plex XML response and extract identifiers.

        Args:
            xml_text: XML response from Plex API

        Returns:
            Tuple of (imdb_id, tmdb_id, media_type)
        """
        try:
            root = ET.fromstring(xml_text)
            media_element = PlexMetadataExtractor._find_media_element(root)

            if not media_element:
                return None, None, None

            imdb_id, tmdb_id = PlexMetadataExtractor._extract_guid_ids(media_element)
            media_type = media_element.get("type")

            return imdb_id, tmdb_id, media_type

        except ET.ParseError as e:
            logger.error(f"XML parsing failed: {e}")
            return None, None, None

    @staticmethod
    def _find_media_element(root: ET.Element) -> Optional[ET.Element]:
        """Find the media element (Video or Directory) in the XML tree."""
        if root.tag != "MediaContainer":
            return None

        media_element = root.find("./Video")
        if media_element is None:
            media_element = root.find("./Directory")

        return media_element

    @staticmethod
    def _extract_guid_ids(
        media_element: ET.Element,
    ) -> tuple[Optional[str], Optional[str]]:
        """Extract IMDB and TMDB IDs from Guid elements."""
        imdb_id = None
        tmdb_id = None

        for guid_tag in media_element.findall("./Guid"):
            guid_str = guid_tag.get("id")
            if not guid_str:
                continue

            if guid_str.startswith(PlexMetadataExtractor.IMDB_PREFIX):
                imdb_id = guid_str.split("//", 1)[1]
            elif guid_str.startswith(PlexMetadataExtractor.TMDB_PREFIX):
                tmdb_id = guid_str.split("//", 1)[1]

        return imdb_id, tmdb_id


async def fetch_item_details_and_extract_ids(
    session: aiohttp.ClientSession, item_data: Dict[str, Any], plex_token_str: str
) -> Dict[str, Any]:
    """
    Fetch full metadata for a single Plex item and extract identifiers.

    Args:
        session: Active aiohttp client session
        item_data: Dict containing 'title', 'url', and 'original_plex_item'
        plex_token_str: Plex authentication token

    Returns:
        Dictionary with extracted metadata and original item reference
    """
    url = item_data["url"]
    title = item_data["title"]
    original_plex_item = item_data["original_plex_item"]

    headers = {"X-Plex-Token": plex_token_str, "Accept": "application/xml"}

    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            if response.status != 200:
                error_msg = f"HTTP{response.status}"
                logger.error(
                    f"Failed to fetch details for '{title}' from {url}: {error_msg}"
                )
                return PlexItemDetails(
                    original_plex_item=original_plex_item, error=error_msg
                ).to_dict()

            xml_text = await response.text()
            imdb_id, tmdb_id, media_type = PlexMetadataExtractor.extract_ids_from_xml(
                xml_text
            )

            if imdb_id is None and tmdb_id is None:
                logger.warning(f"No identifiers found in XML for '{title}' from {url}")
                return PlexItemDetails(
                    original_plex_item=original_plex_item, error="XMLParseError"
                ).to_dict()

            logger.debug(
                f"Extracted metadata for '{title}': "
                f"IMDB={imdb_id}, TMDB={tmdb_id}, Type={media_type}"
            )

            return PlexItemDetails(
                imdb_id=imdb_id,
                tmdb_id=tmdb_id,
                media_type=media_type,
                original_plex_item=original_plex_item,
            ).to_dict()

    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching details for '{title}' from {url}")
        return PlexItemDetails(
            original_plex_item=original_plex_item, error="Timeout"
        ).to_dict()

    except Exception as e:
        logger.error(f"Unexpected error fetching details for '{title}' from {url}: {e}")
        return PlexItemDetails(
            original_plex_item=original_plex_item, error=str(e)
        ).to_dict()


async def run_async_fetches(
    watchlist_items_with_urls: List[Dict[str, Any]],
    plex_token_str: str,
    max_concurrent_connections: int = 10,
) -> List[Dict[str, Any]]:
    """
    Fetch metadata for multiple Plex items concurrently.

    Args:
        watchlist_items_with_urls: List of dicts with 'title', 'url', and 'original_plex_item'
        plex_token_str: Plex authentication token
        max_concurrent_connections: Maximum concurrent connections to Plex API

    Returns:
        List of dictionaries containing extracted metadata for each item
    """
    if not watchlist_items_with_urls:
        return []

    connector = aiohttp.TCPConnector(limit_per_host=max_concurrent_connections)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_item_details_and_extract_ids(session, item_data, plex_token_str)
            for item_data in watchlist_items_with_urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        return results
