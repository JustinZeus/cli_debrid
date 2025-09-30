import logging
import time
import aiohttp
import asyncio
import xml.etree.ElementTree as ET
from typing import List, Dict, Any, Optional


async def fetch_item_details_and_extract_ids(
    session: aiohttp.ClientSession, item_data: Dict[str, Any], plex_token_str: str
) -> Dict[str, Any]:
    """Fetch full metadata for a single Plex item and extract IDs."""
    headers = {"X-Plex-Token": plex_token_str, "Accept": "application/xml"}
    url = item_data["url"]
    title = item_data["title"]
    original_plex_item = item_data["original_plex_item"]

    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            if response.status == 200:
                xml_text = await response.text()
                root = ET.fromstring(xml_text)

                media_element = None
                if root.tag == "MediaContainer":
                    media_element = root.find("./Video")  # Movies
                    if not media_element:
                        media_element = root.find("./Directory")  # Shows

                if not media_element:
                    logging.warning(f"Could not parse XML for {title}")
                    return {
                        "imdb_id": None,
                        "tmdb_id": None,
                        "media_type": None,
                        "original_plex_item": original_plex_item,
                        "error": "XMLParseError",
                    }

                imdb_id = None
                tmdb_id = None
                media_type = media_element.get("type")

                for guid_tag in media_element.findall("./Guid"):
                    guid_str = guid_tag.get("id")
                    if guid_str:
                        if guid_str.startswith("imdb://"):
                            imdb_id = guid_str.split("//")[1]
                        elif guid_str.startswith("tmdb://"):
                            tmdb_id = guid_str.split("//")[1]

                logging.debug(
                    f"Found for '{title}': IMDB={imdb_id}, TMDB={tmdb_id}, Type={media_type}"
                )

                return {
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                    "media_type": media_type,
                    "original_plex_item": original_plex_item,
                }
            else:
                logging.error(f"HTTP {response.status} for {title}")
                return {
                    "imdb_id": None,
                    "tmdb_id": None,
                    "media_type": None,
                    "original_plex_item": original_plex_item,
                    "error": f"HTTP{response.status}",
                }

    except asyncio.TimeoutError:
        logging.error(f"Timeout fetching {title}")
        return {
            "imdb_id": None,
            "tmdb_id": None,
            "media_type": None,
            "original_plex_item": original_plex_item,
            "error": "Timeout",
        }
    except Exception as e:
        logging.error(f"Error fetching {title}: {e}")
        return {
            "imdb_id": None,
            "tmdb_id": None,
            "media_type": None,
            "original_plex_item": original_plex_item,
            "error": str(e),
        }


class WatchlistFetcher:
    """Handles fetching Plex watchlist items with batching support."""

    def __init__(self, batch_size: int = 100, max_concurrent: int = 10):
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent

    async def fetch_all(
        self, items_to_fetch: List[Dict[str, Any]], plex_token: str
    ) -> List[Dict[str, Any]]:
        """Fetch all items, potentially in batches."""
        if not items_to_fetch:
            return []

        # Determine if we need batching
        if len(items_to_fetch) > 500:
            return await self._fetch_in_batches(items_to_fetch, plex_token)
        else:
            return await self._fetch_directly(items_to_fetch, plex_token)

    async def _fetch_directly(
        self, items: List[Dict[str, Any]], token: str
    ) -> List[Dict[str, Any]]:
        """Fetch all items in one go."""
        conn = aiohttp.TCPConnector(limit_per_host=self.max_concurrent)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [
                fetch_item_details_and_extract_ids(session, item, token)
                for item in items
            ]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            return results

    async def _fetch_in_batches(
        self, items: List[Dict[str, Any]], token: str
    ) -> List[Dict[str, Any]]:
        """Fetch items in batches to avoid overwhelming the server."""
        all_results = []
        total_batches = (len(items) + self.batch_size - 1) // self.batch_size

        conn = aiohttp.TCPConnector(limit_per_host=self.max_concurrent)
        async with aiohttp.ClientSession(connector=conn) as session:
            for i in range(0, len(items), self.batch_size):
                batch = items[i : i + self.batch_size]
                batch_num = (i // self.batch_size) + 1

                logging.info(
                    f"Fetching batch {batch_num}/{total_batches} "
                    f"({len(batch)} items)"
                )

                tasks = [
                    fetch_item_details_and_extract_ids(session, item, token)
                    for item in batch
                ]

                batch_results = await asyncio.gather(*tasks, return_exceptions=False)
                all_results.extend(batch_results)

                # Small delay between batches to be nice to the server
                if batch_num < total_batches:
                    await asyncio.sleep(1)

        return all_results


def run_async_fetches(
    watchlist_items: List[Dict[str, Any]], plex_token: str
) -> List[Dict[str, Any]]:
    """Synchronous wrapper for async fetching."""
    fetcher = WatchlistFetcher()
    return asyncio.run(fetcher.fetch_all(watchlist_items, plex_token))
