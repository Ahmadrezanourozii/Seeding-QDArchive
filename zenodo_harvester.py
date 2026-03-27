#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Implementation of the Zenodo repository harvester; features strict date filtering, rate limiting, and QDA relevance scoring.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from config import Settings, get_settings
from constants import STRICT_TERMS, file_ext
from harvester_base import AbstractBaseHarvester

logger = logging.getLogger(__name__)

ZENODO_API_BASE = "https://zenodo.org/api"
DEFAULT_PAGE_SIZE = 100
DEFAULT_THROTTLE_SECONDS = 2.0


def _slugify(text: str, max_length: int = 80) -> str:
    """Generates a URL-safe slug from the project title."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled_project"


def _normalize_metadata(record: dict[str, Any]) -> dict[str, Any]:
    """Extracts and flattens core metadata from the raw Zenodo API response."""
    metadata = record.get("metadata", {})
    rec_id = str(record.get("id") or record.get("recid") or "")

    creators = []
    for creator in metadata.get("creators", []):
        name = creator.get("name") or creator.get("person_or_org", {}).get("name", "")
        if name:
            creators.append({"name": name, "role": "AUTHOR"})

    license_info = metadata.get("license")
    license_id = license_info.get("id") if isinstance(license_info, dict) else license_info

    return {
        "recid": rec_id,
        "title": metadata.get("title", "Untitled"),
        "description": metadata.get("description", ""),
        "doi": metadata.get("doi", ""),
        "language": metadata.get("language", ""),
        "upload_date": metadata.get("publication_date", ""),
        "version": metadata.get("version", "v1"),
        "license": license_id,
        "keywords": metadata.get("keywords", []),
        "creators": creators,
        "project_url": record.get("links", {}).get("html", f"https://zenodo.org/records/{rec_id}"),
        "repository_url": "https://zenodo.org",
    }


class ZenodoHarvester(AbstractBaseHarvester):
    """Harvester for the Zenodo repository focusing on open-access qualitative data."""

    def __init__(self, settings: Settings | None = None, token: str | None = None):
        super().__init__(request_interval=DEFAULT_THROTTLE_SECONDS)
        self._settings = settings or get_settings()
        self._token = token or self._settings.zenodo_access_token
        self._base_url = ZENODO_API_BASE

    @property
    def name(self) -> str:
        return "zenodo"

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Retrieves and filters Zenodo records based on QDA relevance and temporal windows."""
        if not self._token:
            logger.error("[%s] Authentication token missing.", self.name)
            return 0

        # Define the temporal window (last 30 days) for harvesting.
        today = datetime.now(timezone.utc).date()
        date_from = (today - timedelta(days=30)).isoformat()
        date_filter = f"created:[{date_from} TO {today.isoformat()}]"

        search_queries = [s.strip() for s in query.split(" OR ") if s.strip()] if query else STRICT_TERMS

        total_enqueued = 0
        seen_recids: set[str] = set()
        headers = {"Authorization": f"Bearer {self._token}"}
        strict_mode = kwargs.get("strict", False)

        async with self.create_client_session(headers=headers) as session:
            for term in search_queries:
                page = 1
                query_success_count = 0
                logger.info("[%s] Querying: '%s'", self.name, term)

                while True:
                    clean_term = term.strip('"')
                    params = {
                        "q": f'"{clean_term}" AND {date_filter}',
                        "page": page,
                        "size": DEFAULT_PAGE_SIZE,
                        "access_right": "open"
                    }

                    try:
                        async with session.get(f"{self._base_url}/records", params=params) as response:
                            await asyncio.sleep(self.request_interval)
                            
                            if response.status == 429:
                                logger.warning("[%s] Rate limit encountered. Throttling for 60s.", self.name)
                                await asyncio.sleep(60.0)
                                continue

                            if response.status != 200:
                                logger.error("[%s] API Error %d for term '%s'", self.name, response.status, term)
                                break
                            
                            data = await response.json()
                            hits = data.get("hits", {}).get("hits", [])
                            if not hits:
                                break

                            for hit in hits:
                                recid = str(hit.get("id", ""))
                                if recid in seen_recids:
                                    continue

                                # Fetch detailed record to expose all file metadata
                                full_record = await self._fetch_full_record(session, recid)
                                if not full_record:
                                    continue
                                
                                metadata = full_record.get("metadata", {})
                                combined_text = f"{metadata.get('title', '')} {metadata.get('description', '')}"
                                
                                # Validate relevance via the weighted scoring system
                                if not self.is_qda_match(combined_text):
                                    continue
                                
                                files_raw = full_record.get("files", [])
                                if isinstance(files_raw, dict):
                                    files_raw = files_raw.get("entries", []) or list(files_raw.values())
                                
                                # Extract valid data files, ignoring common administrative/web metadata
                                files = []
                                for f in files_raw:
                                    fname = (f.get("key") or f.get("filename") or "")
                                    if fname.lower() in {"article.html", "abstract", "summary.pdf"} or "index.html" in fname.lower():
                                        continue
                                        
                                    links = f.get("links", {})
                                    furl = links.get("self") or links.get("content") or f.get("href") or ""
                                    if fname and furl:
                                        files.append({
                                            "file_key": fname,
                                            "file_url": furl,
                                            "tier": 1 if file_ext(fname) in [".qdpx", ".qdc", ".nvpx", ".mqda"] else 2
                                        })

                                if not files:
                                    continue

                                norm = _normalize_metadata(full_record)
                                task = {
                                    "context_repository": self.name,
                                    "project_identifier": f"{_slugify(norm['title'])}-{recid}",
                                    "record_meta": norm,
                                    "record_raw": hit,
                                    "files": files,
                                    "query_string": clean_term,
                                    "strict": strict_mode
                                }

                                await queue.put(task)
                                seen_recids.add(recid)
                                query_success_count += 1
                                total_enqueued += 1
                                logger.info("[%s] Validated record enqueued: %s", self.name, recid)

                                if max_records and query_success_count >= max_records:
                                    break

                            if (max_records and query_success_count >= max_records) or len(hits) < DEFAULT_PAGE_SIZE:
                                break
                            page += 1

                    except Exception as e:
                        logger.error("[%s] Request failed for '%s': %s", self.name, term, e)
                        break

                if max_records and total_enqueued >= (max_records * len(search_queries)): # Heuristic
                    pass 
        return total_enqueued

    async def _fetch_full_record(self, session: aiohttp.ClientSession, recid: str) -> dict[str, Any] | None:
        """Retrieves the complete metadata for a specific record ID."""
        url = f"{self._base_url}/records/{recid}"
        try:
            async with session.get(url) as response:
                await asyncio.sleep(1.0) # Local throttle for detail fetches
                
                if response.status == 429:
                    logger.warning("[%s] Throttled on detail fetch for %s. Waiting 60s.", self.name, recid)
                    await asyncio.sleep(60.0)
                    return await self._fetch_full_record(session, recid)
                
                if response.status != 200:
                    return None
                    
                return await response.json()
        except Exception as e:
            logger.debug("[%s] Detail fetch failed for %s: %s", self.name, recid, e)
            return None

