#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Reusable Dataverse harvester implementation; supports any Dataverse-compatible repository 
             and provides granular control over subtrees and authentication.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

import aiohttp

from constants import STRICT_TERMS, file_ext, file_tier
from harvester_base import AbstractBaseHarvester

logger = logging.getLogger(__name__)

# Normalize strict terms for identification during search
STRICT_TERMS_SET = {t.strip('"') for t in STRICT_TERMS}


def _slugify(text: str, max_length: int = 80) -> str:
    """Generates a URL-safe slug from the project title."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled_project"


class GenericDataverseHarvester(AbstractBaseHarvester):
    """
    Harvester for Dataverse repositories.
    
    Can be instantiated for specific instances (e.g., Harvard, DANS) and 
    optionally restricted to specific sub-dataverses (subtrees).
    """

    def __init__(
        self, 
        base_url: str, 
        token: str | None = None, 
        source_id: str = "dataverse", 
        subtree: str | None = None
    ) -> None:
        super().__init__()
        self._base_url = base_url.rstrip("/")
        self._token = token or ""
        self._source_id = source_id
        self._subtree = subtree
        
        # Adjust pagination batch size based on known repository limits
        self._per_page = 10 if "harvard.edu" in self._base_url else 100

        if not self._token:
            logger.debug("[%s] Initialized without API token; requests will be unauthenticated.", self.name)

    @property
    def name(self) -> str:
        return self._source_id

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Harvests datasets from the Dataverse instance matching the provided search criteria."""
        if not query:
            logger.error("[%s] Search query is required for harvesting.", self.name)
            return 0

        search_queries = [s.strip() for s in query.split(" OR ") if s.strip()]
        headers = {"X-Dataverse-key": self._token} if self._token else {}

        total_enqueued = 0
        seen_ids: set[str] = set()

        async with self.create_client_session(headers=headers) as session:
            for term in search_queries:
                clean_term = term.strip('"')
                is_strict_term = clean_term in STRICT_TERMS_SET
                
                logger.info("[%s] Querying: '%s'", self.name, clean_term)
                start_offset = 0

                while True:
                    params = {
                        "q": clean_term if clean_term == "*" else f'"{clean_term}"',
                        "type": "dataset",
                        "per_page": self._per_page,
                        "start": start_offset,
                    }
                    if self._subtree:
                        params["subtree"] = self._subtree

                    try:
                        async with session.get(f"{self._base_url}/api/search", params=params) as response:
                            await asyncio.sleep(self.request_interval)

                            if response.status == 429:
                                logger.warning("[%s] Rate limit encountered. Throttling for 60s.", self.name)
                                await asyncio.sleep(60.0)
                                continue

                            if response.status != 200:
                                logger.error("[%s] API Error %d for '%s'", self.name, response.status, clean_term)
                                break

                            data = await response.json()
                            items = data.get("data", {}).get("items", [])
                            if not items:
                                break

                            for item in items:
                                global_id = item.get("global_id", "")
                                if not global_id or global_id in seen_ids:
                                    continue
                                
                                title = item.get("name", "")
                                raw_desc = item.get("description", "")
                                description = " ".join(raw_desc) if isinstance(raw_desc, list) else str(raw_desc)
                                
                                # Validate relevance via weighted scoring unless it's a known strict match
                                if not is_strict_term and clean_term != "*":
                                    if not self.is_qda_match(f"{title} {description}"):
                                        continue

                                logger.debug("[%s] Inspecting metadata for DOI: %s", self.name, global_id)
                                dataset_detail = await self._fetch_dataset_metadata(session, global_id)
                                if not dataset_detail:
                                    continue
                                
                                latest_version = dataset_detail.get("latestVersion", {})
                                files_raw = latest_version.get("files", [])
                                
                                files = []
                                for f_item in files_raw:
                                    is_restricted = f_item.get("restricted", False)
                                    embargo_date = f_item.get("embargoDate", "")
                                    data_file = f_item.get("dataFile", {})
                                    file_id = data_file.get("id")
                                    filename = data_file.get("filename") or f_item.get("label") or "unknown_file"

                                    # Skip system migration artifacts
                                    if "easy-migration.zip" in filename.lower() or "manifest" in filename.lower():
                                        continue

                                    ext = file_ext(filename)
                                    tier = file_tier(ext)
                                    
                                    if is_restricted:
                                        reason = f"Embargoed until {embargo_date}" if embargo_date else "Restricted access"
                                        files.append({
                                            "file_key": filename,
                                            "file_url": "",
                                            "restricted": True,
                                            "failure_reason": reason,
                                            "tier": tier if tier > 0 else 2
                                        })
                                    elif file_id:
                                        file_url = f"{self._base_url}/api/access/datafile/{file_id}"
                                        files.append({
                                            "file_key": filename,
                                            "file_url": file_url,
                                            "restricted": False,
                                            "tier": tier if tier > 0 else 2
                                        })

                                if not files:
                                    continue

                                enq_task = {
                                    "context_repository": self.name,
                                    "project_identifier": f"{_slugify(title)}-{global_id.replace('/', '-').replace(':', '-')}"[:120],
                                    "record_meta": {
                                        "title": title,
                                        "description": description,
                                        "doi": global_id,
                                        "creators": [{"name": a.get("name", ""), "role": "AUTHOR"} for a in item.get("authors", []) if isinstance(a, dict)],
                                        "project_url": f"{self._base_url}/dataset.xhtml?persistentId={global_id}",
                                        "repository_url": self._base_url,
                                        "license": item.get("license_url", ""),
                                        "upload_date": item.get("published_at", ""),
                                    },
                                    "record_raw": item,
                                    "files": files,
                                    "query_string": clean_term,
                                }
                                await queue.put(enq_task)
                                seen_ids.add(global_id)
                                total_enqueued += 1
                                
                                if max_records and total_enqueued >= max_records:
                                    return total_enqueued

                            if len(items) < self._per_page:
                                break
                            start_offset += self._per_page

                    except Exception as e:
                        logger.error("[%s] Harvest failed for '%s': %s", self.name, clean_term, e)
                        break

        return total_enqueued

    async def _fetch_dataset_metadata(self, session: aiohttp.ClientSession, doi: str) -> dict[str, Any] | None:
        """Retrieves detailed dataset metadata via persistent identifier."""
        url = f"{self._base_url}/api/datasets/:persistentId"
        params = {"persistentId": doi}
        try:
            async with session.get(url, params=params) as response:
                await asyncio.sleep(1.0) # Individual record throttle
                if response.status == 429:
                    await asyncio.sleep(60.0)
                    return await self._fetch_dataset_metadata(session, doi)
                if response.status != 200:
                    return None
                data = await response.json()
                return data.get("data")
        except Exception as e:
            logger.debug("[%s] Detail fetch failed for %s: %s", self.name, doi, e)
            return None

