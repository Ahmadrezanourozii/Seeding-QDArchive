#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Implementation of the CESSDA (Consortium of European Social Science Data Archives) 
             harvester; specialized for metadata aggregation from a decentralized European 
             data infrastructure.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from constants import BROAD_TERMS, STRICT_TERMS
from harvester_base import AbstractBaseHarvester

logger = logging.getLogger(__name__)


class CessdaHarvester(AbstractBaseHarvester):
    """
    Harvester for the CESSDA Data Catalogue.
    
    CESSDA acts as an aggregator for European national data archives. 
    Since it primarily links to external hosts, this harvester extracts 
    comprehensive metadata and marks resources as externally hosted.
    """

    def __init__(self) -> None:
        super().__init__()
        self._api_endpoint = "https://datacatalogue.cessda.eu/api/DataSets/v2/search"

    @property
    def name(self) -> str:
        return "cessda"

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Harvests study metadata from CESSDA using the provided search criteria."""
        total_enqueued = 0
        seen_ids: set[str] = set()
        
        # Parallelize across multiple search terms
        search_tasks = []
        if query:
            search_tasks.append((query, True))
        else:
            search_tasks.extend([(q, True) for q in STRICT_TERMS])
            search_tasks.extend([(q, False) for q in BROAD_TERMS])

        async with self.create_client_session() as session:
            # Throttle parallel search waves
            semaphore = asyncio.Semaphore(10)
            
            async def _execute_search(q: str, is_strict_mode: bool) -> None:
                nonlocal total_enqueued
                limit = 200
                offset = 0

                while True:
                    if max_records and total_enqueued >= max_records:
                        break

                    params = {
                        "query": q,
                        "metadataLanguage": "en",
                        "limit": str(limit),
                        "offset": str(offset)
                    }

                    try:
                        async with semaphore:
                            async with session.get(self._api_endpoint, params=params) as resp:
                                if resp.status != 200:
                                    logger.error("[%s] Search failed for '%s': %d", self.name, q, resp.status)
                                    break
                                data = await resp.json()
                    except Exception as e:
                        logger.error("[%s] API request failed for '%s': %s", self.name, q, e)
                        break

                    results = data.get("Results", [])
                    if not results:
                        break

                    for item in results:
                        if max_records and total_enqueued >= max_records:
                            return
                            
                        item_id = item.get("id")
                        if not item_id or item_id in seen_ids:
                            continue
                        seen_ids.add(item_id)
                        
                        # Metadata extraction for relevance scoring
                        title = item.get("titleStudy", "")
                        abstract = item.get("abstract", "")
                        keywords = [k.get("term", "") for k in item.get("keywords", []) if "term" in k]
                        
                        if not is_strict_mode:
                            search_blob = f"{title} {abstract} {' '.join(keywords)}"
                            if not self.is_qda_match(search_blob):
                                continue

                        # Normalizing author metadata
                        authors = []
                        for creator in item.get("creators", []):
                            name = creator.get("name") if isinstance(creator, dict) else creator
                            if name:
                                authors.append({"name": str(name)})

                        study_url = item.get("studyUrl", item.get("studyXmlSourceUrl", ""))
                        
                        # Construct standard record metadata
                        record_meta = {
                            "title": title,
                            "description": abstract,
                            "doi": item_id if item_id.startswith("doi:") else "",
                            "creators_text": " | ".join(a["name"] for a in authors),
                            "persons": authors,
                            "license_id": item.get("dataAccess", "CESSDA-External-Terms"),
                            "project_url": study_url,
                        }

                        # Since CESSDA is an aggregator, we provide a placeholder file 
                        # that links to the original repository.
                        files_payload = [{
                            "file_key": "EXTERNAL_REPOSITORY_NOTICE.txt",
                            "file_url": "https://datacatalogue.cessda.eu",
                            "restricted": True,
                            "failure_reason": f"Study hosted externally. Resource access: {study_url}"
                        }]

                        await queue.put({
                            "context_repository": self.name,
                            "project_identifier": str(item_id),
                            "record_meta": record_meta,
                            "record_raw": item,
                            "files": files_payload,
                        })
                        total_enqueued += 1

                    if len(results) < limit:
                        break
                    offset += limit

            execution_waves = [_execute_search(q, strict) for q, strict in search_tasks]
            await asyncio.gather(*execution_waves)

        return total_enqueued

