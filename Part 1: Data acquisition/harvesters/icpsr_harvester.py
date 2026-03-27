#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Implementation of the ICPSR repository harvester; uses search result scraping 
             to extract embedded JSON metadata.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import aiohttp

from constants import BROAD_TERMS, STRICT_TERMS
from harvester_base import AbstractBaseHarvester

logger = logging.getLogger(__name__)


def _slugify(text: str, max_length: int = 80) -> str:
    """Generates a URL-safe slug from the project title."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled_project"


class ICPSRHarvester(AbstractBaseHarvester):
    """
    Harvester for the ICPSR repository.
    
    Operates by scraping the search results page and parsing embedded JSON objects 
    to retrieve study metadata.
    """

    def __init__(self, request_interval: float | None = None) -> None:
        super().__init__(request_interval=request_interval)
        self._search_url = "https://www.icpsr.umich.edu/web/ICPSR/search/studies"

    @property
    def name(self) -> str:
        return "icpsr"

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Harvests ICPSR studies via search result scraping."""
        search_queries = [s.strip() for s in query.split(" OR ") if s.strip()] if query else (STRICT_TERMS + BROAD_TERMS)
        
        total_enqueued = 0
        seen_study_ids: set[str] = set()

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        }

        async with self.create_client_session(headers=headers) as session:
            for q_text in search_queries:
                logger.info("[%s] Querying: '%s'", self.name, q_text)
                start_offset = 0
                rows_per_page = 50
                query_enqueued = 0
                
                while True:
                    params = {
                        "q": q_text,
                        "start": start_offset,
                        "rows": rows_per_page,
                    }
                    
                    try:
                        async with session.get(self._search_url, params=params) as resp:
                            if resp.status != 200:
                                logger.error("[%s] Search failed with status %d for query: %s", self.name, resp.status, q_text)
                                break
                            
                            html = await resp.text()
                            # Locate the embedded raw JSON metadata block in the HTML source
                            json_match = re.search(r'searchResults\s*:\s*(\{.*?\})\s*,\s*searchConfig', html, re.DOTALL)
                            if not json_match:
                                break
                            
                            try:
                                data = json.loads(json_match.group(1))
                            except json.JSONDecodeError as e:
                                logger.debug("[%s] JSON parsing failed for query '%s': %s", self.name, q_text, e)
                                break
                            
                            response_data = data.get("response", {})
                            docs = response_data.get("docs", [])
                            num_found = response_data.get("numFound", 0)
                            
                            if not docs:
                                break

                            new_records_on_page = 0
                            for doc in docs:
                                study_id = str(doc.get("ID", doc.get("STUDYQ", "")))
                                if not study_id or study_id in seen_study_ids:
                                    continue
                                
                                title = doc.get("TITLE", "Untitled Study")
                                abstract = " ".join(doc.get("SUMMARY", []) + doc.get("DESCRIPTION", []) + doc.get("NOTES", []))
                                
                                # Validate relevance via weighted scoring
                                if not self.is_qda_match(f"{title} {abstract}"):
                                    continue
                                
                                seen_study_ids.add(study_id)
                                new_records_on_page += 1
                                
                                authors = ", ".join(doc.get("AUTHOR", []))
                                date_str = doc.get("DATEUPDATED", "")
                                update_date = date_str.split("T")[0] if date_str else ""
                                
                                landing_url = doc.get("URL") or f"https://www.icpsr.umich.edu/web/ICPSR/studies/{study_id}"
                                project_id = f"{_slugify(title)}-{study_id}"[:120]
                                
                                await queue.put({
                                    "context_repository": self.name,
                                    "project_identifier": project_id,
                                    "record_meta": {
                                        "recid": study_id,
                                        "title": title,
                                        "creators_text": authors,
                                        "license_id": "restricted",
                                        "upload_date": update_date,
                                        "raw": {"search_query": q_text, "archive": doc.get("ARCHIVE", [])},
                                    },
                                    "record_raw": doc,
                                    "files": [{
                                        "file_key": landing_url,
                                        "file_url": landing_url,
                                        "tier": 2,
                                        "is_qda": False,
                                    }],
                                    "query_string": q_text,
                                })
                                query_enqueued += 1
                                total_enqueued += 1

                                if max_records and total_enqueued >= max_records:
                                    return total_enqueued
                            
                            if start_offset + rows_per_page >= num_found or new_records_on_page == 0:
                                break
                            
                            start_offset += rows_per_page
                            await asyncio.sleep(self.request_interval)
                            
                    except Exception as e:
                        logger.error("[%s] scraping error for '%s': %s", self.name, q_text, e)
                        break
        
        return total_enqueued

