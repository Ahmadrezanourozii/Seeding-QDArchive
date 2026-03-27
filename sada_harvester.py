#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Implementation of the SADA (DataFirst) API harvester; specialized for 
             African social science data discovery and NADA-based metadata aggregation.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any
from urllib.parse import quote

import aiohttp

from config import Settings, get_settings
from constants import (
    BROAD_TERMS,
    QDA_EXTENSIONS,
    STRICT_TERMS,
    file_ext,
    file_tier,
    has_required_qda_project_file,
)
from harvester_base import AbstractBaseHarvester

# SADA uses the same NADA catalog engine as IHSN
SADA_API_BASE = "https://www.datafirst.uct.ac.za/dataportal/index.php/api"

logger = logging.getLogger(__name__)

# Normalize strict terms for identification during search
STRICT_TERMS_SET = {t.strip('"') for t in STRICT_TERMS}


def _slugify(text: str, max_length: int = 80) -> str:
    """Generates a URL-safe slug from the project title."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled_project"


class SadaHarvester(AbstractBaseHarvester):
    """
    Harvester for the SADA (DataFirst) catalog.
    
    Crawls the DataFirst portal to discover African social science datasets, 
    performing deep resource inspection to identify relevant QDA files.
    """

    def __init__(
        self, 
        settings: Settings | None = None, 
        *, 
        request_interval: float | None = None
    ) -> None:
        super().__init__(request_interval=request_interval)
        self._settings = settings or get_settings()
        self._token = self._settings.api.get("sada")
        # Throttle concurrent resource inspection to prevent API saturation
        self._semaphore = asyncio.Semaphore(15)

    @property
    def name(self) -> str:
        return "sada"

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Harvests datasets from SADA matching the provided search criteria."""
        search_queries = [s.strip() for s in query.split(" OR ") if s.strip()] if query else (STRICT_TERMS + BROAD_TERMS)
        
        total_enqueued = 0
        seen_ids: set[str] = set()

        async with self.create_client_session() as session:
            headers = {"X-API-KEY": self._token} if self._token else {}
            if not self._token:
                logger.warning("[%s] No API token provided; rate limits may apply.", self.name)
            
            for q_term in search_queries:
                clean_term = q_term.strip('"')
                is_strict_term = clean_term in STRICT_TERMS_SET
                is_wildcard = clean_term == "*"
                
                logger.info("[%s] Querying: '%s'", self.name, clean_term)
                page = 1
                page_size = 50 
                
                while True:
                    search_url = f"{SADA_API_BASE}/catalog/search?ps={page_size}&page={page}&sk={quote(q_term)}"
                    if is_wildcard:
                        search_url = f"{SADA_API_BASE}/catalog/search?ps={page_size}&page={page}"
                        
                    try:
                        async with session.get(search_url, headers=headers) as resp:
                            if resp.status != 200:
                                logger.error("[%s] Search failed with status %d", self.name, resp.status)
                                break
                            data = await resp.json()
                            
                        result_data = data.get("result", {})
                        rows = result_data.get("rows", [])
                        if not rows:
                            break

                        # Inspect records in parallel for this result page
                        inspection_tasks = []
                        for row in rows:
                            idno = row.get("idno")
                            if idno and idno not in seen_ids:
                                inspection_tasks.append(
                                    self._inspect_record(session, row, headers, is_strict_term, is_wildcard)
                                )
                                seen_ids.add(idno)
                        
                        if inspection_tasks:
                            results = await asyncio.gather(*inspection_tasks)
                            for record_task in results:
                                if record_task:
                                    await queue.put(record_task)
                                    total_enqueued += 1
                                    if max_records and total_enqueued >= max_records:
                                        return total_enqueued

                        if len(rows) < page_size:
                            break
                        page += 1
                    except Exception as e:
                        logger.error("[%s] Search failed for '%s': %s", self.name, clean_term, e)
                        break

        return total_enqueued

    async def _inspect_record(
        self, 
        session: aiohttp.ClientSession, 
        row: dict[str, Any], 
        headers: dict[str, str],
        is_strict: bool,
        is_wildcard: bool
    ) -> dict[str, Any] | None:
        """Performs deep inspection of a single SADA record and its associated resources."""
        idno = row.get("idno")
        if not idno:
            return None
            
        async with self._semaphore:
            details_url = f"{SADA_API_BASE}/catalog/{idno}"
            resources_url = f"{SADA_API_BASE}/catalog/resources/{idno}"
            
            try:
                # Fetch detailed metadata
                async with session.get(details_url, headers=headers) as d_resp:
                    if d_resp.status != 200:
                        return None
                    d_data = await d_resp.json()
                    dataset = d_data.get("dataset", {})
                
                # Fetch accompanying files/resources
                async with session.get(resources_url, headers=headers) as r_resp:
                    if r_resp.status != 200:
                        resources_list = []
                    else:
                        r_data = await r_resp.json()
                        resources_list = r_data.get("resources", [])
            except Exception as e:
                logger.debug("[%s] Inspection failed for %s: %s", self.name, idno, e)
                return None

        # Consolidate metadata blocks for relevance scoring (skip for strict/wildcard)
        if not is_strict and not is_wildcard:
            text_blocks = [
                row.get("title", ""),
                row.get("subtitle", ""),
                row.get("nation", ""),
                dataset.get("title", ""),
                dataset.get("subtitle", ""),
                str(dataset.get("abstract", "")),
                str(dataset.get("description", "")),
                str(dataset.get("study_notes", "")),
            ]
            for res in resources_list:
                text_blocks.append(res.get("title", ""))
                text_blocks.append(res.get("filename", ""))
            
            if not self.is_qda_match(" ".join(filter(None, text_blocks))):
                return None
            
        file_entries = []
        for res in resources_list:
            url = res.get("url")
            filename = res.get("filename") or res.get("title")
            if not url or not filename:
                continue
            
            ext = file_ext(filename)
            tier = file_tier(ext)
            if tier == 0:
                continue
                
            file_entries.append({
                "file_key": filename,
                "file_url": url,
                "tier": tier,
                "is_qda": ext in QDA_EXTENSIONS,
                "is_required_qda": has_required_qda_project_file(ext),
            })
        
        if not file_entries:
            return None

        # Build production-ready metadata container
        recid_safe = idno.replace("/", "-")[:80]
        title = row.get("title", "Untitled Study")
        project_identifier = f"{_slugify(title)}-{recid_safe}"[:120]
        
        metadata = dataset.get("metadata", {})
        doc_desc = metadata.get("doc_desc", {})
        producers = doc_desc.get("producers", [])
        persons = [{"name": p.get("name", ""), "role": p.get("role", "PRODUCER")} for p in producers]

        record_meta = {
            "recid": idno,
            "title": title,
            "creators_text": " | ".join([p["name"] for p in persons if p.get("name")]),
            "description": row.get("subtitle", "") or title,
            "license_id": "DataFirst-Terms-of-Use",
            "raw": dataset,
            "persons": persons
        }
        
        return {
            "record_meta": record_meta,
            "record_raw": dataset,
            "project_identifier": project_identifier,
            "context_repository": self.name,
            "files": file_entries
        }

