#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Implementation of the Dryad repository harvester; handles OAuth2 authentication, search query batching, and QDA relevance filtering.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any
from urllib.parse import quote

import aiohttp

from config import Settings, get_settings
from constants import (
    QDA_EXTENSIONS,
    STRICT_SEARCH_QUERY,
    file_ext,
    file_tier,
)
from harvester_base import AbstractBaseHarvester

logger = logging.getLogger(__name__)

DRYAD_API_BASE = "https://datadryad.org/api/v2"
DRYAD_OAUTH_URL = "https://datadryad.org/oauth/token"
MAX_RETRIES_429 = 10
DEFAULT_PAGE_SIZE = 50


def _initialize_cpu_pool() -> Executor:
    """Initializes a process pool for parallelizing file metadata calculations."""
    max_workers = max(1, (os.cpu_count() or 2) - 1)
    try:
        return ProcessPoolExecutor(max_workers=max_workers)
    except Exception:
        # Fallback to threads if process forking is restricted in the environment.
        return ThreadPoolExecutor(max_workers=max_workers)


_CPU_POOL = _initialize_cpu_pool()


def _slugify(text: str, max_length: int = 80) -> str:
    """Generates a URL-safe slug from the project title."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled_project"


def _map_license(license_url: str | None) -> str | None:
    """Maps a Dryad license URL to a standardized internal identifier."""
    if not license_url:
        return None
    url_lower = license_url.lower()
    if "cc0" in url_lower or "cc-zero" in url_lower:
        return "cc0"
    if "cc-by" in url_lower:
        return "cc-by"
    return license_url


def _format_authors(authors: list[dict[str, Any]] | None) -> str:
    """Formats a list of author dictionary objects into a single string."""
    if not authors:
        return ""
    names = []
    for author in authors:
        first = author.get("firstName", "").strip()
        last = author.get("lastName", "").strip()
        full_name = f"{last}, {first}".strip(", ")
        if full_name:
            names.append(full_name)
    return " | ".join(names)


async def _get_access_token(
    session: aiohttp.ClientSession, client_id: str, client_secret: str
) -> str | None:
    """Retrieves an OAuth2 access token via the client_credentials grant."""
    try:
        async with session.post(
            DRYAD_OAUTH_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("access_token")
    except Exception as e:
        logger.debug("OAuth2 token retrieval failed: %s", e)
        return None


async def _managed_request(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    throttle: float,
    method: str = "GET",
    retries: int = 0,
) -> dict[str, Any] | list[Any] | None:
    """Performs an HTTP request with exponential backoff for rate limiting."""
    await asyncio.sleep(throttle)
    try:
        async with session.request(method, url, headers=headers) as resp:
            if resp.status == 429:
                if retries >= MAX_RETRIES_429:
                    return None
                wait_time = 5.0 * (2**retries)
                logger.warning("[%s] Rate limited. Retrying in %.1fs...", url, wait_time)
                await asyncio.sleep(wait_time)
                return await _managed_request(
                    session, url, headers, throttle, method, retries + 1
                )
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        logger.error("Request failed: %s (%s)", url, e)
        return None


def _filter_files_metadata(files_list: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filters and categorizes file metadata; executed in a CPU-bound pool."""
    results = []
    for f in files_list:
        path = f.get("path", "") or f.get("name", "")
        if not path:
            continue
        links = f.get("_links", {}) or f.get("links", {})
        href = (links.get("stash:download") or {}).get("href") or links.get("download", "")
        if not href:
            continue
        if not href.startswith("http"):
            href = "https://datadryad.org" + href

        ext = file_ext(path)
        tier = file_tier(ext)
        if tier > 0:
            results.append({
                "file_key": path.split("/")[-1],
                "file_url": href,
                "tier": tier,
                "is_qda": ext in QDA_EXTENSIONS,
            })
    return results


class DryadHarvester(AbstractBaseHarvester):
    """Harvester implementation for the Dryad Data Repository."""

    def __init__(
        self, settings: Settings | None = None, *, request_interval: float | None = None
    ):
        super().__init__(request_interval=request_interval)
        self._settings = settings or get_settings()
        self._api_settings = self._settings.api
        self._account_id = self._api_settings.get("dryad_account_id")
        self._secret = self._api_settings.get("dryad_secret")
        self._token = self._api_settings.get("dryad")

    @property
    def name(self) -> str:
        return "dryad"

    async def _ensure_authentication(self, session: aiohttp.ClientSession) -> str | None:
        """Ensures a valid OAuth2 token is available for the current session."""
        if self._token:
            return self._token
        if self._account_id and self._secret:
            self._token = await _get_access_token(session, self._account_id, self._secret)
        return self._token

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Harvests records from Dryad and enqueues tasks for valid candidates."""
        total_enqueued = 0
        seen_ids: set[str] = set()
        
        # sniper mode: default to specialized QDA queries if no override is provided.
        search_query = query or STRICT_SEARCH_QUERY
        keywords = [s.strip('"') for s in search_query.split(" OR ") if s.strip()] or [""]

        async with self.create_client_session() as session:
            token = await self._ensure_authentication(session)
            headers = {"Authorization": f"Bearer {token}"} if token else {}

            for kw in keywords:
                page = 1
                query_enqueued = 0
                logger.info("[%s] Beginning sequence for: '%s'", self.name, kw or "unfiltered")

                while True:
                    search_url = f"{DRYAD_API_BASE}/search?per_page={DEFAULT_PAGE_SIZE}&page={page}"
                    search_url += f"&q={quote(kw or '*')}"

                    data = await _managed_request(session, search_url, headers, self.request_interval)
                    if not data or not isinstance(data, dict):
                        break

                    embedded = data.get("_embedded", {})
                    datasets = embedded.get("stash:datasets", [])
                    if not datasets:
                        break

                    for ds in datasets:
                        did = ds.get("identifier", "") or ds.get("id", "")
                        if not did or did in seen_ids:
                            continue

                        # QDA Firewall check via Weighted Scoring
                        combined_text = f"{ds.get('title', '')} {ds.get('abstract', '')}"
                        if not self.is_qda_match(combined_text):
                            continue

                        # Fetch version details to resolve file hierarchy
                        version_links = (ds.get("_links", {})).get("stash:version", {})
                        v_href = version_links.get("href") if isinstance(version_links, dict) else None
                        if not v_href:
                            continue
                        if not v_href.startswith("http"):
                            v_href = "https://datadryad.org" + v_href

                        v_data = await _managed_request(session, v_href, headers, self.request_interval)
                        if not v_data:
                            continue

                        # Resolve file URLs via the stash:files relationship
                        f_links = (v_data.get("_links", {})).get("stash:files", {})
                        f_href = f_links.get("href") if isinstance(f_links, dict) else None
                        if not f_href:
                            continue
                        if not f_href.startswith("http"):
                            f_href = "https://datadryad.org" + f_href

                        f_data = await _managed_request(session, f_href, headers, self.request_interval)
                        f_list = (f_data.get("_embedded", {})).get("stash:files", []) if f_data else []

                        # Delegate file metadata processing to the CPU pool
                        loop = asyncio.get_running_loop()
                        f_entries = await loop.run_in_executor(_CPU_POOL, _filter_files_metadata, f_list)

                        if f_entries:
                            # Normalize metadata for persistence
                            title = (v_data or ds).get("title", ds.get("title", "Untitled"))
                            author_text = _format_authors((v_data or ds).get("authors", []))
                            recid = did.replace("doi:", "").replace("/", "-")[:50]
                            
                            norm_meta = {
                                "recid": did,
                                "title": title,
                                "creators_text": author_text,
                                "license_id": _map_license(ds.get("license") or v_data.get("license")) or "cc0",
                            }
                            
                            await queue.put({
                                "record_meta": norm_meta,
                                "record_raw": {"dataset": ds, "version": v_data, "files": f_list},
                                "project_identifier": f"{_slugify(title)}-{recid}"[:120],
                                "context_repository": self.name,
                                "files": f_entries,
                            })
                            
                            seen_ids.add(did)
                            query_enqueued += 1
                            total_enqueued += 1
                            if max_records and query_enqueued >= max_records:
                                break

                    if (max_records and query_enqueued >= max_records) or len(datasets) < DEFAULT_PAGE_SIZE:
                        break
                    page += 1

        return total_enqueued

