#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Shared utility module for OAI-PMH interactions; provides XML parsing, 
             metadata extraction, and pagination logic for OAI harvesters.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp
import yarl

from constants import (
    ARCHIVE_EXTENSIONS,
    MEDIA_EXTENSIONS,
    QDA_EXTENSIONS,
    TEXT_EXTENSIONS,
    has_required_qda_project_file,
    is_open_license,
)

logger = logging.getLogger(__name__)

# Configuration constants for OAI-PMH harvesting
OAI_REQUEST_TIMEOUT = 45
DISCOVERY_CONCURRENCY = int(os.environ.get("OAI_DISCOVERY_CONCURRENCY", "3"))

# XML Namespaces for common metadata schemas
NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "dcterms": "http://purl.org/dc/terms/",
    "datacite": "http://datacite.org/schema/kernel-4",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
}


def _slugify(text: str, max_length: int = 80) -> str:
    """Generates a URL-safe slug from the project title."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled_project"


def _extract_extension(path: str) -> str:
    """Extracts the file extension, handling special QDA types."""
    path_lower = path.lower()
    if path_lower.endswith(".atlproj"):
        return ".atlproj"
    dot_index = path_lower.rfind(".")
    return path_lower[dot_index:] if dot_index >= 0 else ""


def _map_file_tier(extension: str) -> int:
    """Maps a file extension to a priority tier (1=QDA, 2=Text/Archive, 3=Media)."""
    if extension in QDA_EXTENSIONS:
        return 1
    if extension in TEXT_EXTENSIONS or extension in ARCHIVE_EXTENSIONS:
        return 2
    if extension in MEDIA_EXTENSIONS:
        return 3
    return 0


async def oai_request(
    session: aiohttp.ClientSession,
    base_url: str,
    params: dict[str, str],
    request_interval: float,
    timeout: float | None = None,
) -> str:
    """Executes a polite OAI-PMH request with robust error handling and retries."""
    url_base = base_url.split("?")[0]
    
    ps = dict(params)
    if "resumptionToken" in ps:
        token = ps.pop("resumptionToken")
        query_string = urlencode(ps) + f"&resumptionToken={quote(token)}"
    else:
        query_string = urlencode(ps)
        
    full_url = f"{url_base}?{query_string}"
    url_obj = yarl.URL(full_url, encoded=True)
    
    await asyncio.sleep(request_interval)
    
    client_timeout = aiohttp.ClientTimeout(
        total=timeout if timeout is not None else OAI_REQUEST_TIMEOUT
    )
    
    max_retries = 3
    # Fallback response for persistent 404s
    empty_response = (
        '<?xml version="1.0"?><OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">'
        '<ListRecords></ListRecords></OAI-PMH>'
    )
    
    for attempt in range(max_retries + 1):
        try:
            async with session.get(url_obj, timeout=client_timeout) as resp:
                if resp.status == 404:
                    return empty_response
                resp.raise_for_status()
                return await resp.text(encoding="utf-8", errors="replace")
                
        except (asyncio.TimeoutError, aiohttp.ServerTimeoutError) as e:
            if attempt == max_retries:
                raise
            logger.warning("[%s] Timeout on attempt %d: %s", base_url, attempt + 1, e)
            await asyncio.sleep(2 * (attempt + 1))
            
        except aiohttp.ClientResponseError as e:
            if e.status in (429, 500, 502, 503, 504) and attempt < max_retries:
                backoff = 10 * (2**attempt)
                logger.warning("[%s] API Error %d; retrying in %ds", base_url, e.status, backoff)
                await asyncio.sleep(backoff)
                continue
            raise
            
    return ""


def parse_oai_response(xml_text: str) -> ET.Element:
    """Parses OAI-PMH XML and checks for service-level errors."""
    root = ET.fromstring(xml_text)
    error_el = root.find(".//{http://www.openarchives.org/OAI/2.0/}error")
    if error_el is not None:
        error_code = error_el.get("code", "")
        if error_code == "noRecordsMatch":
            return root
        error_msg = (error_el.text or "").strip()
        raise RuntimeError(f"OAI-PMH error [{error_code}]: {error_msg}")
    return root


def get_resumption_token(root: ET.Element) -> str | None:
    """Extracts the resumption token for pagination."""
    token_el = root.find(".//{http://www.openarchives.org/OAI/2.0/}resumptionToken")
    if token_el is not None and token_el.text:
        return token_el.text.strip()
    return None


def get_oai_records(root: ET.Element) -> list[ET.Element]:
    """Retrieves all record elements from an OAI-PMH response."""
    return root.findall(".//{http://www.openarchives.org/OAI/2.0/}record")


def extract_metadata_element(record: ET.Element, metadata_prefix: str) -> ET.Element | None:
    """Accesses the specific metadata container within an OAI record."""
    meta_container = record.find(".//{http://www.openarchives.org/OAI/2.0/}metadata")
    if meta_container is None:
        return None
        
    if metadata_prefix == "oai_dc":
        return meta_container.find(".//{http://purl.org/dc/elements/1.1/}dc") or meta_container
    if metadata_prefix == "oai_datacite":
        return meta_container.find(".//{http://datacite.org/schema/kernel-4}resource") or meta_container
    return meta_container


def extract_links_from_metadata(metadata_el: ET.Element | None, metadata_prefix: str) -> list[tuple[str, str]]:
    """Crawls metadata elements to find potential file URLs and resource identifiers."""
    links: list[tuple[str, str]] = []
    if metadata_el is None:
        return links

    def _add_link(url: str, key_suffix: str | None = None) -> None:
        url = url.strip()
        if not url or not url.startswith("http"):
            return
        # Infer a filename if possible, otherwise use a generic key
        key = key_suffix or url.split("/")[-1].split("?")[0] or "resource_link"
        links.append((key, url))

    if metadata_prefix == "oai_dc":
        for tag in ["identifier", "relation", "source"]:
            for el in metadata_el.findall(f".//{{{NS['dc']}}}{tag}"):
                text = (el.text or "").strip()
                if text:
                    _add_link(text)
    else:
        # Datacite specific inspection
        for tag in ["identifier", "alternateIdentifier", "relatedIdentifier"]:
            for el in metadata_el.findall(f".//{{http://datacite.org/schema/kernel-4}}{tag}"):
                text = (el.text or "").strip()
                if text.startswith("http"):
                    _add_link(text)

    return links


def etree_to_dict(el: ET.Element) -> dict[str, Any]:
    """Recursively flattens an ElementTree into a nested dictionary."""
    data: dict[str, Any] = {}
    for child in el:
        tag_name = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if child.text and child.text.strip():
            if tag_name in data:
                if not isinstance(data[tag_name], list):
                    data[tag_name] = [data[tag_name]]
                data[tag_name].append(child.text.strip())
            else:
                data[tag_name] = child.text.strip()
        elif len(child):
            data[tag_name] = etree_to_dict(child)
    return data


async def oai_list_records(
    session: aiohttp.ClientSession,
    base_url: str,
    metadata_prefix: str,
    *,
    from_date: str | None = None,
    until_date: str | None = None,
    set_spec: str | None = None,
    request_interval: float = 0.5,
    resumption_token: str | None = None,
    timeout: float | None = None,
) -> tuple[list[ET.Element], str | None]:
    """Performs a ListRecords operation, handling either an initial request or pagination."""
    if resumption_token:
        # Respect target repository with a longer delay between pages if token-based
        await asyncio.sleep(1.0)
        params = {"verb": "ListRecords", "resumptionToken": resumption_token}
    else:
        params = {"verb": "ListRecords", "metadataPrefix": metadata_prefix}
        if from_date:
            params["from"] = from_date
        if until_date:
            params["until"] = until_date
        if set_spec:
            params["set"] = set_spec
            
    xml_text = await oai_request(session, base_url, params, request_interval, timeout=timeout)
    try:
        root = parse_oai_response(xml_text)
    except Exception as e:
        logger.warning("[%s] Response parsing failed: %s", base_url, e)
        return [], None
        
    return get_oai_records(root), get_resumption_token(root)


def build_file_entries(links: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """Filters discovered links and maps them to sanitized file entry structures."""
    entries = []
    for file_key, file_url in links:
        ext = _extract_extension(file_key)
        tier = _map_file_tier(ext)
        if tier == 0:
            continue
            
        entries.append({
            "file_key": file_key,
            "file_url": file_url,
            "tier": tier,
            "is_qda": ext in QDA_EXTENSIONS,
            "is_required_qda": has_required_qda_project_file(ext),
        })
    return entries


def get_oai_identifier(record: ET.Element) -> str:
    """Extracts the unique OAI identifier from the record header."""
    header = record.find(".//{http://www.openarchives.org/OAI/2.0/}header")
    if header is None:
        return ""
    ident_el = header.find("{http://www.openarchives.org/OAI/2.0/}identifier")
    return (ident_el.text or "").strip()


def get_title_and_creators(metadata_el: ET.Element | None, metadata_prefix: str) -> tuple[str, str]:
    """Standardizes extraction of title and creator strings across namespaces."""
    title, creators = "untitled_project", ""
    if metadata_el is None:
        return title, creators
        
    if metadata_prefix == "oai_dc":
        title_el = metadata_el.find(".//{http://purl.org/dc/elements/1.1/}title")
        if title_el is not None and title_el.text:
            title = title_el.text.strip()
            
        creator_els = metadata_el.findall(".//{http://purl.org/dc/elements/1.1/}creator")
        creators = " | ".join(c.text.strip() for c in creator_els if c.text)
    else:
        # Datacite parsing
        title_el = metadata_el.find(".//{http://datacite.org/schema/kernel-4}title")
        if title_el is not None and title_el.text:
            title = title_el.text.strip()
            
        creator_name_els = metadata_el.findall(".//{http://datacite.org/schema/kernel-4}creatorName")
        creators = " | ".join(c.text.strip() for c in creator_name_els if c.text)
        
    return title, creators


def get_license_from_meta(metadata_el: ET.Element | None, metadata_prefix: str) -> str | None:
    """Attempts to retrieve license or usage rights information."""
    if metadata_el is None:
        return None
        
    if metadata_prefix == "oai_dc":
        rights_el = metadata_el.find(".//{http://purl.org/dc/elements/1.1/}rights")
        if rights_el is not None and rights_el.text:
            return rights_el.text.strip()
    else:
        # Check standard Datacite rights element
        rights_el = metadata_el.find(".//{http://datacite.org/schema/kernel-4}rights")
        if rights_el is not None and rights_el.text:
            return rights_el.text.strip()
            
    return None


async def generic_oai_fetch(
    session: aiohttp.ClientSession,
    base_url: str,
    queue: asyncio.Queue[dict[str, Any]],
    repository_name: str,
    seen_urls: set[str],
    max_records: int | None,
    request_interval: float,
    prefixes: tuple[str, ...] = ("oai_datacite", "oai_dc"),
) -> int:
    """Generic OAI-PMH crawler for standard metadata retrieval."""
    active_prefix = prefixes[0]
    # Autodetect supported metadata prefix
    for pref in prefixes:
        try:
            await oai_list_records(session, base_url, pref, request_interval=request_interval)
            active_prefix = pref
            break
        except Exception:
            continue
            
    total_harvested = 0
    token: str | None = None
    
    while True:
        records, next_token = await oai_list_records(
            session, base_url, active_prefix, 
            request_interval=request_interval, 
            resumption_token=token
        )
        
        for record in records:
            if max_records and total_harvested >= max_records:
                return total_harvested
                
            meta_el = extract_metadata_element(record, active_prefix)
            license_val = get_license_from_meta(meta_el, active_prefix)
            
            # Enforce open data policy for generic discovery
            if not is_open_license(license_val):
                continue
                
            links = extract_links_from_metadata(meta_el, active_prefix)
            file_entries = build_file_entries(links)
            if not file_entries:
                continue
                
            # Avoid re-processing globally seen URLs
            fresh_entries = [e for e in file_entries if e["file_url"] not in seen_urls]
            if not fresh_entries:
                continue
                
            seen_urls.update(e["file_url"] for e in fresh_entries)
            
            oai_id = get_oai_identifier(record)
            title, creators = get_title_and_creators(meta_el, active_prefix)
            project_id = f"{_slugify(title)}-{_slugify(oai_id)[:40]}"[:120]
            
            task = {
                "context_repository": repository_name,
                "project_identifier": project_id,
                "record_meta": {
                    "recid": oai_id,
                    "title": title,
                    "creators_text": creators,
                    "license_id": license_val,
                    "raw": {},
                },
                "record_raw": {"oai_id": oai_id},
                "files": fresh_entries,
            }
            await queue.put(task)
            total_harvested += 1
            
        if not next_token:
            break
        token = next_token
        
    return total_harvested


def date_range_chunks(
    from_year: int = 2000,
    interval_months: int = 6,
    until_iso: str | None = None,
) -> list[tuple[str, str]]:
    """Partitions a multi-year date range into manageable monthly chunks."""
    today = date.today()
    limit_date = today
    if until_iso:
        try:
            limit_date = date.fromisoformat(until_iso[:10])
        except ValueError:
            pass
            
    start_date = date(from_year, 1, 1)
    if start_date >= limit_date:
        return [(start_date.isoformat(), limit_date.isoformat())]
        
    chunks: list[tuple[str, str]] = []
    current_date = start_date
    
    while current_date < limit_date:
        # Calculate next boundary
        next_month = current_date.month + interval_months
        next_year = current_date.year + (next_month - 1) // 12
        next_month = (next_month - 1) % 12 + 1
        
        next_start = date(next_year, next_month, 1)
        chunk_end = next_start - timedelta(days=1)
        if chunk_end > limit_date:
            chunk_end = limit_date
            
        chunks.append((current_date.isoformat(), chunk_end.isoformat()))
        if next_start >= limit_date:
            break
        current_date = next_start
        
    return chunks


def date_range_slices(
    from_iso: str, 
    until_iso: str, 
    num_slices: int
) -> list[tuple[str, str]]:
    """Divides an ISO date range into N roughly equal sub-ranges for parallel processing."""
    try:
        start_dt = datetime.strptime(from_iso[:10], "%Y-%m-%d")
        end_dt = datetime.strptime(until_iso[:10], "%Y-%m-%d")
    except ValueError:
        return [(from_iso, until_iso)]
        
    if start_dt >= end_dt:
        return [(from_iso, until_iso)]
        
    delta_seconds = (end_dt - start_dt).total_seconds() / num_slices
    slices = []
    
    for i in range(num_slices):
        slice_start = start_dt.timestamp() + (i * delta_seconds)
        slice_end = start_dt.timestamp() + ((i + 1) * delta_seconds) if i < num_slices - 1 else end_dt.timestamp()
        
        slices.append((
            datetime.utcfromtimestamp(slice_start).strftime("%Y-%m-%d"),
            datetime.utcfromtimestamp(slice_end).strftime("%Y-%m-%d"),
        ))
        
    return slices

