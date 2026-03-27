#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Implementation of the Odum Institute (UNC) OAI-PMH harvester; specialized for 
             social science data discovery using the oai_datacite metadata schema.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from constants import is_open_license
from harvester_base import AbstractBaseHarvester
from oai_common import (
    build_file_entries,
    extract_all_links_from_metadata,
    extract_metadata_element,
    get_license_from_metadata,
    get_oai_identifier,
    get_title_and_creators_from_metadata,
    get_uploader_email_from_metadata,
    oai_list_records,
    record_passes_qda_filter,
)

# Odum Institute's Dataverse OAI-PMH endpoint
ODUM_OAI_ENDPOINT = "https://dataverse.unc.edu/oai"
METADATA_PREFIX = "oai_datacite"

logger = logging.getLogger(__name__)


def _slugify(text: str, max_length: int = 40) -> str:
    """Generates a URL-safe slug from the provided text."""
    text = re.sub(r"[^\w\s-]", "", text).strip()
    text = re.sub(r"[-\s]+", "-", text).strip("-")[:max_length]
    return text.lower() or "untitled"


class OdumHarvester(AbstractBaseHarvester):
    """
    Harvester for the Odum Institute Data Archive.
    
    Utilizes OAI-PMH protocols with the oai_datacite schema to discover 
    and aggregate high-quality social science datasets.
    """

    def __init__(self, request_interval: float | None = None) -> None:
        super().__init__(request_interval=request_interval)

    @property
    def name(self) -> str:
        return "odum"

    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Harvests study metadata from Odum Institute via OAI-PMH."""
        total_enqueued = 0
        resumption_token: str | None = None

        async with self.create_client_session() as session:
            while True:
                records, next_token = await oai_list_records(
                    session,
                    ODUM_OAI_ENDPOINT,
                    METADATA_PREFIX,
                    request_interval=self.request_interval,
                    resumption_token=resumption_token,
                )
                
                for record in records:
                    if max_records and total_enqueued >= max_records:
                        return total_enqueued
                    
                    metadata_element = extract_metadata_element(record, METADATA_PREFIX)
                    license_str = get_license_from_metadata(metadata_element, METADATA_PREFIX)
                    
                    # Enforce open data access standards
                    if not is_open_license(license_str):
                        continue
                        
                    links = extract_all_links_from_metadata(metadata_element, METADATA_PREFIX)
                    file_entries = build_file_entries(links)
                    
                    # Apply domain-specific relevance filtering
                    if not record_passes_qda_filter(metadata_element, METADATA_PREFIX, file_entries):
                        continue
                        
                    oai_id = get_oai_identifier(record)
                    title, creators = get_title_and_creators_from_metadata(metadata_element, METADATA_PREFIX)
                    uploader_email = get_uploader_email_from_metadata(metadata_element, METADATA_PREFIX)
                    
                    # Generate a clean, unique project identifier
                    project_id = f"{_slugify(title)}-{_slugify(oai_id)}"[:120]

                    record_meta = {
                        "recid": oai_id,
                        "title": title,
                        "creators_text": creators,
                        "license_id": license_str,
                        "uploader_email": uploader_email,
                        "raw": {},
                    }

                    await queue.put({
                        "context_repository": self.name,
                        "project_identifier": project_id,
                        "record_meta": record_meta,
                        "record_raw": {"oai_id": oai_id},
                        "files": file_entries,
                    })
                    total_enqueued += 1

                if not next_token:
                    break
                resumption_token = next_token

        return total_enqueued

