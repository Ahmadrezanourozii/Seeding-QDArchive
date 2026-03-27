#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Abstract base harvester interface; defines the standard contract and shared scoring logic for all repository harvesters.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import aiohttp


class AbstractBaseHarvester(ABC):
    """
    Base class for all repository harvesters. Orchestrates metadata acquisition and
    enqueues record tasks for the asynchronous downloader.
    """

    DEFAULT_REQUEST_INTERVAL = 1.0
    CONNECTOR_LIMIT = 100
    DNS_TTL_SECONDS = 300

    def __init__(self, request_interval: float | None = None) -> None:
        self.request_interval = (
            request_interval
            if request_interval is not None
            else self.DEFAULT_REQUEST_INTERVAL
        )

    def create_client_session(
        self, headers: dict[str, str] | None = None
    ) -> aiohttp.ClientSession:
        """
        Initializes a high-performance aiohttp session with shared connector settings.
        Dedicated connectors prevent harvester sessions from being bottlenecked by downloader traffic.
        """
        connector = aiohttp.TCPConnector(
            limit=self.CONNECTOR_LIMIT,
            ttl_dns_cache=self.DNS_TTL_SECONDS,
        )
        timeout = aiohttp.ClientTimeout(total=30)

        default_headers = {
            "User-Agent": "QDArchive Academic Research Bot - Data Collection / aiohttp",
            "Accept": "application/json",
            "Connection": "keep-alive",
        }

        if headers:
            default_headers.update(headers)

        return aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers=default_headers
        )

    # Weighted Scoring System for CAQDAS (Qualitative Data Analysis Software) relevance.
    # Score threshold of 10 is required for record validation.
    QDA_WEIGHTS = {
        # TIER 1: HIGH RELEVANCE (10 Points) - Explicit software or standards
        "maxqda": 10, "nvivo": 10, "atlas.ti": 10, "quirkos": 10, "dedoose": 10,
        "transana": 10, "f4analyse": 10, "qualcoder": 10, "qda miner": 10,
        "leipzig corpus miner": 10, "caqdas": 10, "qdas": 10,
        "computer assisted qualitative data analysis": 10, "qdpx": 10, "mx18": 10,
        "mx20": 10, "mx22": 10, "mx24": 10, "mex22": 10, "mex24": 10, "nvp": 10,
        "nvpx": 10, "qdc": 10, "xml project exchange file": 10, "refi-qda": 10,
        "maxmaps": 10, "smart coding tool": 10, "code matrix browser": 10,
        "interactive quote matrix": 10, "maxdictio": 10, "code relations browser": 10,
        "document portrait": 10,
        # TIER 2: MEDIUM RELEVANCE (5 Points) - Specific methodologies and sampling
        "semi-structured interview": 5, "in-depth interview": 5,
        "face-to-face in-depth interview": 5, "focus group transcript": 5,
        "interview transcript": 5, "interview schedule": 5, "topic guide": 5,
        "codebook thematic analysis": 5, "reflexive thematic analysis": 5,
        "qualitative case study": 5, "prolonged engagement": 5,
        "persistent observation": 5, "member checking": 5, "respondent validation": 5,
        "action research": 5, "trustworthiness in qualitative": 5,
        "rich thick description": 5, "theoretical sampling": 5,
        "purposive sampling": 5, "snowball sampling": 5, "constant comparison": 5,
        "interpretative phenomenological analysis": 5, "grounded theory": 5,
        "mixed methods": 5, "open coding": 5, "axial coding": 5, "selective coding": 5,
        "ethnographic research": 5, "cognitive interviewing": 5, "inductive coding": 5,
        "narrative analysis": 5, "inductive thematic saturation": 5,
        # TIER 3: GENERAL RELEVANCE (3 Points) - General qualitative terminology
        "thematic analysis": 3, "focus group": 3, "qualitative data": 3,
        "qualitative research": 3, "transcript": 3, "open-ended": 3, "codebook": 3,
        "memos": 3, "field notes": 3, "lived experience": 3, "creative coding": 3,
        "peer debriefing": 3, "intercoder agreement": 3, "case variables": 3,
        "observation data": 3, "content analysis": 3, "summaries": 3,
        "paraphrases": 3, "audit trail": 3, "coded segments": 3, "latent codes": 3,
        "semantic codes": 3, "code frequencies": 3,
    }

    def is_qda_match(self, text: str) -> bool:
        """
        Validates record relevance using a weighted keyword scoring system.
        Returns True if the cumulative score meets the threshold of 10.
        """
        if not text:
            return False

        text_lower = text.lower()
        score: int = 0

        for term, weight in self.QDA_WEIGHTS.items():
            if term in text_lower:
                score += weight
                if score >= 10:
                    return True

        return score >= 10

    @property
    @abstractmethod
    def name(self) -> str:
        """Identifier for the harvester (e.g., 'zenodo', 'dryad')."""
        ...

    @abstractmethod
    async def fetch_records(
        self,
        queue: asyncio.Queue[dict[str, Any]],
        *,
        max_records: int | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> int:
        """
        Retrieves metadata from the target API and enqueues download tasks.
        Returns the number of validated records enqueued.
        """
        ...

    async def close(self) -> None:
        """Performs cleanup of asynchronous resources and connections."""
        pass

