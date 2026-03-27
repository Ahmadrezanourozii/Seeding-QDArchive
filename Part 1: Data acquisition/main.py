#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Entry point for the QDArchive data acquisition pipeline; orchestrates harvesters and downloader.
"""
from __future__ import annotations

import argparse
import asyncio
import logging

from config import get_base_path, get_settings
from db import init_db, run_db_writer
from downloader import run_downloader
from harvester_base import AbstractBaseHarvester
from harvesters.zenodo_harvester import ZenodoHarvester
from harvesters.dryad_harvester import DryadHarvester
from harvesters.generic_dataverse_harvester import GenericDataverseHarvester
from harvesters.icpsr_harvester import ICPSRHarvester
from harvesters.odum_harvester import OdumHarvester
from harvesters.ihsn_harvester import IHSNHarvester
from harvesters.sada_harvester import SadaHarvester
from harvesters.cessda_harvester import CessdaHarvester

from constants import STRICT_TERMS, BROAD_TERMS

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
DEFAULT_DOWNLOADER_WORKERS = 50

# ── Query Definitions ──────────────────────────────────────────────────────────
# FULL_QUERIES contains the complete set of keywords for precise search APIs.
FULL_QUERIES = [q.strip('"') for q in list(set(STRICT_TERMS + BROAD_TERMS))]

# STRICT_QUERIES restricts searches to multi-word phrases to minimize noise.
STRICT_QUERIES = [q for q in FULL_QUERIES if len(q.split()) >= 3]

# Dataverse-family repositories requiring consistent base URL configuration.
DATAVERSE_SOURCES = [
    ("qdr", "https://data.qdr.syr.edu"),
    ("dans_ssh", "https://ssh.datastations.nl"),
    ("dans_life", "https://lifesciences.datastations.nl"),
    ("dans_archaeology", "https://archaeology.datastations.nl"),
    ("dans_physical", "https://phys-techsciences.datastations.nl"),
    ("dataversenl", "https://dataverse.nl"),
    ("dataverseno", "https://dataverse.no"),
    ("murray", "https://dataverse.harvard.edu"),
    ("aussda", "https://data.aussda.at"),
]

DATAVERSE_SOURCE_IDS = frozenset(s[0] for s in DATAVERSE_SOURCES)
EXTRA_SOURCE_IDS = ["odum", "icpsr"]

ALL_SOURCE_IDS = (
    ["zenodo", "dryad"] + [s[0] for s in DATAVERSE_SOURCES] + EXTRA_SOURCE_IDS
)


def _query_for(source_id: str, user_query: str | None) -> str | None:
    """Selects the appropriate search query based on the repository's API behavior."""
    if user_query:
        return user_query
    if source_id == "zenodo":
        return " OR ".join(STRICT_QUERIES)
    
    # Harvesters that implement internal iteration or specific search logic.
    if source_id in ["cessda", "sada", "ihsn"]:
        return None

    return " OR ".join(FULL_QUERIES)


def build_harvesters(sources: list[str], settings) -> list[AbstractBaseHarvester]:
    """Instantiates harvester objects for the specified repository identifiers."""
    dataverse_index = {src_id: url for src_id, url in DATAVERSE_SOURCES}
    harvesters: list[AbstractBaseHarvester] = []

    for src in sources:
        src_lower = src.lower().strip()
        if src_lower == "zenodo":
            harvesters.append(ZenodoHarvester(settings))
        elif src_lower == "dryad":
            harvesters.append(DryadHarvester(settings))
        elif src_lower == "odum":
            harvesters.append(OdumHarvester())
        elif src_lower == "icpsr":
            harvesters.append(ICPSRHarvester())
        elif src_lower == "ihsn":
            harvesters.append(IHSNHarvester(settings))
        elif src_lower == "sada":
            harvesters.append(SadaHarvester())
        elif src_lower == "cessda":
            harvesters.append(CessdaHarvester())
        elif src_lower in dataverse_index:
            base_url = dataverse_index[src_lower]
            token = settings.api.get(src_lower)
            subtree = "mra" if src_lower == "murray" else None
            harvesters.append(
                GenericDataverseHarvester(base_url, token, src_lower, subtree=subtree)
            )
        else:
            logger.warning("Unknown source '%s' — skipping.", src)
    return harvesters


async def run_pipeline(
    harvesters: list[AbstractBaseHarvester],
    *,
    max_records: int | None = None,
    args_query: str | None = None,
    max_workers: int = DEFAULT_DOWNLOADER_WORKERS,
    strict: bool = False,
) -> None:
    """Orchestrates the asynchronous discovery and download sub-pipelines."""
    if not harvesters:
        logger.warning("No harvesters selected.")
        return
        
    get_base_path()
    await init_db()

    task_queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
    db_queue: asyncio.Queue = asyncio.Queue()
    settings = get_settings()

    db_writer_task = await run_db_writer(db_queue)
    downloader_task = asyncio.create_task(
        run_downloader(
            task_queue,
            db_queue,
            token=settings.zenodo_access_token,
            max_workers=max_workers,
            strict=strict,
        )
    )

    async def run_one(h: AbstractBaseHarvester) -> None:
        per_harvester_query = _query_for(h.name, args_query)
        try:
            n = await h.fetch_records(
                task_queue,
                max_records=max_records,
                query=per_harvester_query,
                strict=strict,
            )
            logger.info("Harvester (%s) enqueued %d record(s).", h.name, n)
        except BaseException as exc:
            err_msg = str(exc).strip() or repr(exc)
            logger.warning(
                "Harvester (%s) failed with %s: %s",
                h.name,
                type(exc).__name__,
                err_msg,
                exc_info=True,
            )

    harvester_tasks = [asyncio.create_task(run_one(h), name=h.name) for h in harvesters]
    try:
        await asyncio.gather(*harvester_tasks, return_exceptions=True)
        
        for _ in range(max_workers):
            await task_queue.put(None)
        
        logger.info("Discovery complete; draining download queue.")
        await task_queue.join()
        await downloader_task
    except asyncio.CancelledError:
        logger.warning("Pipeline cancelled.")
    finally:
        for h in harvesters:
            try:
                await h.close()
            except Exception as e:
                logger.error("Error closing harvester %s: %s", h.name, e)

        db_queue.put_nowait({"op": "close"})
        try:
            await asyncio.wait_for(asyncio.shield(db_writer_task), timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            db_writer_task.cancel()
            try:
                await db_writer_task
            except asyncio.CancelledError:
                pass
    logger.info("Pipeline stopped.")


def main() -> None:
    """Parses command-line arguments and initializes the acquisition process."""
    parser = argparse.ArgumentParser(
        description="Seeding QDArchive — Multi-Source Metadata & Data Discovery Pipeline"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        metavar="N",
        help="Max records to harvest per source.",
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help="Custom search query (overrides defaults).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Only keep projects containing QDA files.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all integrated harvesters in parallel.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_DOWNLOADER_WORKERS,
        metavar="N",
        help="Number of parallel download workers (default: %(default)s).",
    )
    parser.add_argument(
        "--sources",
        type=str,
        default=None,
        metavar="LIST",
        help="Comma-separated list of sources (e.g. zenodo,dryad).",
    )
    args = parser.parse_args()

    settings = get_settings()
    if args.all:
        sources = ALL_SOURCE_IDS
    elif args.sources:
        sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    else:
        sources = ["zenodo"]

    harvesters = build_harvesters(sources, settings)
    if not harvesters:
        logger.error("No valid harvesters identified.")
        return

    # default to broad strict query for Zenodo-level noise reduction
    query = args.query or " OR ".join(STRICT_QUERIES)
    
    display_query = str(query)
    if len(display_query) > 60:
        display_query = display_query[:60] + "..."
        
    logger.info(
        "Starting acquisition for: %s (Query: %s)",
        [h.name for h in harvesters],
        display_query,
    )
    
    asyncio.run(
        run_pipeline(
            harvesters,
            max_records=args.max_records,
            args_query=args.query,
            max_workers=args.workers,
            strict=args.strict,
        )
    )


if __name__ == "__main__":
    main()
