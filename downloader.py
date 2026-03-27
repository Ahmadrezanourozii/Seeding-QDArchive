#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Asynchronous downloader for the QDArchive pipeline; manages file acquisition, archive extraction, and metadata persistence.
"""
from __future__ import annotations

import asyncio
import logging
import shutil
import tarfile
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp

from config import get_base_path, get_settings
from constants import ARCHIVE_EXTENSIONS, QDA_EXTENSIONS, file_ext

# Identifiers for Dataverse-based repositories requiring specialized API headers.
DATAVERSE_REPOS = frozenset({
    "qdr", "dans_ssh", "dans_life", "dans_archaeology",
    "dans_physical", "dataversenl", "dataverseno",
})

# Mapping of source names to internal repository identifiers.
REPOSITORY_ID_MAP = {
    "zenodo": 1, "dryad": 2, "uk-data-service": 3, "qdr": 4,
    "dans_ssh": 5, "dans_life": 5, "dans_archaeology": 5, "dans_physical": 5,
    "dataversenl": 6, "dataverseno": 6, "ada": 7, "sada": 8, "ihsn": 9,
    "aussda": 12, "cessda": 13, "icpsr": 15, "murray": 18,
}

AUTHOR_ROLES = ("UPLOADER", "AUTHOR", "OWNER", "OTHER")
STREAM_CHUNK_SIZE = 1024 * 1024  # 1 MiB
logger = logging.getLogger(__name__)


def _safe_filename(name: str) -> str:
    """Sanitizes a filename by removing directory traversal patterns and whitespace."""
    name = name.replace("..", "").strip()
    return name or "unnamed_file"


async def _extract_archive(archive_path: Path, extract_to: Path) -> bool:
    """Extracts a compressed archive and flattens its root directory if applicable."""
    try:
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path, "r") as z:
                z.extractall(path=extract_to)
        elif tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as t:
                t.extractall(path=extract_to)
        else:
            return False

        # Flatten logic: if the archive contains exactly one folder, move its contents up.
        children = list(extract_to.iterdir())
        if len(children) == 1 and children[0].is_dir():
            root_child = children[0]
            for grand_child in root_child.iterdir():
                shutil.move(str(grand_child), str(extract_to / grand_child.name))
            root_child.rmdir()
        return True
    except Exception as e:
        logger.warning("Extraction failed for %s: %s", archive_path.name, e)
    return False


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    *,
    token: str | None = None,
    use_dataverse_key: bool = False,
) -> int:
    """Downloads a single file via HTTP GET and persists it to the localized destination."""
    headers = {}
    if token:
        if use_dataverse_key:
            headers["X-Dataverse-key"] = token
        else:
            headers["Authorization"] = f"Bearer {token}"
    try:
        bytes_written = 0
        async with session.get(url, headers=headers or None) as resp:
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(dest, "wb") as f:
                async for chunk in resp.content.iter_chunked(STREAM_CHUNK_SIZE):
                    f.write(chunk)
                    bytes_written += len(chunk)
        return bytes_written
    except Exception as e:
        logger.debug("Download failed for %s: %s", url, e)
        return 0


def _get_auth_context(repo_name: str, default_token: str | None) -> tuple[str | None, bool]:
    """Determines the authentication token and header type for a given repository."""
    settings = get_settings()
    token = settings.api.get(repo_name) or default_token
    is_dataverse = repo_name.lower() in DATAVERSE_REPOS
    return token, is_dataverse


async def process_record_task(
    session: aiohttp.ClientSession,
    task: dict[str, Any],
    base_dir: Path,
    db_queue: asyncio.Queue[dict[str, Any]],
    token: str | None = None,
    strict: bool = False,
) -> tuple[int, bool]:
    """Orchestrates the acquisition of a single record, including downloads and DB queuing."""
    context_repo = task.get("context_repository", "zenodo")
    proj_id = str(task.get("project_identifier", "unknown"))
    repo_token, use_dv_key = _get_auth_context(context_repo, token)

    record_meta = task.get("record_meta", {})
    files = task.get("files", [])
    version = str(record_meta.get("version", "v1"))

    # Resolved data hierarchy: /base_dir/repo/project/version/
    project_dir = base_dir / context_repo / proj_id / version

    total_bytes = 0
    downloaded_any = False
    
    # Partition files based on accessibility
    active_files = [f for f in files if not f.get("restricted", False)]
    prefailed_files = [f for f in files if f.get("restricted", False)]

    with tempfile.TemporaryDirectory() as tmp_dir:
        temp_path = Path(tmp_dir)
        try:
            for entry in active_files:
                f_name = _safe_filename(entry.get("file_key", ""))
                f_url = entry.get("file_url", "")
                if not f_name or not f_url:
                    continue

                dest = temp_path / f_name
                size = await download_file(
                    session, f_url, dest, token=repo_token, use_dataverse_key=use_dv_key
                )
                if size <= 0:
                    continue

                total_bytes += size
                if file_ext(f_name) in ARCHIVE_EXTENSIONS:
                    if await _extract_archive(dest, temp_path):
                        try:
                            dest.unlink()
                        except OSError:
                            pass

            all_files = [p for p in temp_path.rglob("*") if p.is_file()]
            qda_files = [p for p in all_files if file_ext(p.name) in QDA_EXTENSIONS]

            # Strict mode filter: discard projects without explicit QDA extensions
            if strict and not qda_files:
                logger.info("Record %s: Filtering out (no QDA files).", proj_id)
                return total_bytes, False

            valid_files = qda_files if strict else all_files
            if valid_files:
                project_dir.mkdir(parents=True, exist_ok=True)
                for f_path in valid_files:
                    rel_path = f_path.relative_to(temp_path)
                    dest_path = project_dir / rel_path
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(f_path), str(dest_path))
                downloaded_any = True

            # Prepare relational metadata for database queuing
            if valid_files or prefailed_files:
                repo_db_id = REPOSITORY_ID_MAP.get(context_repo.lower(), 99)
                project_row = {
                    "query_string": task.get("query_string", ""),
                    "repository_id": repo_db_id,
                    "repository_url": record_meta.get("repository_url", f"https://{context_repo}.org"),
                    "project_url": record_meta.get("project_url", f"https://{context_repo}.org/record/{proj_id}"),
                    "version": version,
                    "title": record_meta.get("title", "Unknown Title"),
                    "description": record_meta.get("description", ""),
                    "language": record_meta.get("language", ""),
                    "doi": record_meta.get("doi", ""),
                    "license": str(record_meta.get("license", "")),
                    "upload_date": record_meta.get("upload_date", ""),
                    "download_date": datetime.now(timezone.utc).isoformat(),
                    "download_repository_folder": context_repo,
                    "download_project_folder": proj_id,
                    "download_version_folder": version,
                    "download_method": "SCRAPING | API",
                }

                db_files = []
                for p in valid_files:
                    db_files.append({
                        "file_name": str(p.relative_to(temp_path)),
                        "file_type": file_ext(p.name).lstrip("."),
                        "status": "SUCCEEDED",
                        "failure_reason": ""
                    })

                for f in prefailed_files:
                    fname = f.get("file_key", "unknown")
                    db_files.append({
                        "file_name": fname,
                        "file_type": file_ext(fname).lstrip("."),
                        "status": "FAILED",
                        "failure_reason": f.get("failure_reason", "Restricted access")
                    })

                # Deduplicate and flatten keywords
                raw_kws = record_meta.get("keywords", [])
                if isinstance(raw_kws, str):
                    raw_kws = [raw_kws]
                kws = list({k.strip() for chunk in raw_kws if isinstance(chunk, str) for k in chunk.split(",") if k.strip()})

                db_persons = []
                for creator in record_meta.get("creators", []):
                    name = creator.get("name", "")
                    role = creator.get("role", "AUTHOR").upper()
                    db_persons.append({"name": name, "role": role if role in AUTHOR_ROLES else "OTHER"})

                db_queue.put_nowait({
                    "op": "insert_project",
                    "project": project_row,
                    "files": db_files,
                    "keywords": kws,
                    "persons": [p for p in db_persons if p["name"]]
                })

        except Exception as e:
            logger.error("Processing failed for %s: %s", proj_id, e, exc_info=True)

    return total_bytes, downloaded_any


async def run_downloader(
    task_queue: asyncio.Queue[dict[str, Any]],
    db_queue: asyncio.Queue[dict[str, Any]],
    token: str | None = None,
    base_dir: Path | None = None,
    max_workers: int = 5,
    strict: bool = False,
) -> None:
    """Drains the task queue and manages the pool of asynchronous download workers."""
    base_dir = base_dir or get_base_path()
    base_dir.mkdir(parents=True, exist_ok=True)
    start_time = asyncio.get_running_loop().time()
    
    total_bytes = 0
    total_projects = 0
    metrics_lock = asyncio.Lock()

    async def _worker(worker_id: int, session: aiohttp.ClientSession) -> None:
        nonlocal total_bytes, total_projects
        while True:
            try:
                task = await asyncio.wait_for(task_queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                continue
            if task is None:
                task_queue.task_done()
                break
            try:
                bytes_in, success = await process_record_task(
                    session, task, base_dir, db_queue, token=token, strict=strict
                )
                async with metrics_lock:
                    total_bytes += bytes_in
                    if success:
                        total_projects += 1
            except Exception:
                logger.exception("Worker-%d: Unexpected error", worker_id)
            finally:
                task_queue.task_done()

    async with aiohttp.ClientSession() as session:
        workers = [asyncio.create_task(_worker(i + 1, session)) for i in range(max_workers)]
        await asyncio.gather(*workers)

    elapsed = max(asyncio.get_running_loop().time() - start_time, 1e-6)
    logger.info(
        "Downloader finished: %.2f MB | Projects: %d | Time: %.2fs", 
        total_bytes / (1024 * 1024), total_projects, elapsed
    )

