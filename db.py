#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Database schema definitions and asynchronous SQLite persistence layer for the QDArchive pipeline.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite

from config import get_db_path

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS PROJECTS (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string TEXT,
    repository_id INTEGER NOT NULL,
    repository_url TEXT NOT NULL,
    project_url TEXT NOT NULL,
    version TEXT DEFAULT '',
    title TEXT NOT NULL,
    description TEXT,
    language TEXT,
    doi TEXT,
    license TEXT,
    upload_date TEXT,
    download_date TEXT NOT NULL,
    download_repository_folder TEXT NOT NULL,
    download_project_folder TEXT NOT NULL,
    download_version_folder TEXT,
    download_method TEXT NOT NULL,
    UNIQUE(project_url, version)
);

CREATE TABLE IF NOT EXISTS FILES (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    file_type TEXT NOT NULL,
    status TEXT NOT NULL,
    failure_reason TEXT,
    FOREIGN KEY(project_id) REFERENCES PROJECTS(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS KEYWORDS (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    keyword TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES PROJECTS(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS PERSON_ROLE (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    FOREIGN KEY(project_id) REFERENCES PROJECTS(id) ON DELETE CASCADE
);
"""


async def init_db(db_path: Path | None = None) -> None:
    """Initializes the database schema and enables foreign key constraints."""
    db_path = db_path or get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(db_path)) as conn:
        await conn.execute("PRAGMA foreign_keys = ON;")
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()


async def _db_writer_loop(
    conn: aiosqlite.Connection,
    queue: asyncio.Queue[dict[str, Any]],
    stop: asyncio.Event,
) -> None:
    """Internal loop for processing database write operations from an asynchronous queue."""
    while not stop.is_set():
        try:
            item = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        if item is None:
            break
        op = item.get("op")
        if op == "insert_project":
            await _insert_project_full(conn, item)
        elif op == "close":
            break
        queue.task_done()


async def _insert_project_full(conn: aiosqlite.Connection, item: dict[str, Any]) -> None:
    """Performs an atomic upsert of a project and its associated metadata."""
    p = item.get("project", {})
    files = item.get("files", [])
    keywords = item.get("keywords", [])
    persons = item.get("persons", [])

    project_url = p.get("project_url")
    version = p.get("version", "") or ""

    cursor = await conn.execute(
        """
        INSERT INTO PROJECTS (
            query_string, repository_id, repository_url, project_url, version,
            title, description, language, doi, license, upload_date, download_date,
            download_repository_folder, download_project_folder, download_version_folder, download_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(project_url, version) DO UPDATE SET
            download_date = excluded.download_date,
            title = excluded.title,
            description = excluded.description,
            license = excluded.license,
            upload_date = excluded.upload_date,
            download_repository_folder = excluded.download_repository_folder,
            download_project_folder = excluded.download_project_folder,
            download_version_folder = excluded.download_version_folder
        RETURNING id
        """,
        (
            p.get("query_string"),
            p.get("repository_id"),
            p.get("repository_url"),
            project_url,
            version,
            p.get("title", "Unknown Title"),
            p.get("description"),
            p.get("language"),
            p.get("doi"),
            p.get("license"),
            p.get("upload_date"),
            p.get("download_date", datetime.now(timezone.utc).isoformat()),
            p.get("download_repository_folder"),
            p.get("download_project_folder"),
            p.get("download_version_folder"),
            p.get("download_method", "SCRAPING | API"),
        ),
    )
    row = await cursor.fetchone()
    if not row:
        logger.error(
            "Failed to retrieve project_id after insert/upsert for %s", project_url
        )
        return

    project_id = row[0]

    # Clean legacy associated records to maintain referential integrity on upsert
    await conn.execute("DELETE FROM FILES WHERE project_id = ?", (project_id,))
    await conn.execute("DELETE FROM KEYWORDS WHERE project_id = ?", (project_id,))
    await conn.execute("DELETE FROM PERSON_ROLE WHERE project_id = ?", (project_id,))

    for f in files:
        await conn.execute(
            """
            INSERT INTO FILES (project_id, file_name, file_type, status, failure_reason)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                project_id,
                f.get("file_name"),
                f.get("file_type"),
                f.get("status"),
                f.get("failure_reason"),
            ),
        )

    for k in keywords:
        if isinstance(k, str):
            await conn.execute(
                "INSERT INTO KEYWORDS (project_id, keyword) VALUES (?, ?)",
                (project_id, k.strip()),
            )

    for p_role in persons:
        await conn.execute(
            "INSERT INTO PERSON_ROLE (project_id, name, role) VALUES (?, ?, ?)",
            (project_id, p_role.get("name"), p_role.get("role", "UNKNOWN")),
        )

    await conn.commit()


async def run_db_writer(
    queue: asyncio.Queue[dict[str, Any]],
    db_path: Path | None = None,
) -> asyncio.Task[None]:
    """Initializes and returns a task for the background database writer."""
    db_path = db_path or get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    stop = asyncio.Event()

    async def _run() -> None:
        async with aiosqlite.connect(str(db_path)) as conn:
            await conn.execute("PRAGMA foreign_keys = ON;")
            await conn.executescript(SCHEMA_SQL)
            await conn.commit()
            await _db_writer_loop(conn, queue, stop)

    return asyncio.create_task(_run())


async def get_existing_project_urls(db_path: Path | None = None) -> set[str]:
    """Retrieves a set of all project URLs currently stored in the database."""
    db_path = db_path or get_db_path()
    if not db_path.exists():
        return set()
    async with aiosqlite.connect(str(db_path)) as conn:
        async with conn.execute("SELECT project_url FROM PROJECTS") as cur:
            rows = await cur.fetchall()
    return {r[0] for r in rows if r[0]}

