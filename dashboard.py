#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Real-time CLI dashboard for the QDArchive acquisition pipeline. 
             Provides telemetry on harvested projects, file distributions, and 
             repository-specific health metrics.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from config import get_settings

# Configure minimal logging for the dashboard
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def get_acquisition_stats() -> dict[str, Any] | str:
    """
    Queries the metadata database to generate a snapshot of acquisition telemetry.
    """
    settings = get_settings()
    db_path = Path(settings.db_path)

    if not db_path.exists():
        return f"Database not found at: {db_path.absolute()}"

    try:
        # Use a high timeout to accommodate potential network latency or I/O contention
        with sqlite3.connect(str(db_path), timeout=30) as conn:
            cursor = conn.cursor()

            # 1. High-level aggregates
            cursor.execute("SELECT COUNT(*) FROM FILES WHERE status = 'SUCCEEDED'")
            total_files = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(DISTINCT id) FROM PROJECTS")
            total_projects = cursor.fetchone()[0]

            # 2. Repository-specific distribution
            cursor.execute("""
                SELECT repository_url, COUNT(FILES.id) 
                FROM PROJECTS 
                LEFT JOIN FILES ON PROJECTS.id = FILES.project_id 
                WHERE status = 'SUCCEEDED'
                GROUP BY repository_url
                ORDER BY COUNT(FILES.id) DESC
            """)
            repo_telemetry = cursor.fetchall()

            # 3. File type (software) distribution
            cursor.execute("""
                SELECT file_type FROM FILES WHERE status = 'SUCCEEDED'
            """)
            raw_types = cursor.fetchall()
            type_distribution = {}
            for (f_type,) in raw_types:
                ext_name = f_type.upper() if f_type else 'UNKNOWN'
                type_distribution[ext_name] = type_distribution.get(ext_name, 0) + 1

            # 4. Recent activity log
            cursor.execute("""
                SELECT download_project_folder 
                FROM PROJECTS 
                ORDER BY download_date DESC 
                LIMIT 5
            """)
            recent_activity = [row[0] for row in cursor.fetchall() if row[0]]

        return {
            "total_files": total_files,
            "total_projects": total_projects,
            "repos": repo_telemetry,
            "extensions": type_distribution,
            "recent": recent_activity
        }
    except Exception as e:
        logger.error("Failed to read telemetry: %s", e)
        return str(e)


def render_dashboard(stats: dict[str, Any]) -> None:
    """Renders the acquisition telemetry to the terminal."""
    os.system('cls' if os.name == 'nt' else 'clear')
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("=" * 70)
    print(f" QDARCHIVE PIPELINE TELEMETRY | Sync: {timestamp}")
    print("=" * 70)
    
    print(f" [+] Projects Discovered  : {stats['total_projects']}")
    print(f" [+] Records Synchronized : {stats['total_files']}")
    
    print("\n [ Repository Footprint ]")
    print("-" * 35)
    for repo, count in stats['repos']:
        repo_label = (repo or "internal")[:25]
        print(f"  {repo_label:<25} : {count:5} files")

    print("\n [ Software Ecosystem ]")
    print("-" * 35)
    sorted_exts = sorted(stats['extensions'].items(), key=lambda x: x[1], reverse=True)
    for ext, count in sorted_exts[:10]: # Show top 10
        print(f"  {ext:<25} : {count:5} files")

    print("\n [ Live Acquisition Stream ]")
    print("-" * 35)
    for project in stats['recent']:
        print(f"  -> {project[:60]}...")

    print("\n" + "=" * 70)
    print(" Refreshing every 30s | Press Ctrl+C to terminate.")


def main() -> None:
    """Dashboard entry point and event loop."""
    print("Initializing QDArchive Telemetry Engine...")
    
    try:
        while True:
            stats_bundle = get_acquisition_stats()
            
            if isinstance(stats_bundle, dict):
                render_dashboard(stats_bundle)
            else:
                print(f" \033[91m[!] Telemetry Error:\033[0m {stats_bundle}")
                print(" Re-establishing database handshake in 30s...")
            
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n[!] Dashboard terminated by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()

