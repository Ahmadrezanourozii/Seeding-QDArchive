"""Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg

Description: CLI entry point to repair file extensions and assign PROJECTS.type labels.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

from project_type import assign_project_types, default_database_path, ensure_type_column, repair_files


def main() -> int:
    parser = argparse.ArgumentParser(description="Assign QDArchive project types from file extensions.")
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(os.environ.get("DATABASE_PATH", default_database_path())),
        help="Path to the merged SQLite database.",
    )
    args = parser.parse_args()

    if not args.database.is_file():
        sys.stderr.write(f"Database not found: {args.database}\n")
        return 1

    conn = sqlite3.connect(args.database)
    cursor = conn.cursor()
    ensure_type_column(cursor)
    repaired = repair_files(conn)
    assign_project_types(conn)
    conn.commit()

    cursor.execute(
        'SELECT type, COUNT(*) FROM "PROJECTS" GROUP BY type ORDER BY COUNT(*) DESC'
    )
    summary = cursor.fetchall()
    conn.close()

    sys.stdout.write(f"Repaired file_type rows: {repaired}\n")
    for label, count in summary:
        sys.stdout.write(f"  {label}: {count}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
