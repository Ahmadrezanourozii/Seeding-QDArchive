"""Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg

Description: Zero-shot ISIC Rev.5 division classifier for QDA and QD projects using multilingual embeddings.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

PART_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PART_ROOT.parent
DEFAULT_DB = REPO_ROOT / "23726011-seeding.db"
ISIC_CSV = PART_ROOT / "taxonomy" / "ISIC_Rev_5_english_structure.csv"
REPORT_XLSX = PART_ROOT / "reports" / "isic_class_distribution_by_repository.xlsx"
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

QDA_PRIMARY = frozenset({
    "pdf", "docx", "doc", "txt", "qdpx", "qdc", "qpd", "qlt", "ppj", "pprj",
    "mqda", "mqbac", "mqtc", "mqex", "mqmtr", "atlasproj", "zip", "rar",
})
QD_PRIMARY = frozenset({
    "mp4", "wav", "mp3", "csv", "png", "jpg", "jpeg", "svg", "xlsx", "xls",
    "pptx", "rtf", "avif", "ico",
})


def load_divisions(csv_path: Path) -> list[dict[str, str]]:
    """Parse ISIC Rev.5 two-digit division codes with parent section metadata."""
    divisions: list[dict[str, str]] = []
    section_code = ""
    section_title = ""
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            code = (row.get("ISIC Rev 5 Code") or "").strip().strip('"')
            title = (row.get("ISIC Rev 5 Title") or "").strip().strip('"')
            if not code:
                continue
            if len(code) == 1 and code.isalpha():
                section_code, section_title = code, title
                continue
            if re.fullmatch(r"\d{2}", code):
                divisions.append({
                    "code": code,
                    "title": title,
                    "section_code": section_code,
                    "section_title": section_title,
                })
    return divisions


def division_label(division: dict[str, str]) -> str:
    return (
        f"ISIC Revision 5 economic activity, division {division['code']}: "
        f"{division['title']}. Section {division['section_code']} "
        f"{division['section_title']}."
    )


def ensure_schema(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    cursor.execute('PRAGMA table_info("PROJECTS")')
    project_columns = {row[1] for row in cursor.fetchall()}
    for column, ddl in (
        ("class", 'ALTER TABLE "PROJECTS" ADD COLUMN "class" TEXT'),
        ("isic_division_code", 'ALTER TABLE "PROJECTS" ADD COLUMN isic_division_code TEXT'),
        ("isic_section_title", 'ALTER TABLE "PROJECTS" ADD COLUMN isic_section_title TEXT'),
        ("class_tags", 'ALTER TABLE "PROJECTS" ADD COLUMN class_tags TEXT'),
    ):
        if column not in project_columns:
            cursor.execute(ddl)
    cursor.execute('PRAGMA table_info("FILES")')
    if "class" not in {row[1] for row in cursor.fetchall()}:
        cursor.execute('ALTER TABLE "FILES" ADD COLUMN "class" TEXT')


def basename_only(path: str) -> str:
    normalized = (path or "").replace("\\", "/")
    return normalized.rsplit("/", 1)[-1] if normalized else ""


def build_project_text(
    *,
    title: str,
    description: str | None,
    language: str | None,
    doi: str | None,
    query_string: str | None,
    keywords: str | None,
    primary_files: str | None,
    file_types: str | None,
    project_type: str,
) -> str:
    sections = [
        f"Research archive type: {project_type}.",
        f"Title: {title or ''}",
    ]
    if description:
        sections.append(f"Abstract / description: {description[:3500]}")
    if language:
        sections.append(f"Language: {language}")
    if doi and not doi.lower().startswith("http"):
        sections.append(f"DOI: {doi}")
    if query_string:
        sections.append(f"Topic / query context: {query_string[:800]}")
    if keywords:
        sections.append(f"Keywords: {keywords[:2000]}")
    if primary_files:
        sections.append(f"Primary document and data file names: {primary_files[:4000]}")
    if file_types:
        sections.append(f"File format summary: {file_types[:500]}")
    return "\n".join(sections)


def fetch_keywords(conn: sqlite3.Connection, project_ids: list[int]) -> dict[int, str]:
    if not project_ids:
        return {}
    conn.execute("PRAGMA group_concat_max_len = 1000000")
    placeholders = ",".join("?" * len(project_ids))
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT project_id, GROUP_CONCAT(keyword, '; ')
        FROM "KEYWORDS"
        WHERE project_id IN ({placeholders})
        GROUP BY project_id
        """,
        project_ids,
    )
    return {int(row[0]): row[1] or "" for row in cursor.fetchall()}


def fetch_file_summaries(
    conn: sqlite3.Connection,
    project_ids: list[int],
    project_type: str,
) -> dict[int, tuple[str, str]]:
    if not project_ids:
        return {}
    primary = QDA_PRIMARY if project_type == "QDA_PROJECT" else QD_PRIMARY
    conn.execute("PRAGMA group_concat_max_len = 1000000")
    placeholders = ",".join("?" * len(project_ids))
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT project_id, file_name, file_type
        FROM "FILES"
        WHERE project_id IN ({placeholders})
        """,
        project_ids,
    )

    grouped: dict[int, list[tuple[str, str]]] = {pid: [] for pid in project_ids}
    for project_id, file_name, file_type in cursor.fetchall():
        grouped.setdefault(int(project_id), []).append(
            (str(file_name or ""), str(file_type or "").lower())
        )

    summaries: dict[int, tuple[str, str]] = {}
    for project_id, rows in grouped.items():
        primary_names: list[str] = []
        type_counts: dict[str, int] = {}
        for file_name, ext in rows:
            type_counts[ext] = type_counts.get(ext, 0) + 1
            if ext in primary:
                base = basename_only(file_name)
                if base and base not in primary_names:
                    primary_names.append(base)
        names_blob = " | ".join(primary_names[:120])
        types_blob = ", ".join(
            f"{ext}:{count}"
            for ext, count in sorted(type_counts.items(), key=lambda item: -item[1])[:40]
        )
        summaries[project_id] = (names_blob, types_blob)
    return summaries


def write_excel_report(conn: sqlite3.Connection, output_path: Path) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT repository_id, repository_url, type AS project_type,
               "class" AS isic_division_title, COUNT(*) AS n_projects
        FROM "PROJECTS"
        WHERE type IN ('QDA_PROJECT', 'QD_PROJECT') AND "class" IS NOT NULL
        GROUP BY repository_id, repository_url, type, "class"
        ORDER BY repository_id, project_type, n_projects DESC
        """
    )
    frame = pd.DataFrame(
        cursor.fetchall(),
        columns=[
            "repository_id",
            "repository_url",
            "project_type",
            "isic_division_title",
            "n_projects",
        ],
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for project_type, sheet_name in (("QDA_PROJECT", "QDA_counts"), ("QD_PROJECT", "QD_counts")):
            subset = frame[frame["project_type"] == project_type]
            if subset.empty:
                pd.DataFrame({"note": [f"No classified rows for {project_type}"]}).to_excel(
                    writer, sheet_name=sheet_name, index=False
                )
                continue
            pivot = subset.pivot_table(
                index="repository_id",
                columns="isic_division_title",
                values="n_projects",
                aggfunc="sum",
                fill_value=0,
            )
            pivot.to_excel(writer, sheet_name=sheet_name)
            subset.groupby(["repository_id", "repository_url"], as_index=False).agg(
                projects_classified=("n_projects", "sum")
            ).to_excel(writer, sheet_name=f"{sheet_name}_repo_meta", index=False)
        frame.to_excel(writer, sheet_name="long_format_all", index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Assign ISIC Rev.5 classes to QDA/QD projects.")
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(os.environ.get("DATABASE_PATH", DEFAULT_DB)),
    )
    parser.add_argument("--limit", type=int, default=0, help="Classify only the first N projects.")
    parser.add_argument("--batch-size", type=int, default=48)
    args = parser.parse_args()

    if not ISIC_CSV.is_file():
        sys.stderr.write(f"Missing taxonomy file: {ISIC_CSV}\n")
        return 1
    if not args.database.is_file():
        sys.stderr.write(f"Database not found: {args.database}\n")
        return 1

    divisions = load_divisions(ISIC_CSV)
    if not divisions:
        sys.stderr.write("No ISIC divisions parsed from taxonomy CSV.\n")
        return 1

    labels = [division_label(division) for division in divisions]
    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    conn.commit()

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, type, title, description, language, doi, query_string
        FROM "PROJECTS"
        WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')
        ORDER BY id
        """
    )
    projects = [dict(row) for row in cursor.fetchall()]
    if args.limit:
        projects = projects[: args.limit]

    model = SentenceTransformer(MODEL_NAME)
    model.max_seq_length = min(model.max_seq_length, 384)
    label_embeddings = model.encode(
        labels,
        batch_size=32,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    cursor.execute(
        """
        UPDATE "PROJECTS"
        SET "class" = NULL, isic_division_code = NULL,
            isic_section_title = NULL, class_tags = NULL
        WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')
        """
    )
    cursor.execute(
        """
        UPDATE "FILES" SET "class" = NULL
        WHERE project_id IN (
            SELECT id FROM "PROJECTS" WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')
        )
        """
    )
    conn.commit()

    batch_size = args.batch_size
    for offset in range(0, len(projects), batch_size):
        batch = projects[offset : offset + batch_size]
        ids = [int(project["id"]) for project in batch]
        keywords = fetch_keywords(conn, ids)
        summaries: dict[int, tuple[str, str]] = {}
        qda_ids = [int(project["id"]) for project in batch if project["type"] == "QDA_PROJECT"]
        qd_ids = [int(project["id"]) for project in batch if project["type"] == "QD_PROJECT"]
        if qda_ids:
            summaries.update(fetch_file_summaries(conn, qda_ids, "QDA_PROJECT"))
        if qd_ids:
            summaries.update(fetch_file_summaries(conn, qd_ids, "QD_PROJECT"))

        texts = []
        for project in batch:
            project_id = int(project["id"])
            primary_files, file_types = summaries.get(project_id, ("", ""))
            texts.append(
                build_project_text(
                    title=project["title"] or "",
                    description=project.get("description"),
                    language=project.get("language"),
                    doi=project.get("doi"),
                    query_string=project.get("query_string"),
                    keywords=keywords.get(project_id, ""),
                    primary_files=primary_files,
                    file_types=file_types,
                    project_type=project["type"],
                )
            )

        embeddings = model.encode(
            texts,
            batch_size=min(batch_size, 32),
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        similarities = embeddings @ label_embeddings.T
        top_three = np.argsort(-similarities, axis=1)[:, :3]

        for index, project in enumerate(batch):
            best = int(top_three[index, 0])
            division = divisions[best]
            tags = " | ".join(divisions[int(rank)]["title"] for rank in top_three[index])
            cursor.execute(
                """
                UPDATE "PROJECTS"
                SET "class" = ?, isic_division_code = ?, isic_section_title = ?, class_tags = ?
                WHERE id = ?
                """,
                (
                    division["title"],
                    division["code"],
                    f"{division['section_code']} — {division['section_title']}",
                    tags,
                    int(project["id"]),
                ),
            )
        conn.commit()

    cursor.execute(
        """
        UPDATE "FILES"
        SET "class" = (
            SELECT p."class" FROM "PROJECTS" p WHERE p.id = "FILES".project_id
        )
        WHERE project_id IN (
            SELECT id FROM "PROJECTS" WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')
        )
        """
    )
    conn.commit()
    write_excel_report(conn, REPORT_XLSX)
    conn.close()
    sys.stdout.write(f"Classification complete. Report written to {REPORT_XLSX}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
