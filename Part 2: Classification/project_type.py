"""Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg

Description: Extension-based project-type taxonomy and noisy file_type repair for QDArchive metadata.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

QDA_EXTENSIONS = frozenset({
    "mqda", "mqbac", "mqtc", "mqex", "mqmtr", "mx24", "mx24bac", "mc24", "mex24",
    "mx22", "mx20", "mx18", "mx12", "mx11", "mx5", "mx4", "mx3", "mx2", "m2k",
    "loa", "sea", "mtr", "mod", "mex22", "nvp", "nvpx", "atlasproj", "hpr7",
    "ppj", "pprj", "qlt", "qdpx", "qdc", "qpd", "pdf", "docx", "doc", "txt",
    "zip", "rar",
})

QD_EXTENSIONS = frozenset({
    "rtf", "mp4", "wav", "mp3", "jpg", "jpeg", "png", "svg", "ico", "avif",
    "xlsx", "xls", "pptx", "ps", "csv",
})

OTHER_EXTENSIONS = frozenset({
    "md", "rmd", "html", "py", "js", "ts", "vue", "json", "sql", "sh", "yml",
    "ipynb", "php", "xml", "css", "m", "r", "dat", "db", "ini", "conf", "sav",
    "dta", "sps", "do", "por", "tab", "stpr", "dct", "shp", "shx", "dbf", "prj",
    "sbn", "sbx",
})

NOT_A_PROJECT_EXTENSIONS = frozenset({
    "rdata", "rproj", "fasta", "phy", "newick", "tree",
})

TYPE_RANK = {
    "QDA_PROJECT": 1,
    "QD_PROJECT": 2,
    "OTHER_PROJECT": 3,
    "NOT_A_PROJECT": 4,
}

_ALLOWED_FILES = frozenset({"FILES", "files"})
_ALLOWED_PROJECTS = frozenset({"PROJECTS", "projects"})


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def default_database_path() -> Path:
    return repo_root() / "23726011-seeding.db"


def normalize_token(value: str) -> str:
    return (value or "").strip().lower().lstrip(".")


def basename(path: str) -> str:
    normalized = (path or "").replace("\\", "/")
    return normalized.rsplit("/", 1)[-1] if normalized else ""


def extension_from_filename(file_name: str) -> str:
    base = basename(file_name)
    if "." not in base:
        return ""
    return normalize_token(base.rsplit(".", 1)[-1])


def is_plausible_extension_token(file_type: str, token: str) -> bool:
    raw = (file_type or "").strip()
    if not raw or any(ch in raw for ch in " /\t\n\\"):
        return False
    if not 1 <= len(token) <= 40:
        return False
    return all(ch.isalnum() or ch in "._-" for ch in token)


def effective_extension(file_name: str, file_type: str) -> str:
    token = normalize_token(file_type)
    if token and is_plausible_extension_token(file_type, token):
        return token
    return extension_from_filename(file_name)


def infer_extension_when_missing(file_name: str) -> str:
    """Resolve extension for repository-specific placeholder rows without file_type."""
    path = file_name or ""
    base = basename(path)
    lower = path.lower()

    if base == "dataset" or lower.endswith("/dataset"):
        return "dat"
    if lower.startswith("https:/") or lower.startswith("http:/"):
        return "html"
    if base in {"Dockerfile", "Pipfile", "artisan", "LICENSE", "LICENSE_MEDIA"}:
        return {"Dockerfile": "dockerfile", "Pipfile": "pipfile", "artisan": "php",
                "LICENSE": "md", "LICENSE_MEDIA": "md"}[base]
    if "start-container" in lower:
        return "sh"
    if "." in base:
        return extension_from_filename(path)
    return ""


def cleaned_file_type(file_name: str, file_type: str) -> str | None:
    """Return corrected file_type when repair is required, otherwise None."""
    path = file_name or ""
    raw = (file_type or "").strip()

    if normalize_token(raw) == "url" and path.lower().endswith(".txt"):
        return "txt"
    if not raw:
        inferred = infer_extension_when_missing(path)
        return inferred or None
    if any(ch in raw for ch in " \t\n"):
        return extension_from_filename(path) or infer_extension_when_missing(path) or None

    token = normalize_token(raw)
    if not is_plausible_extension_token(raw, token):
        return extension_from_filename(path) or infer_extension_when_missing(path) or None
    return None


def category_for_extension(ext: str) -> str:
    if not ext:
        return "OTHER_PROJECT"
    token = normalize_token(ext)
    if token in QDA_EXTENSIONS:
        return "QDA_PROJECT"
    if token in QD_EXTENSIONS:
        return "QD_PROJECT"
    if token in OTHER_EXTENSIONS:
        return "OTHER_PROJECT"
    if token in NOT_A_PROJECT_EXTENSIONS:
        return "NOT_A_PROJECT"
    return "OTHER_PROJECT"


def project_type_from_extensions(extensions: list[str]) -> str:
    best_rank = TYPE_RANK["NOT_A_PROJECT"]
    for ext in extensions:
        best_rank = min(best_rank, TYPE_RANK[category_for_extension(ext)])
    for label, rank in TYPE_RANK.items():
        if rank == best_rank:
            return label
    return "OTHER_PROJECT"


def _quote_identifier(name: str) -> str:
    if name not in _ALLOWED_FILES | _ALLOWED_PROJECTS:
        raise ValueError(f"Unsupported SQL identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def ensure_type_column(cursor: sqlite3.Cursor, projects_table: str = "PROJECTS") -> None:
    table = _quote_identifier(projects_table)
    cursor.execute(f"PRAGMA table_info({table})")
    columns = {row[1] for row in cursor.fetchall()}
    if "type" in columns:
        return
    cursor.execute(
        f"""
        ALTER TABLE {table} ADD COLUMN type TEXT NOT NULL DEFAULT 'OTHER_PROJECT'
        CHECK (type IN ('QDA_PROJECT', 'QD_PROJECT', 'OTHER_PROJECT', 'NOT_A_PROJECT'))
        """
    )


def repair_files(conn: sqlite3.Connection, files_table: str = "FILES") -> int:
    table = _quote_identifier(files_table)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT id, file_name, file_type FROM {table}")
    updates: list[tuple[str, int]] = []
    for row in cursor.fetchall():
        current = normalize_token(row["file_type"] or "")
        repaired = cleaned_file_type(str(row["file_name"] or ""), str(row["file_type"] or ""))
        if repaired is not None and current != repaired:
            updates.append((repaired, int(row["id"])))
    if updates:
        cursor.executemany(f"UPDATE {table} SET file_type = ? WHERE id = ?", updates)
    return len(updates)


def assign_project_types(
    conn: sqlite3.Connection,
    *,
    files_table: str = "FILES",
    projects_table: str = "PROJECTS",
) -> None:
    files = _quote_identifier(files_table)
    projects = _quote_identifier(projects_table)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    extensions_by_project: dict[int, list[str]] = {}
    cursor.execute(f"SELECT project_id, file_name, file_type FROM {files}")
    for row in cursor.fetchall():
        project_id = int(row["project_id"])
        ext = effective_extension(str(row["file_name"] or ""), str(row["file_type"] or ""))
        if not ext:
            ext = infer_extension_when_missing(str(row["file_name"] or ""))
        extensions_by_project.setdefault(project_id, []).append(ext)

    cursor.execute(f"SELECT id FROM {projects}")
    updates = []
    for (project_id,) in cursor.fetchall():
        extensions = extensions_by_project.get(int(project_id), [])
        label = project_type_from_extensions(extensions) if extensions else "OTHER_PROJECT"
        updates.append((label, int(project_id)))
    cursor.executemany(f"UPDATE {projects} SET type = ? WHERE id = ?", updates)
