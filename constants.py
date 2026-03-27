#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Central repository for QDA file extensions, search terms, and license classification rules.
"""
from __future__ import annotations

import re
from typing import FrozenSet

# ── Extension Definitions ──────────────────────────────────────────────────────
# TARGET_EXTENSIONS: Exhaustive list of QDA project formats (case-insensitive).
_TARGET_RAW = (
    ".mqda", ".mqbac", ".mqtc", ".mqex", ".mqmtr", ".mx24", ".mx24bac",
    ".mc24", ".mex24", ".mx22", ".mx20", ".mx18", ".mx12", ".mx11",
    ".mx5", ".mx4", ".mx3", ".mx2", ".m2k", ".loa", ".sea", ".mtr",
    ".mod", ".mex22", ".nvp", ".nvpx", ".atlasproj", ".hpr7", ".ppj",
    ".pprj", ".qlt", ".qdpx", ".qdc", ".qpd", ".qpd",
)
TARGET_EXTENSIONS: FrozenSet[str] = frozenset(e.lower() for e in _TARGET_RAW)

# Primary interchange formats (REFI-QDA)
PRIMARY_INTERCHANGE: FrozenSet[str] = frozenset({".qdpx", ".qdc"})

# MAXQDA project and backup formats
MAXQDA_SUITE: FrozenSet[str] = frozenset({
    ".mx24", ".mx22", ".mx20", ".mx18", ".mx12", ".mx11", ".mx5", ".mx4",
    ".mx3", ".mx2", ".m2k", ".mx24bac", ".mqbac", ".mqex", ".mex24",
    ".mex22", ".mc24", ".mqtc", ".mqdam", ".mqmtr", ".mtr", ".mod", ".mqda",
})

# NVivo project formats
NVIVO: FrozenSet[str] = frozenset({".nvpx", ".nvp"})

# Quirkos project formats
QUIRKOS: FrozenSet[str] = frozenset({".qpd"})

# ATLAS.ti and miscellaneous QDA formats
ATLAS_OTHERS: FrozenSet[str] = frozenset({
    ".ppj", ".pprj", ".qlt", ".atlasproj", ".hpr7"
})

# Analysis-related metadata formats
ANALYSIS_METADATA: FrozenSet[str] = frozenset({".loa", ".sea"})

# Common archive formats that may encapsulate QDA projects
ARCHIVE_EXTENSIONS: FrozenSet[str] = frozenset({
    ".zip", ".rar", ".7z", ".tar", ".tar.gz", ".tgz"
})

# Consolidated set of all QDA-specific extensions
QDA_EXTENSIONS: FrozenSet[str] = (
    PRIMARY_INTERCHANGE
    | MAXQDA_SUITE
    | NVIVO
    | QUIRKOS
    | ATLAS_OTHERS
    | ANALYSIS_METADATA
)

# Extensions that trigger mandatory record acceptance (Tier 1)
REQUIRED_QDA_EXTENSIONS: FrozenSet[str] = (
    PRIMARY_INTERCHANGE | MAXQDA_SUITE | NVIVO | QUIRKOS
)

# ── Search Terminology ──────────────────────────────────────────────────────────
# STRICT_TERMS: Highly specific keywords used to filter general repositories.
STRICT_TERMS = [
    "qdpx", "qdc", "nvp", "nvpx", "mx24", "mx22", "mx20", "mx18", "mex24", "mex22",
    "XML Project Exchange File", "CAQDAS", "QDAS", "NVivo", "MAXQDA", "ATLAS.ti",
    "f4analyse", '"QDA Miner"', "QualCoder", "Quirkos", "Transana", "RQDA", "Dedoose",
    '"MAXQDA Analytics Pro"', '"Leipzig Corpus Miner"', '"REFI-QDA Standard"',
    '"Rotterdam Exchange Format Initiative"', '"REFI-QDA Project"', '"REFI-QDA Codebook"',
    '"Interoperability standard"', '"a qualitative methodology is employed"',
    '"audit trail"', '"axial coding"', '"codebook thematic analysis"',
    '"cognitive interviewing"', '"computer assisted qualitative data analysis"',
    '"constant comparative method"', '"constant comparison"',
    '"constructivist grounded theory"', '"convenience sampling"', '"criterion sampling"',
    '"data saturation"', '"dependability and credibility"',
    '"ensure the trustworthiness of the research"', '"ethnographic research"',
    '"extreme case sampling"', '"face-to-face in-depth interviews"',
    '"focus group discussions"', '"generating initial themes"', '"grounded theory"',
    '"in-depth interviews"', '"inductive coding"', '"inductive thematic saturation"',
    '"interpretative phenomenological analysis"', '"latent codes"', '"lived experience"',
    '"maximum variation sampling"', '"member checking"',
    '"member checking in qualitative research"', '"negative case analysis"',
    '"open coding"', '"open, axial, and selective coding"', '"open-ended questions"',
    '"open-ended responses"', '"participant observation"', '"participant validation"',
    '"peer debriefing"', '"peer debriefing, negative case analysis"',
    '"persistent observation"', '"phenomenological study"', '"prolonged engagement"',
    '"prolonged engagement, persistent observation"', '"purposive and snowball sampling"',
    '"purposive sampling"', '"qualitative case study approach"',
    '"qualitative descriptive research"', '"reflexive thematic analysis"',
    '"respondent validation"', '"rich thick description"', '"selective coding"',
    '"semantic codes"', '"semi-structured interviews"',
    '"semi-structured interviews were conducted"', '"snowball sampling"',
    '"synthesized member checking"', '"the constant comparative method"',
    '"thematic analysis"', '"theoretical sampling"', '"theoretical sampling and saturation"',
    '"theoretical saturation"', '"thick description"',
    '"trustworthiness in qualitative research"', '"trustworthiness of the research"',
    '"typical case sampling"',
]

# BROAD_TERMS: General qualitative terms for targeted domain repositories.
BROAD_TERMS = [
    '"Qualitative Data Analysis"', '"Qualitative Research"', '"Qualitative Inquiry"',
    '"Mixed Methods"', '"Content Analysis"', '"Grounded Theory"', '"Narrative Analysis"',
    '"Axial Coding"', '"Action Research"', '"Thematic Analysis"',
    '"Semi-structured interviews"', '"Focus groups"', '"Interview transcripts"',
    '"Focus group transcripts"', '"Qualitative data"', '"Field notes"',
    '"Observation data"', '"Qualitative datasets"', '"Topic guide"',
    '"Interview schedule"', '"Open qualitative data"', "Codebook", '"Code system"',
    '"Coding scheme"', "Memos", '"Code memos"', '"Document memos"', "Annotations",
    '"Coded segments"', '"Paraphrased segments"', "Paraphrases", "Emoticodes",
    '"Case variables"', '"Focus group speaker codes"', '"Document groups"',
    '"Summary Grid"', '"Intercoder agreement"', '"Code co-occurrence"',
    '"Code frequencies"', '"Transcription timestamps"', '"Smart Coding Tool"',
    "MAXMaps", "MAXDictio", '"Code Matrix Browser"', '"Code Relations Browser"',
    '"Interactive Quote Matrix"', '"Document Portrait"', "Codeline", '"Word Trends"',
    '"Questions - Themes - Theories"', '"Creative Coding"',
]

# ── Utility Functions ──────────────────────────────────────────────────────────

def has_required_qda_project_file(ext: str) -> bool:
    """Checks if the extension corresponds to a mandatory QDA project file."""
    return (ext.lower() if ext else "") in REQUIRED_QDA_EXTENSIONS


def record_has_qda_link_in_metadata(metadata_blob: str | dict | list | None) -> bool:
    """Scans metadata fields for links ending in known QDA project extensions."""
    if metadata_blob is None:
        return False
    if isinstance(metadata_blob, (dict, list)):
        text = _flatten_metadata_to_text(metadata_blob)
    else:
        text = str(metadata_blob)
    text_lower = text.lower()
    for ext in TARGET_EXTENSIONS:
        if re.search(re.escape(ext) + r"(?:\?|#|\s|$|\)|,|\")", text_lower):
            return True
    return False


def _flatten_metadata_to_text(obj: dict | list) -> str:
    """Recursively flattens nested metadata structures into a searchable string."""
    out: list[str] = []

    def walk(v: object) -> None:
        if isinstance(v, dict):
            for val in v.values():
                walk(val)
        elif isinstance(v, list):
            for item in v:
                walk(item)
        else:
            out.append(str(v))

    walk(obj)
    return " ".join(out)


# Mapping of extensions to their respective software types
QDA_EXT_TO_SOFTWARE: dict[str, str] = {
    ".qdpx": "REFI-QDA",
    ".qdc": "REFI-QDA",
    **{e: "MAXQDA" for e in MAXQDA_SUITE},
    **{e: "NVivo" for e in NVIVO},
    ".qpd": "Quirkos",
    **{e: "ATLAS.ti" for e in ATLAS_OTHERS},
    **{e: "Analysis-Metadata" for e in ANALYSIS_METADATA},
}

# Primary data and media tiers
TEXT_EXTENSIONS: FrozenSet[str] = frozenset({".pdf", ".docx", ".txt", ".rtf"})
MEDIA_EXTENSIONS: FrozenSet[str] = frozenset({".mp3", ".wav", ".mp4"})


def file_ext(path: str) -> str:
    """Extracts the lowercase extension from a file path."""
    p = path.lower()
    i = p.rfind(".")
    return p[i:] if i >= 0 else ""


def file_tier(ext: str) -> int:
    """Categorizes file extensions into acquisition tiers: 1=QDA, 2=Text, 3=Media."""
    if ext in QDA_EXTENSIONS:
        return 1
    if ext in TEXT_EXTENSIONS or ext in ARCHIVE_EXTENSIONS:
        return 2
    if ext in MEDIA_EXTENSIONS:
        return 3
    return 0
