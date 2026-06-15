# Part 2: Classification

Metadata classification pipeline for the merged QDArchive SQLite database: extension-based project typing and ISIC Rev.5 zero-shot industry labeling.

## Pipeline overview

| Step | Script | Purpose |
| :--- | :--- | :--- |
| 1 | `assign_project_types.py` | Repair noisy `FILES.file_type` values and set `PROJECTS.type` (`QDA_PROJECT`, `QD_PROJECT`, `OTHER_PROJECT`, `NOT_A_PROJECT`). |
| 2 | `isic_classifier.py` | Zero-shot ISIC Rev.5 **division** (2-digit) classification for `QDA_PROJECT` and `QD_PROJECT` records. |

The canonical database lives at the repository root: [`../23726011-seeding.db`](../23726011-seeding.db).

## Database schema additions

**`PROJECTS`**

| Column | Description |
| :--- | :--- |
| `type` | Material-type label derived from constituent file extensions. |
| `class` | ISIC Rev.5 division title (human-readable). |
| `isic_division_code` | Two-digit ISIC division code. |
| `isic_section_title` | Parent ISIC section (letter + title). |
| `class_tags` | Top-3 division titles for search and faceting. |

**`FILES`**

| Column | Description |
| :--- | :--- |
| `class` | Inherited from the parent project after ISIC classification. |

## Project typing rules

- Extensions are normalized from `FILES.file_type`, with filename fallback when the stored type is missing or noisy (e.g. Dataverse `url` rows pointing to `.txt` sidecars).
- `NOT_A_PROJECT` is reserved for explicit non-research artefacts (`rdata`, `rproj`, `fasta`, `phy`, `newick`, `tree`).
- Project `type` follows the strongest signal among its files: **QDA → QD → OTHER → NOT_A**.

## ISIC classifier

- **Taxonomy**: [ISIC Rev. 5](https://unstats.un.org/unsd/classifications/Econ/ISIC.cshtml) division level (87 two-digit classes) from `taxonomy/ISIC_Rev_5_english_structure.csv`.
- **Model**: `paraphrase-multilingual-mpnet-base-v2` (sentence-transformers).
- **Method**: Cosine similarity between project metadata embeddings and pre-encoded division labels (zero-shot, no training data).
- **Metadata embedded**: title, description, language, DOI, query context, keywords, primary file names, and format summary. URLs are excluded.
- **Scope**: Only `QDA_PROJECT` and `QD_PROJECT` rows are classified; file `class` is propagated from the parent project.

## Setup

```bash
cd "Part 2: Classification"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # add HF_TOKEN for faster Hugging Face downloads
```

## Usage

```bash
# Step 1 — assign extension-based project types
python assign_project_types.py

# Step 2 — ISIC division classification + Excel report
python isic_classifier.py

# Smoke test (first 10 projects)
python isic_classifier.py --limit 10
```

## Outputs

- Updated `23726011-seeding.db` at the repository root.
- `reports/isic_class_distribution_by_repository.xlsx` — pivot tables of ISIC division counts per `repository_id` for QDA and QD projects.

## Consolidation note

The root database consolidates the Part 1 seeding harvest with two supplemental SQLite exports (deduplicated on normalized `project_url`). Source databases are not versioned in this repository because of size; only the merged canonical database is published.

---

*Author: Ahmadreza Nourozi | FAU Erlangen-Nuremberg | Master's in Artificial Intelligence*
