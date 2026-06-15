# Seeding QDArchive

**QDArchive** is a production-grade pipeline for aggregating, classifying, and standardizing qualitative research data from global repositories.

This repository documents a three-phase research engineering project:

1. **Part 1 — Data Acquisition** — asynchronous harvesting and validation
2. **Part 2 — Classification** — extension-based typing and ISIC Rev.5 industry labeling
3. **Part 3 — Data Analysis** *(planned)*

---

## Part 1: Data Acquisition

### Key innovations

**Python firewall and weighted scoring** — Records pass through a scoring engine that evaluates title, description, and keywords against a CAQDAS-aware taxonomy (NVivo, ATLAS.ti, MAXQDA). Only records above the relevance threshold enter `PROJECTS`.

**High-concurrency architecture** — Producer–consumer design on `aiohttp` / `asyncio` separates metadata harvesters from the asynchronous downloader.

**Hybrid acquisition** — API-first ingestion with scraping fallbacks for repositories with limited API coverage (ICPSR, CESSDA).

### Acquisition metrics

| Metric | Value |
| :--- | :--- |
| **Total download volume** | > 270 GB |
| **Initial verified QDA projects (Part 1)** | 3,852 |
| **Initial harvested files (Part 1)** | 20,470 |
| **Database quality report** | [Sanitized, de-duplicated, enriched](https://ahmadrezanourozii.github.io/Seeding-QDArchive/report/) |

### Source distribution (Part 1 seed)

- **Zenodo**: 1,221 projects
- **CESSDA Catalogue**: 979 projects
- **Dataverse Network**: 1,200+ projects (Harvard, QDR, DANS, etc.)
- **IHSN & ICPSR**: specialized social science archives

### [Part 1: Data acquisition](./Part%201:%20Data%20acquisition/)

Harvester package, asynchronous downloader, and the SQLite metadata schema.

---

## Part 2: Classification

Extension-based material typing and zero-shot **ISIC Rev. 5** division classification for qualitative archive metadata.

### Pipeline

| Stage | Output |
| :--- | :--- |
| **Database consolidation** | Merged canonical `23726011-seeding.db` (deduplicated on `project_url`) |
| **Project typing** | `PROJECTS.type` — `QDA_PROJECT`, `QD_PROJECT`, `OTHER_PROJECT`, `NOT_A_PROJECT` |
| **ISIC labeling** | `PROJECTS.class`, `isic_division_code`, `isic_section_title`, `class_tags` |
| **File propagation** | `FILES.class` inherited from parent project |
| **Distribution report** | Excel pivot: repository × ISIC division counts |

### Classification metrics (merged database)

| Metric | Value |
| :--- | :--- |
| **Total projects** | 53,532 |
| **Total file records** | 550,020 |
| **QDA projects** | 36,596 |
| **QD projects** | 5,296 |
| **Other projects** | 11,622 |
| **Not-a-project** | 18 |
| **ISIC-classified projects** | 41,892 (QDA + QD) |
| **ISIC taxonomy depth** | Division level (2-digit, 87 classes) |
| **Embedding model** | `paraphrase-multilingual-mpnet-base-v2` |

### Engineering highlights

- **Noisy extension repair** — Corrects mis-tagged Dataverse sidecars (`url` → `txt`) and placeholder rows (`dataset` → inferred type) before typing.
- **Zero-shot semantic matching** — Multilingual sentence embeddings matched against ISIC division labels; no labeled training set required.
- **Metadata-only inference** — Title, description, keywords, DOI, and primary file names are embedded; repository URLs are excluded from the feature text.
- **Search tags** — Top-3 ISIC division titles stored in `class_tags` for faceted search.

### [Part 2: Classification](./Part%202:%20Classification/)

Scripts, ISIC taxonomy reference, requirements, and the repository distribution report.

---

## Project structure

```
Seeding-QDArchive/
├── 23726011-seeding.db          # Canonical merged + classified metadata
├── Part 1: Data acquisition/    # Harvesting pipeline
├── Part 2: Classification/      # Typing + ISIC classifier
└── Visualization of my code/    # Architecture diagrams
```

## Data storage

Raw harvested files (>270 GB) are stored externally:

- **Google Drive**: [QDArchive Raw Data](https://drive.google.com/drive/folders/1oNg3-zzRhJhrN8E34G3Kxv2ZNv7VQ4dq?usp=sharing)
- **Database viewer**: [SQLite viewer](https://beta.sqliteviewer.app/23726011-seeding.db/table/FILES)
- **Quality report**: [Data quality profiling](https://ahmadrezanourozii.github.io/Seeding-QDArchive/report/)

---

## Lessons learned

**Part 1**

- **Rate-limit wall** — Wildcard fetching and user-agent rotation for anti-bot surfaces (Harvard Murray Archive).
- **Ghost folders** — Filesystem integrity checks remove empty download directories when metadata is public but files are restricted.
- **Virtual file records** — External links (CESSDA) stored as virtual files with `FAILED: Hosted externally` status.

**Part 2**

- **Extension noise** — Repository-specific `file_type` values required filename-aware repair before reliable typing.
- **Merge deduplication** — Normalized `project_url` as the canonical key when consolidating heterogeneous SQLite exports.
- **Hierarchical taxonomy** — ISIC division level balances granularity with zero-shot accuracy without supervised labels.

---

*Author: Ahmadreza Nourozi*  
*FAU Erlangen-Nuremberg | Master's in Artificial Intelligence*
