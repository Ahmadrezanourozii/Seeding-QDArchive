# QDArchive: Part 1 - Data Acquisition Module

This module represents the first milestone of the QDArchive project. It is responsible for the large-scale asynchronous harvesting, filtering, and acquisition of qualitative data from 14+ global research repositories.

## 🏗 Architectural Design
The module is built on a **Producer-Consumer / Async IO** architecture:
1.  **Harvesters (Producers)**: Specialized Python modules that iterate through repository APIs (or scrape web interfaces) to discover projects.
2.  **The QDA Firewall (Validator)**: A weighted keyword scoring engine that validates projects for qualitative relevance before they reach the queue.
3.  **The Downloader (Consumer)**: A high-concurrency engine that acquires files and manages the relational database state.
4.  **Telemetry Dashboard**: A real-time CLI interface that provides live health metrics of the pipeline.

## 💎 Data Quality & Engineering Excellence
We built several internal tools to ensure the resulting `23726011-seeding.db` database is of senior-engineer quality:
- **Deduplication Logic**: Aggregator overlaps (where a project appears in multiple archives) were purged using the `db_cleaner.py` "Data Surgeon," reducing vector database pollution.
- **AI-Enhanced Metadata**: Missing language tags and descriptions were inferred using the `langdetect` library and content mirroring.
- **Foreign Key Integrity**: Automated scripts mapped orphaned files and corrected virtual link types.

---

## 📖 Engineering Logs & Issues Resolved

During the 270GB+ acquisition process, we encountered several critical bottlenecks. This log documents the architectural decisions made to resolve them:

### 1. Security & Bot Prevention
- **🛑 Rate Limits (Murray Archive)**: Rapid parallel requests resulted in `403 Forbidden` errors.
  - **Decision**: Switched from granular keyword searching to **Wildcard Fetching** (`q=*`) and implemented **Request Throttling** (Sleep/Jitter) to masquerade as an academic research bot.
- **🧱 Anti-Bot Firewalls (ADA Australia)**: Cloudflare-style JS challenges blocked direct API access.
  - **Decision**: To maintain asynchronous performance, we moved these sources to the `FAILED: Pending whitelist` queue, preferring system stability over the complexity of heavy headless browser simulation.

### 2. Database & File System Integrity
- **👻 Ghost Folders**: We faced records with public metadata but restricted actual files.
  - **Decision**: Developed a cleanup surgical script to purge directories that didn't have corresponding succeeded file records, ensuring a 1:1 match between the database and the 270GB local storage.
- **🌍 The Virtual File Hack (CESSDA)**: European aggregators link to data without hosting it.
  - **Decision**: Invented the **Virtual File Record** (`*.url`). Instead of breaking the database schema, we stored external links as "Succeeded Virtual Files," preserving the entire European metadata footprint for RAG indexing.

### 3. Data Cleansing
- **👯 Duplication Overlap**: One project from DANS appeared 61 times due to aggregator mirroring.
  - **Decision**: Built the `db_cleaner.py` utility. It identifies duplicates and intelligently retains only the "Best Version" based on DOI presence and direct repository reliability.

---

## 📂 Implementation Details
```text
QDArchive_Code/Part 1: Data acquisition/
├── main.py                # Orchestration & Pipeline entry
├── config.py              # Environment & API management
├── constants.py           # QDA Terminology & Tier weights
├── database/              # Relational Storage
│   └── 23726011-seeding.db    # The finalized 3,852 record database
├── harvesters/            # Repository-specific harvesting logic
│   ├── zenodo_harvester.py
│   ├── dryad_harvester.py
│   └── ...
├── dashboard.py           # Real-time Telemetry CLI
└── requirements.txt       # Python dependencies
```

## 🛠 Usage
```bash
# Start the pipeline
python main.py --all --strict --workers 10

# Monitor live telemetry
python dashboard.py
```

---
*Author: Ahmadreza Nourozi*  
*FAU Erlangen-Nuremberg*
