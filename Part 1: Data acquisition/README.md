# QDArchive: Qualitative Data Acquisition Pipeline

A production-grade, asynchronous data harvesting pipeline designed to aggregate Qualitative Data Analysis (QDA) resources from major global research repositories. 

## Overview
QDArchive implements a robust architecture for discovering, filtering, and acquiring social science research data. It specializes in identifying QDA-specific file formats (e.g., `.nvp`, `.hprx`, `.atlasti`) across diverse APIs and OAI-PMH endpoints.

## Core Features
- **Asynchronous Harvesting**: High-performance metadata extraction using `aiohttp` and `asyncio`.
- **Intelligent Filtering**: Multi-tier QDA relevance scoring and automated metadata inspections.
- **Heterogeneous Sources**: Unified interface for Zenodo, Dryad, Dataverse, ICPSR, IHSN, CESSDA, and more.
- **Relational Persistence**: SQL-backed metadata storage with detailed acquisition telemetry.

## Raw Data Access
Due to the large volume of harvested files, the raw datasets (Qualitative Data) are hosted externally:
- **Google Drive**: [QDArchive Raw Data](https://drive.google.com/drive/folders/1oNg3-zzRhJhrN8E34G3Kxv2ZNv7VQ4dq?usp=sharing)

## Module Structure
```text
.
├── main.py                # Pipeline entry point & orchestration
├── config.py              # Centralized configuration & environment management
├── constants.py           # Domain-specific constants & QDA tier definitions
├── database/              # Metadata storage
│   └── metadata.sqlite    # SQLite database (Part 1 output)
├── downloader.py          # Asynchronous file acquisition engine
├── harvester_base.py      # Abstract interface for repository harvesters
├── oai_common.py          # Shared utilities for OAI-PMH protocols
├── dashboard.py           # Real-time CLI telemetry dashboard
├── harvesters/            # Repository-specific implementations
│   ├── zenodo_harvester.py
│   ├── dryad_harvester.py
│   └── ...
└── requirements.txt       # Project dependencies
```

## Installation & Usage
1. Clone the repository and navigate to this directory.
2. Install dependencies: `pip install -r requirements.txt`
3. Configure environment variables in `.env` (see `.env.example`).
4. Run the pipeline: `python main.py`

## Author
**Ahmadreza Nourozi**  
Master's Student in Artificial Intelligence  
FAU Erlangen-Nuremberg
