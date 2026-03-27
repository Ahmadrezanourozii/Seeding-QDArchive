# QDArchive: Qualitative Data Acquisition Pipeline

A production-grade, asynchronous data harvesting pipeline designed to aggregate Qualitative Data Analysis (QDA) resources from major global research repositories. 

## Overview
QDArchive implements a robust architecture for discovering, filtering, and acquiring social science research data. It specializes in identifying QDA-specific file formats (e.g., `.nvp`, `.hprx`, `.atlasti`) across diverse APIs and OAI-PMH endpoints.

## Core Features
- **Asynchronous Harvesting**: High-performance metadata extraction using `aiohttp` and `asyncio`.
- **Intelligent Filtering**: Multi-tier QDA relevance scoring and automated metadata inspections.
- **Heterogeneous Sources**: Unified interface for Zenodo, Dryad, Dataverse, ICPSR, IHSN, CESSDA, and more.
- **Relational Persistence**: SQL-backed metadata storage with detailed acquisition telemetry.
- **Real-time Telemetry**: Integrated CLI dashboard for monitoring pipeline health and acquisition stats.

## Project Structure
```text
.
├── main.py                # Pipeline entry point & orchestration
├── config.py              # Centralized configuration & environment management
├── constants.py           # Domain-specific constants & QDA tier definitions
├── db.py                  # Database schema & persistence layer
├── downloader.py          # Asynchronous file acquisition engine
├── harvester_base.py      # Abstract interface for repository harvesters
├── oai_common.py          # Shared utilities for OAI-PMH protocols
├── dashboard.py           # Real-time CLI telemetry dashboard
├── harvesters/            # Repository-specific implementations
│   ├── zenodo_harvester.py
│   ├── dryad_harvester.py
│   ├── cessda_harvester.py
│   └── ...
└── requirements.txt       # Project dependencies
```

## Installation
1. Clone the repository.
2. Create a virtual environment: `python -m venv venv`
3. Install dependencies: `pip install -r requirements.txt`
4. Configure environment variables in `.env` (see `.env.example`).

## Usage
Start the harvesting pipeline:
```bash
python main.py
```

Monitor progress in real-time:
```bash
python dashboard.py
```

## Author
**Ahmadreza Nourozi**  
Master's Student in Artificial Intelligence  
FAU Erlangen-Nuremberg

## License
Creative Commons Attribution 4.0 International (CC BY 4.0)
