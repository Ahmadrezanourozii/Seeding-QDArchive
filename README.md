# Seeding QDArchive

**QDArchive** is a production-grade, asynchronous data harvesting pipeline designed to aggregate and standardize Qualitative Data Analysis (QDA) resources from major global research repositories. 

This repository documents the first phase of a large-scale data engineering project: **Part 1 - Data Acquisition**.

## 🚀 Key Innovations & Engineering Highlights

### 1. The "Python Firewall" & Weighted Scoring System
Most repository search engines (like Zenodo or Dataverse) return significant noise when queried with qualitative terms. To combat this, we implemented a **two-tier validation firewall**:
- **Pre-selection Querying**: Dynamic query generation using advanced QDA-specific operators.
- **Weighted Scoring System**: Every discovered record is passed through a Python-based scoring engine. It evaluates the title, description, and keywords against a weighted taxonomy of CAQDAS software (NVivo, ATLAS.ti, MAXQDA) and qualitative methodologies. A record only enters the `PROJECTS` table if it crosses a strict relevance threshold, ensuring the vector space remains "poison-free."

### 2. High-Concurrency Asynchronous Architecture
Built on `aiohttp` and `asyncio`, the pipeline utilizes a producer-consumer architecture. It separates the **Discovery Metadata Harvesters** from the **Asynchronous Downloader**, allowing for thousands of parallel I/O operations without bottlenecking the system.

### 3. Hybrid Acquisition: API + Web Scraping
For repositories with limited API surfaces (e.g., ICPSR or CESSDA), the pipeline dynamically switches to scraping logic to ensure 100% metadata coverage where standard API calls fail.

---

## 📊 Acquisition Metrics at a Glance

| Metric | Value |
| :--- | :--- |
| **Total Download Volume** | > 270 GB |
| **Verified QDA Projects** | 3,852 |
| **Successfully Harvested Files** | 20,470 |
| **Database Quality** | Sanitized, De-duplicated, Enriched |

### Source Distribution
The pipeline integrates 14+ global research archives:
- **Zenodo**: 1,221 Projects
- **CESSDA Catalogue**: 979 Projects (European Union)
- **Dataverse Network**: 1,200+ Projects (Harvard, QDR, DANS, etc.)
- **IHSN & ICPSR**: Specialized social science archives.

---

## 🛠 Project Structure

### [Part 1: Data acquisition](./Part%201:%20Data%20acquisition/)
The core engine. Contains the harvester package, the asynchronous downloader, and the optimized SQLite metadata database.

## 📦 Data Storage
Due to the massive volume (>270GB), the raw harvested files are stored in an external high-availability storage:
- **Google Drive**: [QDArchive Raw Data](https://drive.google.com/drive/folders/1oNg3-zzRhJhrN8E34G3Kxv2ZNv7VQ4dq?usp=sharing)
- **Database Viewer**: [View SQLite Database](https://beta.sqliteviewer.app/metadata.sqlite/table/FILES)

---

## 📖 Lessons Learned & Engineering Challenges (Part 1)

Building a scaleable pipeline for heterogeneous data sources presented several architectural bottlenecks:

- **The Rate-Limit Wall**: Repositories like Harvard (Murray Archive) triggered 403 Forbidden errors due to rapid parallel querying. We resolved this by implementing **Wildcard Fetching** and sophisticated **User-Agent Masquerading** to bypass anti-bot detection silently.
- **The "Ghost Folder" Problem**: We encountered records where metadata was public but files were restricted. Initially, this created empty directories on disk. We implemented a **File System Integrity Check** to purge these "ghost folders," ensuring the database and disk remain perfectly synchronized for future RAG ingestion.
- **The Virtual File Hack**: Many aggregators (like CESSDA) do not host files but link to them. Our system was originally designed for direct downloads. We innovated the **Virtual File Record**—storing external source links as "Virtual Files" with a `FAILED: Hosted externally` status, preserving 100% of the European metadata footprint without breaking the relational schema.

---
*Author: Ahmadreza Nourozi*  
*FAU Erlangen-Nuremberg | Master’s in Artificial Intelligence*
