# Seeding QDArchive

This repository contains the source code and data for the QDArchive project, organized into modular components.

## Project Structure

### [Part 1: Data acquisition](./Part%201:%20Data%20acquisition/)
The first module of the project, focusing on asynchronous metadata harvesting and automated acquisition of qualitative data from global research repositories.

- **Status**: Completed & Sanitized
- **Core Engine**: Asynchronous pipeline using `aiohttp` and `asyncio`.
- **Harvesters**: Specialized modules for Zenodo, Dryad, CESSDA, Dataverse, and more.

## Data Storage
The raw harvested files (Qualitative Data) are stored externally due to size constraints. You can access the complete collection here:
- **Google Drive**: [QDArchive Raw Data](https://drive.google.com/drive/folders/1oNg3-zzRhJhrN8E34G3Kxv2ZNv7VQ4dq?usp=sharing)

---
*Author: Ahmadreza Nourozi*
