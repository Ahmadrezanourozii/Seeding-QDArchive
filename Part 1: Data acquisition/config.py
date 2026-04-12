#!/usr/bin/env python3
"""
Author: Ahmadreza Nourozi | Master's Student in Artificial Intelligence, FAU Erlangen-Nuremberg
Description: Manages application settings, path resolutions, and dynamic API credential loading from environment variables.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str = "") -> str:
    """Reads an environment variable and returns a stripped string."""
    return os.environ.get(key, default).strip()


def _load_dynamic_tokens() -> dict[str, str]:
    """Scans environment variables for API tokens and maps them to source identifiers."""
    tokens = {}
    for key, value in os.environ.items():
        k = key.upper()
        if any(k.endswith(s) for s in ["_API_TOKEN", "_ACCESS_TOKEN", "_TOKEN"]):
            for suffix in ["_API_TOKEN", "_ACCESS_TOKEN", "_TOKEN"]:
                if k.endswith(suffix):
                    source_id = k[: -len(suffix)].lower()
                    tokens[source_id] = value.strip()
                    break

    # Specialized credentials for the Dryad API
    tokens["dryad_account_id"] = _env("DRYAD_ACCOUNT_ID") or _env("DRYAD_CLIENT_ID")
    tokens["dryad_secret"] = _env("DRYAD_SECRET")
    return tokens


@dataclass(frozen=True)
class APICredentials:
    """Container for dynamically loaded API credentials."""

    tokens: dict[str, str] = field(default_factory=_load_dynamic_tokens)

    def get(self, key: str) -> str:
        """Retrieves a credential by its source identifier (e.g., 'zenodo')."""
        return self.tokens.get(key.lower(), "")

    def as_dict(self) -> dict[str, Any]:
        """Returns the credential mapping as a dictionary."""
        return {k: v for k, v in self.tokens.items()}


@dataclass(frozen=True)
class Settings:
    """Global application configuration including directory paths and API access."""

    data_dir: str = field(default_factory=lambda: _env("DATA_DIR"))
    api: APICredentials = field(default_factory=APICredentials)

    @property
    def zenodo_access_token(self) -> str:
        """Explicit getter for the Zenodo access token."""
        return self.api.get("zenodo")

    # Path to the SQLite metadata database
    metadata_db_path: str = field(default_factory=lambda: _env("METADATA_DB_PATH"))

    def get_base_path(self) -> Path:
        """Resolves and ensures the existence of the primary data directory."""
        if not self.data_dir:
            raise RuntimeError(
                "DATA_DIR is not set. Environment variable configuration is required."
            )
        path = Path(self.data_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_db_path(self) -> Path:
        """Resolves the absolute path to the metadata database file."""
        if self.metadata_db_path:
            return Path(self.metadata_db_path)
        return Path(__file__).parent / "database" / "23726011-seeding.db"


# Global settings singleton
_settings: Settings | None = None


def get_settings() -> Settings:
    """Returns the initialized global settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def get_base_path() -> Path:
    """Global helper to retrieve the base data directory path."""
    return get_settings().get_base_path()


def get_db_path() -> Path:
    """Global helper to retrieve the metadata database path."""
    return get_settings().get_db_path()

