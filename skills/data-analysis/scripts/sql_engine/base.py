from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from sql_engine.profile import SqlEngineProfile


class BaseEngine(ABC):
    @abstractmethod
    def get_profile(self) -> SqlEngineProfile:
        """Return SQL dialect and engine-specific guidance."""

    @abstractmethod
    def list_tables_meta(self, candidates: list[str] | None = None) -> list[dict[str, str]]:
        """Return table metadata for SQL introspection."""

    @abstractmethod
    def execute_select(self, sql: str) -> list[dict[str, Any]]:
        """Execute one read-only SELECT/WITH query and return rows."""

    def close(self) -> None:
        """Best-effort resource cleanup for engines that hold pools."""
