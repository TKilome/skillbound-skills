from __future__ import annotations

from typing import Any

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.pool import NullPool

from sql_engine.base import BaseEngine
from sql_engine.config import EngineConfig
from sql_engine.profile import SqlEngineProfile


class SqlAlchemyEngine(BaseEngine):
    def __init__(self, cfg: EngineConfig, profile: SqlEngineProfile) -> None:
        self._cfg = cfg
        self._profile = profile
        self._engine = create_engine(cfg.dsn, pool_pre_ping=True, poolclass=NullPool)

    def get_profile(self) -> SqlEngineProfile:
        return self._profile

    def list_tables_meta(self, candidates: list[str] | None = None) -> list[dict[str, str]]:
        selected = {name.lower() for name in candidates or [] if name}
        inspector = inspect(self._engine)
        output: list[dict[str, str]] = []
        for table in inspector.get_table_names():
            if selected and table.lower() not in selected:
                continue
            columns = inspector.get_columns(table)
            column_lines = [f"  {col['name']} {col.get('type')}" for col in columns]
            ddl = f"CREATE TABLE {table} (\n" + ",\n".join(column_lines) + "\n)"
            output.append({"table": table, "ddl": ddl})
        return output

    def execute_select(self, sql: str) -> list[dict[str, Any]]:
        with self._engine.connect() as conn:
            result = conn.execute(text(sql))
            return [dict(row._mapping) for row in result]

    def close(self) -> None:
        self._engine.dispose()
