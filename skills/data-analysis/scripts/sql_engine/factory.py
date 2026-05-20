from __future__ import annotations

from sql_engine.base import BaseEngine
from sql_engine.config import EngineConfig
from sql_engine.mysql_engine import MysqlEngine
from sql_engine.starrocks_engine import StarRocksEngine
from sql_engine.trino_engine import TrinoEngine


def create_engine_from_config(cfg: EngineConfig) -> BaseEngine:
    if cfg.kind == "mysql":
        return MysqlEngine(cfg)
    if cfg.kind == "starrocks":
        return StarRocksEngine(cfg)
    if cfg.kind == "trino":
        return TrinoEngine(cfg)
    raise ValueError(f"不支持的执行引擎：{cfg.kind}。仅支持 mysql、starrocks、trino。")
