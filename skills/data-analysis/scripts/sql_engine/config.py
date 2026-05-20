from __future__ import annotations

from dataclasses import dataclass


SUPPORTED_ENGINE_KINDS = ("mysql", "starrocks", "trino")


@dataclass(frozen=True)
class EngineConfig:
    kind: str
    dsn: str


def normalize_engine_kind(value: str | None) -> str:
    kind = (value or "").strip().lower()
    if not kind:
        raise ValueError("engine kind is required")
    if kind not in SUPPORTED_ENGINE_KINDS:
        raise ValueError(
            f"不支持的执行引擎：{kind}。仅支持 mysql、starrocks、trino。"
        )
    return kind


def normalize_dsn(value: str | None) -> str:
    dsn = (value or "").strip()
    if not dsn:
        raise RuntimeError("dsn is required")
    if dsn.startswith("mysql://"):
        return "mysql+pymysql://" + dsn[len("mysql://") :]
    return dsn
