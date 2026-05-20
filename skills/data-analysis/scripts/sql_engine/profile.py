from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SqlEngineProfile:
    kind: str
    dialect: str
    identifier_quote: str = "`"
    date_functions: dict[str, str] = field(default_factory=dict)
    performance_notes: tuple[str, ...] = ()
    validation_notes: tuple[str, ...] = ()


MYSQL_PROFILE = SqlEngineProfile(
    kind="mysql",
    dialect="mysql",
    date_functions={
        "current_date": "CURRENT_DATE",
        "date_sub_days": "DATE_SUB(CURRENT_DATE, INTERVAL {n} DAY)",
    },
    performance_notes=(
        "优先使用可命中索引的过滤条件。",
        "避免对索引列包裹函数导致索引失效。",
        "JOIN key 类型应保持一致。",
    ),
)


STARROCKS_PROFILE = SqlEngineProfile(
    kind="starrocks",
    dialect="starrocks",
    date_functions={
        "current_date": "CURRENT_DATE()",
        "date_sub_days": "DATE_SUB(CURRENT_DATE(), INTERVAL {n} DAY)",
    },
    performance_notes=(
        "优先使用分区字段过滤，保证分区裁剪。",
        "大表 JOIN 前先过滤和聚合，避免 SELECT *。",
        "关注分桶键、排序键和物化视图命中。",
    ),
    validation_notes=("当前执行引擎的大查询应尽量带时间/分区过滤条件。",),
)


TRINO_PROFILE = SqlEngineProfile(
    kind="trino",
    dialect="trino",
    identifier_quote='"',
    date_functions={
        "current_date": "current_date",
        "date_sub_days": "date_add('day', -{n}, current_date)",
        "yyyymmdd_partition_string": "date_format(date_add('day', -{n}, current_date), '%Y%m%d')",
    },
    performance_notes=(
        "优先使用分区字段过滤，减少跨 connector 扫描。",
        "避免 SELECT *，只投影回答所需字段。",
        "大表 JOIN 前先过滤和聚合。",
    ),
    validation_notes=(
        "当前执行引擎的大查询应尽量带时间或分区过滤条件。",
        "Trino/Presto 的 date_format 使用 MySQL 风格格式串，例如 '%Y%m%d'。",
        "如需 Java 风格日期格式串，应使用 format_datetime(timestamp, 'yyyyMMdd')。",
    ),
)


def profile_for_kind(kind: str) -> SqlEngineProfile:
    if kind == "mysql":
        return MYSQL_PROFILE
    if kind == "starrocks":
        return STARROCKS_PROFILE
    if kind == "trino":
        return TRINO_PROFILE
    raise ValueError(f"不支持的执行引擎：{kind}。仅支持 mysql、starrocks、trino。")
