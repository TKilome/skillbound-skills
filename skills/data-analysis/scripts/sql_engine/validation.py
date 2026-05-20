from __future__ import annotations

import re
from time import perf_counter
from typing import Any

from sql_engine.profile import SqlEngineProfile


FORBIDDEN_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "merge",
    "replace",
    "grant",
    "revoke",
    "use",
    "set",
    "call",
    "exec",
    "execute",
    "load",
    "copy",
    "export",
    "analyze",
    "optimize",
}


def _strip_trailing_semicolon(sql: str) -> str:
    return sql.rstrip().rstrip(";").strip()


def _keyword_hits(sql: str) -> list[str]:
    lowered = sql.lower()
    return sorted(
        kw for kw in FORBIDDEN_KEYWORDS if re.search(rf"\b{re.escape(kw)}\b", lowered)
    )


def validate_sql(
    sql: str | None,
    *,
    profile: SqlEngineProfile,
    require_limit: bool = False,
) -> dict[str, Any]:
    raw = (sql or "").strip()
    parser_dialect = "mysql" if profile.dialect == "starrocks" else profile.dialect
    diagnostics: dict[str, Any] = {
        "engineKind": profile.kind,
        "dialect": parser_dialect,
        "sqlLength": len(raw),
        "parser": "fallback",
        "parseElapsedMs": None,
    }
    errors: list[str] = []
    warnings: list[str] = []

    if not raw:
        errors.append("SQL 为空，无法执行。")
        return {
            "passed": False,
            "errors": errors,
            "warnings": warnings,
            "validatedSql": None,
            "diagnostics": diagnostics,
        }
    if len(raw) > 50_000:
        return {
            "passed": False,
            "errors": ["SQL 过长，超过 50000 字符。"],
            "warnings": [],
            "validatedSql": None,
            "diagnostics": diagnostics,
        }

    stripped = _strip_trailing_semicolon(raw)

    try:
        import sqlglot  # type: ignore
        from sqlglot import expressions as exp  # type: ignore
    except Exception:
        parsed = None
    else:
        try:
            started = perf_counter()
            parsed = [
                expr
                for expr in sqlglot.parse(raw, read=parser_dialect)
                if expr is not None
            ]
            diagnostics["parser"] = "sqlglot"
            diagnostics["parseElapsedMs"] = round((perf_counter() - started) * 1000, 3)
        except Exception as exc:
            diagnostics["parser"] = "sqlglot"
            diagnostics["parseElapsedMs"] = round((perf_counter() - started) * 1000, 3)
            return {
                "passed": False,
                "errors": [f"SQL 解析失败：{exc}。"],
                "warnings": [],
                "validatedSql": stripped,
                "diagnostics": diagnostics,
            }

    if parsed is not None:
        if len(parsed) != 1:
            errors.append("仅允许单条 SQL，不允许包含多个语句。")
        elif not isinstance(parsed[0], (exp.Select, exp.Union)):
            errors.append("仅允许 SELECT 或 WITH ... SELECT 查询。")
        else:
            forbidden_nodes = (
                exp.Insert,
                exp.Update,
                exp.Delete,
                exp.Drop,
                exp.Create,
                exp.Alter,
                exp.TruncateTable,
                exp.Merge,
                exp.Command,
            )
            hit = parsed[0].find(*forbidden_nodes)
            if hit is not None:
                errors.append(f"SQL 包含禁止操作：{type(hit).__name__}。")
    else:
        if ";" in stripped:
            errors.append("仅允许单条 SQL，不允许包含多个语句。")
        if not re.match(r"(?is)^(with|select)\s", stripped):
            errors.append("仅允许 SELECT 或 WITH ... SELECT 查询。")

    hits = _keyword_hits(stripped)
    if hits:
        errors.append(f"SQL 包含禁止关键字：{', '.join(hits)}。")

    padded = f" {stripped.lower()} "
    has_limit = " limit " in padded
    if " where " not in padded and not has_limit:
        warnings.append("SQL 未包含 WHERE/LIMIT，可能扫描较多数据。")
    if " join " in padded and " on " not in padded and " using " not in padded:
        warnings.append("SQL 包含 JOIN 但未检测到 ON/USING 条件，请确认不是笛卡尔积。")
    if profile.kind in {"starrocks", "trino"}:
        if "select *" in padded:
            warnings.append("当前执行引擎查询建议避免 SELECT *，只投影必要字段。")
        if " where " not in padded:
            warnings.append("当前执行引擎的大查询建议包含时间或分区过滤条件，便于分区裁剪。")
    if require_limit and not has_limit:
        errors.append("明细查询必须包含 LIMIT 或由运行时强制限制行数。")

    return {
        "passed": not errors,
        "errors": errors,
        "warnings": warnings,
        "validatedSql": stripped,
        "diagnostics": diagnostics,
    }
