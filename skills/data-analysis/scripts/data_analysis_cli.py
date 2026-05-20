#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import urllib.request
import uuid
from pathlib import Path
from time import perf_counter
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from sql_engine.config import EngineConfig, normalize_dsn  # noqa: E402
from sql_engine.factory import create_engine_from_config  # noqa: E402
from sql_engine.profile import profile_for_kind  # noqa: E402
from sql_engine.config import normalize_engine_kind  # noqa: E402
from sql_engine.validation import validate_sql as validate_sql_for_profile  # noqa: E402


def _json_response(payload: dict[str, Any], *, exit_code: int = 0) -> int:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str))
    return exit_code


def _read_json_file(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _read_payload(input_path: str | None) -> dict[str, Any]:
    if input_path:
        payload = _read_json_file(input_path)
    else:
        raw = sys.stdin.read().strip()
        payload = json.loads(raw) if raw else {}
    if not isinstance(payload, dict):
        raise ValueError("input payload must be a JSON object")
    return payload


def _output_dir() -> Path:
    path = Path(__file__).resolve().parent / "output"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_json_artifact(
    prefix: str,
    payload: dict[str, Any],
) -> Path:
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = _output_dir() / f"{prefix}_{stamp}_{uuid.uuid4().hex[:8]}.json"
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return path


def _write_csv_artifact(
    prefix: str,
    rows: list[dict[str, Any]],
) -> Path:
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = _output_dir() / f"{prefix}_{stamp}_{uuid.uuid4().hex[:8]}.csv"
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _error_response(code: str, message: str, **detail: Any) -> dict[str, Any]:
    return {
        "ok": False,
        "errorCode": code,
        "error": message,
        **detail,
    }


def _success_response(**payload: Any) -> dict[str, Any]:
    return {"ok": True, **payload}


def _env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _coalesce(value: Any, *env_names: str) -> Any:
    if value not in (None, ""):
        return value
    for env_name in env_names:
        env_value = _env_value(env_name)
        if env_value is not None:
            return env_value
    return value


def _coalesce_int(value: int | None, *env_names: str) -> int | None:
    if value is not None:
        return value
    for env_name in env_names:
        env_value = _env_value(env_name)
        if env_value is None:
            continue
        try:
            return int(env_value)
        except ValueError:
            return None
    return None


def _coalesce_bool(value: bool, *env_names: str) -> bool:
    if value:
        return value
    for env_name in env_names:
        env_value = _env_value(env_name)
        if env_value is None:
            continue
        return env_value.lower() in {"1", "true", "yes", "y", "on"}
    return value


def validate_sql(
    sql: str | None,
    *,
    engine_kind: str,
    require_limit: bool = False,
) -> dict[str, Any]:
    kind = normalize_engine_kind(engine_kind)
    return validate_sql_for_profile(
        sql,
        profile=profile_for_kind(kind),
        require_limit=require_limit,
    )


def _load_chroma_metadata_rows(
    *,
    question: str,
    top_k: int,
    host: str,
    port: int | None,
    ssl: bool,
    auth_token: str | None,
    tenant: str | None,
    database: str | None,
    collection: str,
) -> list[dict[str, Any]]:
    try:
        import chromadb
    except ImportError as exc:
        raise RuntimeError("chromadb dependency is not installed") from exc

    kwargs: dict[str, Any] = {
        "host": host,
        "ssl": ssl,
    }
    if port is not None:
        kwargs["port"] = port
    if tenant:
        kwargs["tenant"] = tenant
    if database:
        kwargs["database"] = database
    if auth_token:
        kwargs["headers"] = {"Authorization": f"Bearer {auth_token}"}
    client = chromadb.HttpClient(**kwargs)
    chroma_collection = client.get_collection(collection)
    result = chroma_collection.query(
        query_texts=[question or ""],
        n_results=top_k,
        include=["metadatas", "documents"],
    )

    metadatas = (result.get("metadatas") or [[]])[0] or []
    documents = (result.get("documents") or [[]])[0] or []
    rows: list[dict[str, Any]] = []
    for idx, metadata in enumerate(metadatas):
        row = dict(metadata or {})
        document = documents[idx] if idx < len(documents) else None
        if isinstance(document, str) and document.strip():
            raw_doc = document.strip()
            try:
                doc_value = json.loads(raw_doc)
            except json.JSONDecodeError:
                row.setdefault("description", raw_doc)
            else:
                if isinstance(doc_value, dict):
                    row.update({key: value for key, value in doc_value.items() if key not in row})
                else:
                    row.setdefault("description", raw_doc)
        rows.append(row)
    return rows


STARROCKS_VECTOR_TABLE = "data_ai_rag.rag_documents"
STARROCKS_TABLE_METADATA_COLLECTION_ID = 1


def _embed_query_text(
    *,
    text: str,
    model: str,
    base_url: str,
    api_key: str,
) -> list[float]:
    endpoint = base_url.rstrip("/") + "/embeddings"
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({"model": model, "input": text}, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8"))
    embedding = ((payload.get("data") or [{}])[0] or {}).get("embedding")
    if not isinstance(embedding, list):
        raise RuntimeError("embedding response does not contain data[0].embedding")
    return [float(item) for item in embedding]


def _extract_create_statement_from_content(content: str) -> str:
    marker = "建表语句:"
    if marker not in content:
        return content.strip() if content.strip().upper().startswith("CREATE TABLE") else ""
    ddl = content.split(marker, 1)[1].strip()
    for next_marker in ("\n\n上游表:", "\n\n下游表:", "\n\nINSERT 语句:", "\n\n描述:"):
        if next_marker in ddl:
            ddl = ddl.split(next_marker, 1)[0].strip()
    return ddl


def _load_starrocks_vector_metadata_rows(
    *,
    question: str,
    top_k: int,
    dsn: str,
    embedding_model: str,
    embedding_base_url: str,
    embedding_api_key: str,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(top_k), 50))
    query_embedding = _embed_query_text(
        text=question,
        model=embedding_model,
        base_url=embedding_base_url,
        api_key=embedding_api_key,
    )
    query_embedding_json = json.dumps(query_embedding)
    sql = f"""
SELECT
  doc_id,
  content,
  metadata_json,
  approx_cosine_similarity(
    embedding,
    CAST(PARSE_JSON(:query_embedding) AS ARRAY<FLOAT>)
  ) AS similarity
FROM {STARROCKS_VECTOR_TABLE}
WHERE collection_id = :collection_id
ORDER BY similarity DESC
LIMIT {limit}
"""
    engine = create_engine(normalize_dsn(dsn), pool_pre_ping=True, poolclass=NullPool)
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(sql),
                {
                    "collection_id": STARROCKS_TABLE_METADATA_COLLECTION_ID,
                    "query_embedding": query_embedding_json,
                },
            )
            rows: list[dict[str, Any]] = []
            for row in result:
                item = dict(row._mapping)
                content = str(item.get("content") or "")
                raw_metadata = item.get("metadata_json") or "{}"
                try:
                    metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else dict(raw_metadata)
                except (TypeError, json.JSONDecodeError, ValueError):
                    metadata = {}
                rows.append(
                    {
                        "table_name": metadata.get("name") or item.get("doc_id") or "",
                        "description": metadata.get("description") or content,
                        "ddl": (
                            metadata.get("create_statement")
                            or metadata.get("create_table_statement")
                            or _extract_create_statement_from_content(content)
                        ),
                    }
                )
            return rows
    finally:
        engine.dispose()


def _row_table_name(row: dict[str, Any]) -> str:
    return str(
        row.get("table")
        or row.get("tableName")
        or row.get("table_name")
        or row.get("name")
        or row.get("metadata_name")
        or ""
    ).strip()


def _row_ddl(row: dict[str, Any]) -> str:
    return str(
        row.get("ddl")
        or row.get("create_sql")
        or row.get("createSql")
        or row.get("schema")
        or ""
    ).strip()


def _make_engine(engine_kind: str, dsn: str):
    return create_engine_from_config(
        EngineConfig(kind=normalize_engine_kind(engine_kind), dsn=normalize_dsn(dsn))
    )


def _introspect_tables(
    candidates: list[str] | None = None,
    *,
    engine_kind: str,
    dsn: str,
) -> list[dict[str, str]]:
    engine = _make_engine(engine_kind, dsn)
    try:
        return engine.list_tables_meta(candidates)
    finally:
        engine.close()


def run_data_exploration(
    payload: dict[str, Any],
    *,
    vector_provider: str | None = None,
    vector_host: str | None = None,
    vector_port: int | None = None,
    vector_ssl: bool = False,
    vector_auth_token: str | None = None,
    vector_tenant: str | None = None,
    vector_database: str | None = None,
    vector_collection: str | None = None,
    vector_dsn: str | None = None,
    embedding_model: str | None = None,
    embedding_base_url: str | None = None,
    embedding_api_key: str | None = None,
    embedding_dim: int | None = None,
    engine_kind: str | None = None,
    dsn: str | None = None,
) -> dict[str, Any]:
    question = str(payload.get("question") or payload.get("businessQuestion") or "").strip()
    top_k = int(payload.get("topK") or payload.get("maxTables") or 5)
    vector_provider = _coalesce(vector_provider, "VECTOR_STORE_PROVIDER", "RAG_VECTORSTORE_TYPE")
    vector_host = _coalesce(vector_host, "VECTOR_STORE_HOST")
    vector_port = _coalesce_int(vector_port, "VECTOR_STORE_PORT")
    vector_ssl = _coalesce_bool(vector_ssl, "VECTOR_STORE_SSL")
    vector_auth_token = _coalesce(vector_auth_token, "VECTOR_STORE_AUTH_TOKEN")
    vector_tenant = _coalesce(vector_tenant, "VECTOR_STORE_TENANT")
    vector_database = _coalesce(vector_database, "VECTOR_STORE_DATABASE")
    vector_collection = _coalesce(vector_collection, "VECTOR_STORE_COLLECTION")
    vector_dsn = _coalesce(vector_dsn, "VECTOR_STORE_STARROCKS_DSN", "RAG_STARROCKS_URL")
    embedding_model = _coalesce(embedding_model, "VECTOR_STORE_EMBEDDING_MODEL")
    embedding_base_url = _coalesce(embedding_base_url, "VECTOR_STORE_EMBEDDING_BASE_URL")
    embedding_api_key = _coalesce(embedding_api_key, "VECTOR_STORE_EMBEDDING_API_KEY")
    embedding_dim = _coalesce_int(embedding_dim, "VECTOR_STORE_EMBEDDING_DIM")
    candidates = [
        str(item).strip()
        for item in payload.get("candidateTables", []) or []
        if str(item).strip()
    ]

    rows: list[dict[str, Any]] = []
    source = ""
    if vector_provider:
        if vector_provider not in ("chroma", "starrocks"):
            return _error_response(
                "UnsupportedVectorStore",
                "only chroma and starrocks vector stores are supported",
                provider=vector_provider,
            )
        if vector_provider == "chroma" and (not vector_host or not vector_collection):
            return _error_response(
                "MissingVectorStoreConfig",
                "vector host and collection are required for vector-backed data_exploration",
            )
        source = "vector_store"
        if vector_provider == "chroma":
            rows = _load_chroma_metadata_rows(
                question=question,
                top_k=top_k,
                host=str(vector_host),
                port=vector_port,
                ssl=vector_ssl,
                auth_token=vector_auth_token,
                tenant=vector_tenant,
                database=vector_database,
                collection=str(vector_collection),
            )
        else:
            if not vector_dsn:
                return _error_response(
                    "MissingVectorStoreConfig",
                    "vector dsn is required for StarRocks-backed data_exploration",
                )
            if not embedding_model or not embedding_base_url or not embedding_api_key:
                return _error_response(
                    "MissingEmbeddingConfig",
                    "embedding model, base url, and api key are required for StarRocks vector search",
                    required=[
                        "VECTOR_STORE_EMBEDDING_MODEL",
                        "VECTOR_STORE_EMBEDDING_BASE_URL",
                        "VECTOR_STORE_EMBEDDING_API_KEY",
                    ],
                )
            rows = _load_starrocks_vector_metadata_rows(
                question=question,
                top_k=top_k,
                dsn=vector_dsn,
                embedding_model=embedding_model,
                embedding_base_url=embedding_base_url,
                embedding_api_key=embedding_api_key,
            )
    elif dsn:
        if not engine_kind:
            return _error_response(
                "EngineConfigError",
                "engine kind is required when data_exploration uses SQL introspection",
            )
        source = "sql_introspection"
        rows = _introspect_tables(candidates, engine_kind=engine_kind, dsn=dsn)
    else:
        return _error_response(
            "MissingMetadataSource",
            "vector store config or the selected SQL engine DSN is required for data_exploration",
        )

    tables = []
    for row in rows:
        ddl = _row_ddl(row)
        table = _row_table_name(row)
        if not ddl:
            continue
        tables.append(
            {
                "table": table,
                "ddl": ddl,
                "description": str(row.get("description") or row.get("comment") or ""),
            }
        )
    return _success_response(
        source=source,
        question=question,
        tableCount=len(tables),
        tables=tables,
    )


def run_sql_analysis(
    payload: dict[str, Any],
    *,
    engine_kind: str,
    dsn: str,
) -> dict[str, Any]:
    sql = str(payload.get("sql") or "").strip()
    forbidden_engine_fields = sorted(
        field for field in ("engineKind", "engine_kind", "dialect") if field in payload
    )
    if forbidden_engine_fields:
        return _error_response(
            "EngineSelectionNotAllowed",
            "SQL engine selection must be passed as --engine-kind, not command payload",
            fields=forbidden_engine_fields,
        )
    require_limit = bool(payload.get("requireLimit") or payload.get("require_limit"))
    engine = _make_engine(engine_kind, dsn)
    profile = engine.get_profile()
    validation = validate_sql_for_profile(sql, profile=profile, require_limit=require_limit)
    if not validation["passed"]:
        engine.close()
        return _error_response(
            "ReadOnlySqlRequired",
            "SQL validation failed",
            validation=validation,
        )

    started = perf_counter()
    try:
        rows = engine.execute_select(str(validation["validatedSql"] or sql))
    except Exception as exc:
        return _error_response(
            "SqlExecutionFailed",
            f"{type(exc).__name__}: {exc}",
            validation=validation,
        )
    finally:
        engine.close()
    elapsed_ms = round((perf_counter() - started) * 1000, 3)
    artifact = {
        "sql": validation["validatedSql"],
        "rows": rows,
        "rowCount": len(rows),
        "elapsedMs": elapsed_ms,
        "validation": validation,
    }
    result_path = _write_json_artifact("sql_result", artifact)
    csv_path = _write_csv_artifact("sql_result", rows) if rows else None
    return _success_response(
        sql=validation["validatedSql"],
        rowCount=len(rows),
        resultFile=str(result_path),
        csvFile=str(csv_path) if csv_path else None,
        elapsedMs=elapsed_ms,
        warnings=validation.get("warnings") or [],
    )


def run_answer_result(
    payload: dict[str, Any],
) -> dict[str, Any]:
    question = str(payload.get("question") or "").strip()
    result_file = str(payload.get("resultFile") or payload.get("result_file") or "").strip()
    if not result_file:
        return _error_response("MissingResultFile", "resultFile is required")
    result_path = Path(result_file)
    if not result_path.is_file():
        return _error_response("ResultFileNotFound", f"result file not found: {result_file}")
    result = _read_json_file(result_path)
    sql = str(payload.get("sql") or result.get("sql") or "").strip()
    row_count = int(result.get("rowCount") or len(result.get("rows") or []))
    scope_notes = payload.get("resultScopeNotes") or payload.get("result_scope_notes") or []
    warnings = payload.get("warnings") or (result.get("validation") or {}).get("warnings") or []
    lines = [
        "【TL;DR】",
        f"本次查询返回 {row_count} 行结果，完整结果已写入文件：`{result_path}`。",
    ]
    if question:
        lines.append(f"用户问题：{question}")
    if sql:
        lines.extend(["", "【SQL】", "```sql", sql, "```"])
    if scope_notes:
        lines.extend(["", "【口径】"])
        lines.extend(f"- {item}" for item in scope_notes)
    if warnings:
        lines.extend(["", "【限制与风险】"])
        lines.extend(f"- {item}" for item in warnings)
    reply = "\n".join(lines).strip()
    answer_path = _write_json_artifact(
        "answer_result",
        {
            "question": question,
            "reply": reply,
            "resultFile": str(result_path),
            "rowCount": row_count,
        },
    )
    return _success_response(
        replyFile=str(answer_path),
        resultFile=str(result_path),
        rowCount=row_count,
    )


def _add_input_command(subparsers: Any, name: str, help_text: str) -> Any:
    command = subparsers.add_parser(name, help=help_text)
    command.add_argument("--input", help="Path to a JSON payload file. Reads stdin when omitted.")
    return command


def _add_engine_args(
    command: Any,
    *,
    require_engine_kind: bool,
    include_dsn: bool,
    require_dsn: bool = False,
) -> None:
    command.add_argument(
        "--engine-kind",
        required=require_engine_kind,
        choices=["mysql", "starrocks", "trino"] if require_engine_kind else None,
        help="SQL engine kind. Pass \"$SQL_ENGINE_KIND\" from the host shell.",
    )
    if include_dsn:
        command.add_argument(
            "--dsn",
            required=require_dsn,
            default=None,
            help=(
                "SQLAlchemy-style SQL DSN. Pass the selected engine DSN placeholder, "
                "for example \"$SQL_ENGINE_MYSQL_DSN\", \"$SQL_ENGINE_STARROCKS_DSN\", or \"$SQL_ENGINE_TRINO_DSN\"."
            ),
        )


def _payload_with_overrides(input_path: str | None, **overrides: Any) -> dict[str, Any]:
    payload = _read_payload(input_path)
    for key, value in overrides.items():
        if value is not None:
            payload[key] = value
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Data Analysis skill safety CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate_parser = subparsers.add_parser("validate_sql", help="Validate one read-only SQL statement")
    validate_parser.add_argument("--sql", required=True)
    _add_engine_args(validate_parser, require_engine_kind=True, include_dsn=False)
    validate_parser.add_argument("--require-limit", action="store_true")
    data_parser = _add_input_command(subparsers, "data_exploration", "Find relevant table DDL from vector store or SQL introspection")
    data_parser.add_argument("--vector-provider", choices=["chroma", "starrocks"], default=None)
    data_parser.add_argument("--vector-host", default=None)
    data_parser.add_argument("--vector-port", type=int, default=None)
    data_parser.add_argument("--vector-ssl", action="store_true")
    data_parser.add_argument("--vector-auth-token", default=None)
    data_parser.add_argument("--vector-tenant", default=None)
    data_parser.add_argument("--vector-database", default=None)
    data_parser.add_argument("--vector-collection", default=None)
    data_parser.add_argument("--vector-dsn", default=None)
    data_parser.add_argument("--embedding-model", default=None)
    data_parser.add_argument("--embedding-base-url", default=None)
    data_parser.add_argument("--embedding-api-key", default=None)
    data_parser.add_argument("--embedding-dim", type=int, default=None)
    _add_engine_args(data_parser, require_engine_kind=False, include_dsn=True)
    sql_parser = _add_input_command(subparsers, "sql_analysis", "Validate and execute read-only SQL, writing rows to a result file")
    sql_parser.add_argument("--sql", help="Read-only SELECT/WITH SQL to validate and execute.")
    _add_engine_args(sql_parser, require_engine_kind=True, include_dsn=True, require_dsn=True)
    sql_parser.add_argument("--require-limit", action="store_true")
    answer_parser = _add_input_command(subparsers, "answer_result", "Create a user-facing answer artifact from a SQL result file")

    args = parser.parse_args(argv)
    if args.command == "validate_sql":
        try:
            return _json_response(
                validate_sql(
                    args.sql,
                    engine_kind=args.engine_kind,
                    require_limit=args.require_limit,
                )
            )
        except Exception as exc:
            return _json_response(
                _error_response("EngineConfigError", f"{type(exc).__name__}: {exc}"),
                exit_code=1,
            )
    if args.command == "data_exploration":
        try:
            return _json_response(
                run_data_exploration(
                    _read_payload(args.input),
                    vector_provider=args.vector_provider,
                    vector_host=args.vector_host,
                    vector_port=args.vector_port,
                    vector_ssl=args.vector_ssl,
                    vector_auth_token=args.vector_auth_token,
                    vector_tenant=args.vector_tenant,
                    vector_database=args.vector_database,
                    vector_collection=args.vector_collection,
                    vector_dsn=args.vector_dsn,
                    embedding_model=args.embedding_model,
                    embedding_base_url=args.embedding_base_url,
                    embedding_api_key=args.embedding_api_key,
                    embedding_dim=args.embedding_dim,
                    engine_kind=args.engine_kind,
                    dsn=args.dsn,
                )
            )
        except Exception as exc:
            return _json_response(
                _error_response("DataExplorationFailed", f"{type(exc).__name__}: {exc}"),
                exit_code=1,
            )
    if args.command == "sql_analysis":
        try:
            payload_input = _payload_with_overrides(
                args.input,
                sql=args.sql,
                requireLimit=True if args.require_limit else None,
            )
            if not str(payload_input.get("sql") or "").strip():
                return _json_response(
                    _error_response(
                        "MissingRequiredArgument",
                        "sql_analysis requires --sql or input payload field: sql",
                    ),
                    exit_code=1,
                )
            payload = run_sql_analysis(
                payload_input,
                engine_kind=args.engine_kind,
                dsn=args.dsn,
            )
            return _json_response(payload, exit_code=0 if payload.get("ok") else 1)
        except Exception as exc:
            return _json_response(
                _error_response("SqlAnalysisFailed", f"{type(exc).__name__}: {exc}"),
                exit_code=1,
            )
    if args.command == "answer_result":
        try:
            payload = run_answer_result(_read_payload(args.input))
            return _json_response(payload, exit_code=0 if payload.get("ok") else 1)
        except Exception as exc:
            return _json_response(
                _error_response("AnswerResultFailed", f"{type(exc).__name__}: {exc}"),
                exit_code=1,
            )
    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
