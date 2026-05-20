# Command Catalog

This skill owns the execution boundary. Codex, Claude Code, and similar agents
must call the bundled CLI instead of connecting to databases directly.

Resolve `SKILL_DIR` as the directory containing `SKILL.md`, then run:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" <command> [args]
```

## Environment

Environment variables and conditional groups are declared in `../SKILL.md`.
Pass configured values as explicit CLI arguments; the Python script does not
read environment variables directly.

Result artifacts are always written under `scripts/output/`.

Never pass credentials or engine selection in command payload JSON. Keep
credentials in environment variables or runtime-managed secret injection, and
reference them only through Bash placeholders in CLI arguments.

Choose the DSN placeholder that matches `SQL_ENGINE_KIND`; do not pass all DSNs
to a single command.

## Execution Lifecycle

The CLI is designed for short-lived agent calls. SQLAlchemy connections use
`NullPool`, so each process opens only the connection it needs and releases it
after command completion. Concurrency control, query admission, and cleanup of
old `scripts/output/` artifacts remain host/runtime responsibilities.

## data_exploration

Find relevant table DDL for a user question.

Chroma-backed metadata:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" data_exploration \
  --input /tmp/data_exploration.json \
  --vector-provider "$VECTOR_STORE_PROVIDER" \
  --vector-host "$VECTOR_STORE_HOST" \
  --vector-port "$VECTOR_STORE_PORT" \
  --vector-collection "$VECTOR_STORE_COLLECTION"
```

Add `--vector-ssl` only when `VECTOR_STORE_SSL=true`. Add
`--vector-tenant "$VECTOR_STORE_TENANT"`, `--vector-database
"$VECTOR_STORE_DATABASE"`, and `--vector-auth-token "$VECTOR_STORE_AUTH_TOKEN"`
only when those values are configured. If no vector store is configured, use SQL
introspection with `--engine-kind "$SQL_ENGINE_KIND"` and the selected engine
DSN.

StarRocks-backed metadata:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" data_exploration \
  --input /tmp/data_exploration.json \
  --vector-provider "$VECTOR_STORE_PROVIDER" \
  --vector-dsn "$VECTOR_STORE_STARROCKS_DSN" \
  --embedding-model "$VECTOR_STORE_EMBEDDING_MODEL" \
  --embedding-base-url "$VECTOR_STORE_EMBEDDING_BASE_URL" \
  --embedding-api-key "$VECTOR_STORE_EMBEDDING_API_KEY"
```

Do not use `VECTOR_STORE_STARROCKS_DSN` as the SQL execution DSN. SQL execution
uses `SQL_ENGINE_STARROCKS_DSN`; vector-backed metadata lookup uses
`VECTOR_STORE_STARROCKS_DSN`. StarRocks-backed metadata lookup follows data-ai's
fixed layout: table `data_ai_rag.rag_documents` and `collection_id=1` for table
metadata. It generates a query embedding with the configured OpenAI-compatible
embedding endpoint and orders candidates by `approx_cosine_similarity`.

The CLI also reads the same values from environment variables when explicit
arguments are omitted: `VECTOR_STORE_PROVIDER`, `VECTOR_STORE_STARROCKS_DSN`,
`VECTOR_STORE_EMBEDDING_MODEL`, `VECTOR_STORE_EMBEDDING_BASE_URL`, and
`VECTOR_STORE_EMBEDDING_API_KEY`.

Input:

```json
{
  "question": "统计最近 7 天订单 GMV",
  "candidateTables": ["orders"],
  "topK": 5
}
```

Output includes `ok`, `source`, `tableCount`, and `tables[].ddl`. It does not
generate SQL and does not produce a business conclusion.

## validate_sql

Validate one generated or user-provided SQL statement before execution.

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" validate_sql \
  --sql "SELECT * FROM orders WHERE dt = '2026-05-18' LIMIT 100" \
  --engine-kind "$SQL_ENGINE_KIND"
```

Output JSON:

- `passed`: boolean.
- `errors`: blocking errors.
- `warnings`: non-blocking warnings to mention in the answer.
- `validatedSql`: cleaned SQL with trailing semicolon removed.
- `diagnostics`: engine, dialect, SQL length, parser used, and parse timing.

If `passed` is false, do not invoke `sql_analysis`.

## sql_analysis

Validate and execute one read-only SQL statement. The command writes rows to an
artifact file and returns only metadata and file paths.

Choose the `--dsn` placeholder by `SQL_ENGINE_KIND`:

| `SQL_ENGINE_KIND` | `--dsn` |
| --- | --- |
| `mysql` | `"$SQL_ENGINE_MYSQL_DSN"` |
| `starrocks` | `"$SQL_ENGINE_STARROCKS_DSN"` |
| `trino` | `"$SQL_ENGINE_TRINO_DSN"` |

MySQL:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" sql_analysis \
  --input /tmp/sql_analysis.json \
  --engine-kind "$SQL_ENGINE_KIND" \
  --dsn "$SQL_ENGINE_MYSQL_DSN"
```

StarRocks:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" sql_analysis \
  --input /tmp/sql_analysis.json \
  --engine-kind "$SQL_ENGINE_KIND" \
  --dsn "$SQL_ENGINE_STARROCKS_DSN"
```

Trino:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" sql_analysis \
  --input /tmp/sql_analysis.json \
  --engine-kind "$SQL_ENGINE_KIND" \
  --dsn "$SQL_ENGINE_TRINO_DSN"
```

Input:

```json
{
  "sql": "SELECT COUNT(*) AS order_count, SUM(amount) AS gmv FROM orders LIMIT 100",
  "requireLimit": true
}
```

Output includes `ok`, `sql`, `rowCount`, `resultFile`, optional `csvFile`,
`elapsedMs`, and `warnings`. It must not include `rows`.

## answer_result

Create a user-facing answer artifact from a SQL result file.

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" answer_result \
  --input /tmp/answer_result.json
```

Input:

```json
{
  "question": "最近 7 天订单 GMV 是多少？",
  "resultFile": "/tmp/sql_result.json",
  "resultScopeNotes": ["按订单创建时间过滤最近 7 天"]
}
```

Output includes `ok`, `replyFile`, `resultFile`, and `rowCount`.

## Error Codes

- `MissingMetadataSource`: neither vector store config nor the selected engine DSN
  is configured.
- `ReadOnlySqlRequired`: SQL is not a single read-only `SELECT` or `WITH`.
- `SqlExecutionFailed`: SQL execution failed after validation.
- `MissingResultFile`: `answer_result` was called without a result file.
- `ResultFileNotFound`: the result file path does not exist.
- `DataExplorationFailed`, `SqlAnalysisFailed`, `AnswerResultFailed`: command
  wrapper failures.
