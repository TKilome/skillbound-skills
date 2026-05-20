# Command Map Index

Fast intent-to-command routing for common data-analysis requests.

## CLI Entry

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" <command> [args...]
```

## Environment

Environment variables and conditional groups are declared in `../SKILL.md`.

Result artifacts are always written under `scripts/output/`.

The Python script does not read these variables directly. Use them as Bash
placeholders in explicit CLI arguments, for example
`--engine-kind "$SQL_ENGINE_KIND"` and the matching engine DSN:
`--dsn "$SQL_ENGINE_MYSQL_DSN"`, `--dsn "$SQL_ENGINE_STARROCKS_DSN"`, or `--dsn "$SQL_ENGINE_TRINO_DSN"`.

SQL DSN selection is deterministic:

| `SQL_ENGINE_KIND` | Use this `--dsn` placeholder |
| --- | --- |
| `mysql` | `--dsn "$SQL_ENGINE_MYSQL_DSN"` |
| `starrocks` | `--dsn "$SQL_ENGINE_STARROCKS_DSN"` |
| `trino` | `--dsn "$SQL_ENGINE_TRINO_DSN"` |

Never pass a generic DSN variable and never choose the DSN from the payload.

## High-Frequency Routing Table

| User Intent | Command | Safety |
| --- | --- | --- |
| "有什么表/字段/DDL" | `data_exploration --input <payload.json> <matching VECTOR_STORE_* args>` | Read |
| "找订单/用户/课程相关表" | `data_exploration --input <payload.json> <matching VECTOR_STORE_* args>` | Read |
| "校验 SQL" | `validate_sql --sql <sql> --engine-kind "$SQL_ENGINE_KIND"` | Read |
| "执行这条 SELECT/WITH" | `sql_analysis --sql <sql> --engine-kind "$SQL_ENGINE_KIND" --dsn "<matching SQL_ENGINE_*_DSN placeholder>"` | Read |
| "运行 SQL 并给我结果文件" | `sql_analysis --input <payload.json> --engine-kind "$SQL_ENGINE_KIND" --dsn "<matching SQL_ENGINE_*_DSN placeholder>"` | Read |
| "基于结果文件总结" | `answer_result --input <payload.json>` | Read |

For SQL commands, choose the exact DSN placeholder from the DSN selection table above.
For vector metadata commands, choose Chroma arguments when
`VECTOR_STORE_PROVIDER=chroma`, or StarRocks vector DSN plus embedding arguments
when `VECTOR_STORE_PROVIDER=starrocks`; do not mix provider-specific arguments.

## Disambiguation Rules

- Hard non-trigger cues are checked first. If matched, do not trigger this skill.
- Mutation cues are out of scope: `insert`, `update`, `delete`, `drop`, `alter`,
  `create table`, `truncate`, `merge`, `grant`, `revoke`, `修数据`, `补数据`,
  `建表`, `删表`.
- Cloud/Flink operational requests are out of scope and must not be routed here.
- For ambiguous business data requests, run `data_exploration` first when a
  vector store or selected engine DSN exists; otherwise ask for configuration.
- For SQL execution, run `validate_sql` or `sql_analysis`; never answer SQL
  safety by reasoning alone.
- `sql_analysis` is the only command that requires a SQL argument. If missing,
  return the CLI error and ask only for SQL.
- `data_exploration` must not require SQL text.
- `answer_result` must not require SQL text; it requires `resultFile`.

## Mandatory First-Call Rules

- For table/field/DDL discovery, first call `data_exploration`.
- For SQL validation requests with SQL text, first call `validate_sql`.
- For SQL execution requests with SQL text, first call `sql_analysis`; it
  validates before execution internally.
- For SQL execution requests without SQL text, do not fabricate SQL. Ask for the
  SQL or first perform `data_exploration` if the user is asking for analysis
  rather than execution of an existing SQL statement.
- For answer requests with a result file, first call `answer_result`.
- Do not call `--help` as the first action when a command is clear.

## Safety Legend

- Read: execute directly.
- Mutation: out of scope. Return boundary guidance.
- Destructive: out of scope. Return boundary guidance.

## Credential Safety

Never output SQL DSNs, passwords, tokens, access keys, or resolved environment
variable values. Commands should show placeholder names, not secret values.
