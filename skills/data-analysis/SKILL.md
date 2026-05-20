---
name: data-analysis
description: Use when the user asks for intelligent data retrieval, BI-style analysis, table upload analysis, data requirement clarification, business metric exploration, or read-only SQL generation/execution against configured SQL engines such as MySQL, StarRocks, or Trino. Covers Chinese and English requests for "取数", "数据分析", "SQL分析", "报表", "明细", "汇总", "指标", "口径", and multi-turn analysis conversations.
license: Apache-2.0
compatibility: Requires Python 3.10+ and dependencies installed in the skill-local venv. SQL execution uses SQL_ENGINE_KIND plus the matching SQL_ENGINE_MYSQL_DSN, SQL_ENGINE_STARROCKS_DSN, or SQL_ENGINE_TRINO_DSN. Metadata exploration uses VECTOR_STORE_* config or falls back to SQL introspection.
envs:
  - env: SKILL_DIR
    description: Directory containing this SKILL.md, used to locate bundled references and scripts. Example /path/to/skills/data-analysis.

  - env: SQL_ENGINE_KIND
    description: SQL execution engine selected by the host runtime. Must be one of mysql, starrocks, or trino. Example mysql.

  - env: VECTOR_STORE_PROVIDER
    description: Vector database provider for table metadata retrieval. Must be chroma or starrocks. Example starrocks.
env_groups:
  - name: sql_engine_mysql
    when:
      env: SQL_ENGINE_KIND
      equals: mysql
    envs:
      - env: SQL_ENGINE_MYSQL_DSN
        description: SQLAlchemy MySQL DSN. Example mysql+pymysql://readonly_user:password@mysql.example.com:3306/analytics.

  - name: sql_engine_starrocks
    when:
      env: SQL_ENGINE_KIND
      equals: starrocks
    envs:
      - env: SQL_ENGINE_STARROCKS_DSN
        description: SQLAlchemy StarRocks DSN. StarRocks uses the MySQL-compatible execution driver. Example mysql+pymysql://readonly_user:password@starrocks-fe.example.com:9030/analytics.

  - name: sql_engine_trino
    when:
      env: SQL_ENGINE_KIND
      equals: trino
    envs:
      - env: SQL_ENGINE_TRINO_DSN
        description: SQLAlchemy Trino DSN. Example trino://readonly_user@trino.example.com:8080/hive/analytics.

  - name: vector_chroma
    when:
      env: VECTOR_STORE_PROVIDER
      equals: chroma
    envs:
      - env: VECTOR_STORE_HOST
        description: Chroma host. Example chroma.example.com.
      - env: VECTOR_STORE_PORT
        description: Chroma HTTP port. Example 8000.
      - env: VECTOR_STORE_SSL
        description: Whether to use HTTPS/TLS. Set to true to add --vector-ssl; omit the flag for false. Example true.
      - env: VECTOR_STORE_COLLECTION
        description: Chroma collection containing table metadata records. Example table_catalog.
      - env: VECTOR_STORE_TENANT
        description: Optional Chroma tenant. Example default_tenant.
      - env: VECTOR_STORE_DATABASE
        description: Optional Chroma database. Example default_database.
      - env: VECTOR_STORE_AUTH_TOKEN
        description: Optional bearer token for Chroma access. Example chroma_token_placeholder.

  - name: vector_starrocks
    when:
      env: VECTOR_STORE_PROVIDER
      equals: starrocks
    envs:
      - env: VECTOR_STORE_STARROCKS_DSN
        description: SQLAlchemy StarRocks DSN for table metadata retrieval. The DSN should allow reading data-ai's data_ai_rag.rag_documents table. Example mysql+pymysql://readonly_user:password@starrocks-fe.example.com:9030/data_ai_rag.
      - env: VECTOR_STORE_EMBEDDING_MODEL
        description: OpenAI-compatible embedding model used for StarRocks vector search. Example text-embedding-v3.
      - env: VECTOR_STORE_EMBEDDING_BASE_URL
        description: OpenAI-compatible embedding base URL. Example https://dashscope.aliyuncs.com/compatible-mode/v1.
      - env: VECTOR_STORE_EMBEDDING_API_KEY
        description: Embedding API key. Never print the resolved value.
      - env: VECTOR_STORE_EMBEDDING_DIM
        description: Optional embedding dimension documentation value. Example 1024.
metadata:
  domain: data
  owner: data-ai-team
  allowed-tools: Read Bash
---

# Data Analysis

Give a host agent Data AI-style intelligent retrieval and analysis capability.

**MANDATORY EXECUTION RULE**: When this skill is triggered, execute data work
only through this skill's bundled CLI commands and their command-specific
parameter validation.

Do not bypass the skill by using arbitrary database CLIs, direct ad hoc
connections, shell scripts, notebooks, application source code, or unchecked SQL
execution. If the CLI cannot perform the requested step, return the missing
capability and the next required input instead of inventing results.

The bundled CLI is:

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py"
```

Use it for data exploration, SQL validation, SQL execution, and answer artifact
generation. Pass runtime configuration as explicit CLI arguments using
environment-variable placeholders such as `--engine-kind "$SQL_ENGINE_KIND"`
and a selected engine DSN placeholder such as `--dsn "$SQL_ENGINE_MYSQL_DSN"`,
`--dsn "$SQL_ENGINE_STARROCKS_DSN"`, or `--dsn "$SQL_ENGINE_TRINO_DSN"`. Do not pass credentials
through chat payloads.

## Scope & Boundaries

**In scope**: data requirement clarification, metadata/DDL exploration,
read-only SQL validation, read-only SQL execution, result artifact creation, and
answer artifact creation.

**Out of scope (do NOT handle)**:

- DDL/DML/DCL, migrations, backfills, ETL deployment, stored procedure
  execution, or data repair.
- Credential discovery, password handling, or connection-string construction
  from secrets pasted into chat.
- Unrestricted data export or bulk PII extraction.
- BI dashboard editing, frontend work, Flink operations, or cloud resource
  management.

### Boundary Response

For mutation or data-repair requests:

> "This request is outside the scope of `data-analysis`. This skill only handles
> metadata exploration, read-only SQL validation/execution, and answer artifacts.
> It does not modify database schemas, repair data, run ETL, or perform
> DDL/DML."

For credential requests:

> "This skill cannot inspect, print, or construct credentials. Configure
> `SQL_ENGINE_KIND`, the matching engine DSN, and optional vector store environment
> variables outside the chat payload."

## Trigger Conditions

Trigger this skill when the request contains one or more:

1. Data retrieval or analysis terms: `取数`, `数据分析`, `SQL分析`, `报表`,
   `明细`, `汇总`, `指标`, `口径`, `GMV`, `订单数`, `转化率`.
2. Table/metadata exploration terms: `有什么表`, `字段`, `DDL`, `表结构`,
   `可用数据`, `数据口径`.
3. Read-only SQL execution intent: generate/check/execute `SELECT` or `WITH`,
   run a query, export result file, or explain query output.

Do not trigger for unrelated "analysis" contexts such as code review, frontend
layout analysis, Flink job operations, generic cloud infrastructure, or database
mutation tasks.

## Scope

Supported capabilities:

- Clarify ambiguous data requirements before SQL analysis.
- Explore available business data scopes, fields, metrics, time fields, and
  candidate tables without exposing raw table names as the primary user choice.
- Execute one agent-generated read-only SQL statement after the requirement and
  relevant schema context are clear.
- Explain result artifacts, assumptions, SQL used, row limits, and caveats.

Out of scope:

- DDL, DML, DCL, migrations, backfills, ETL deployment, stored procedure
  execution, or data repair.
- Credential discovery, password handling, or connection-string construction
  from secrets pasted into chat.
- Unbounded data export or unrestricted PII disclosure.
- Fabricating query results when execution fails or no engine is selected.

## Declared Tool Surface

Use these CLI commands only:

- `clarify_requirement`: classify the request, identify missing slots, and ask a
  focused clarification question; this is an agent-side reasoning step.
- `data_exploration`: discover relevant table DDL from
  the configured vector store, or from SQL introspection when no vector store
  is configured.
- `validate_sql`: validate one read-only SQL statement before execution.
- `sql_analysis`: validate and execute one read-only SQL statement, then write
  rows to an artifact file. The command response must not include result rows.
- `answer_result`: create an answer artifact from a SQL result file.

## Command Quick Reference

| User Intent | Command | Type |
| --- | --- | --- |
| 查可用表/字段/DDL | `data_exploration --input <payload.json> <vector provider args>` | Read |
| 校验 SQL | `validate_sql --sql <sql> --engine-kind "$SQL_ENGINE_KIND"` | Read |
| 执行只读 SQL | `sql_analysis --sql <sql> --engine-kind "$SQL_ENGINE_KIND" --dsn "<matching SQL_ENGINE_*_DSN placeholder>"` | Read |
| 基于结果文件生成回答 | `answer_result --input <payload.json>` | Read |

For SQL execution, choose the DSN placeholder by `SQL_ENGINE_KIND`: mysql uses
`SQL_ENGINE_MYSQL_DSN`, starrocks uses `SQL_ENGINE_STARROCKS_DSN`, and trino
uses `SQL_ENGINE_TRINO_DSN`. For metadata exploration, choose Chroma or
StarRocks vector arguments by `VECTOR_STORE_PROVIDER`.

`sql_analysis` is the only command that requires `--sql` or input payload field
`sql`. Other commands should rely on their own CLI validation and should not
inherit SQL-specific checks.

## Operating Flow

1. Classify intent as `detail`, `summary`, `direct_answer`, `data_exploration`,
   `sql_analysis`, `answer_result`, or `boundary`.
2. If the request is ambiguous, follow
   `references/requirement-clarification.md`.
3. If the business scope is unclear, run lightweight `data_exploration` first.
   Use business language in the response; do not ask the user to choose raw table
   names.
4. Use environment-variable placeholders in the Bash command for runtime
   configuration. Never print their resolved values.
5. For SQL work, follow `references/sql-safety.md`. Only one `SELECT` or `WITH`
   statement is allowed. Validate the generated SQL with:

   ```bash
   "$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" validate_sql --sql "<SQL>" --engine-kind "$SQL_ENGINE_KIND"
   ```

6. Execute the query through `sql_analysis` only after required slots and
   relevant schema context are known. Use `--input <payload.json>` or stdin and
   pass `--engine-kind "$SQL_ENGINE_KIND"` plus the matching engine DSN:
   mysql -> `--dsn "$SQL_ENGINE_MYSQL_DSN"`, starrocks ->
   `--dsn "$SQL_ENGINE_STARROCKS_DSN"`, trino ->
   `--dsn "$SQL_ENGINE_TRINO_DSN"`.
7. Return result paths, row counts, assumptions, defaults, and warnings. Do not
   paste result rows into the chat unless the user explicitly asks for a small
   preview.

## Required Slots

For `detail` requests:

- `business_object`
- `time_field`
- `time_range`
- `output_fields`

Default only when the user allows it:

- `limit_sort`: sort by the selected time field descending and limit to 100
  rows.

For `summary` requests:

- `business_object`
- `time_field`
- `time_range`
- `metrics`
- `dimensions`

If only default slots are missing, continue and state the defaults in the final
answer. Do not silently choose a time field.

## Safety Rules

- Treat all SQL as unsafe until it passes validation.
- Allow only a single read-only `SELECT` or `WITH` statement.
- Reject `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `DROP`, `ALTER`, `CREATE`,
  `TRUNCATE`, `CALL`, `EXEC`, `GRANT`, `REVOKE`, `USE`, `SET`, multi-statements,
  shell escapes, and comments that hide extra statements.
- Add or preserve an explicit row limit for detail queries unless the user has
  permission for export and the runtime enforces an export boundary.
- Mask or avoid unnecessary PII in previews and natural-language summaries.
- Never reveal credentials, connection strings, API keys, or raw secret values.

## Resources

### Load After Trigger

- `references/command-map.md` — Intent-to-command routing and first-call rules.
- `references/agent-operating-protocol.md` — Execution flow, parameter strategy,
  and response rules.

### Load On Demand

- `references/requirement-clarification.md` — Read when the user request lacks
  business object, time field/range, metrics, dimensions, or output fields.
- `references/sql-safety.md` — Read before validating or executing SQL.
- `references/command-catalog.md` — Read for command syntax, payload examples,
  environment variables, and error codes.
- `references/error-handling.md` — Read when any CLI command returns `ok: false`
  or exits non-zero.
- `references/table-catalog.md` — Read when the user asks about known business
  table areas from the Data AI project.

## Assets

- `scripts/data_analysis_cli.py` — Main CLI entry.
- `requirement.txt` — Python dependencies for the skill-local venv.
