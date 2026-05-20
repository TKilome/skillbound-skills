# Agent Operating Protocol

Use this file after the skill is triggered. It defines default execution
behavior for Codex, Claude Code, and similar agents.

## 1) Execution Entry

```bash
"$SKILL_DIR/venv/bin/python" "$SKILL_DIR/scripts/data_analysis_cli.py" <command> [args...]
```

Never use `mysql`, `trino`, notebooks, ad hoc Python snippets, or other database
clients as substitutes for this CLI.

## 2) Operation Classification

- **Read**: `data_exploration`, `validate_sql`, `sql_analysis`,
  `answer_result`.
- **Mutating**: out of scope.
- **Destructive**: out of scope.

All in-scope commands are read-only from the user's data perspective. Result
artifacts are written under `scripts/output/`.

## Intent Routing

- Use `direct_answer` for conceptual questions that do not require data access.
- Use `data_exploration` when the user asks what data exists, asks for available
  metrics or dimensions, or provides an ambiguous business topic.
- Use `clarify_requirement` when required slots are missing after lightweight
  exploration.
- Use `sql_analysis` only after the requirement, engine, session, table context,
  and safety constraints are clear.
- Use `boundary` for mutation, admin, credential, unrestricted export, or data
  repair requests.

## 3) Parameter Strategy

1. Map the request to one concrete CLI command using `command-map.md`.
2. Check out-of-scope cues first: mutation, data repair, credential discovery,
   unrestricted export, Flink/cloud operations, or frontend/code work.
3. If command is `data_exploration`, do not require SQL text.
4. If command is `sql_analysis`, require `--sql` or input payload field `sql`.
5. If command is `answer_result`, require `resultFile`; SQL text is optional.
6. For SQL commands, choose `--dsn` from `SQL_ENGINE_KIND`: mysql uses
   `SQL_ENGINE_MYSQL_DSN`, starrocks uses `SQL_ENGINE_STARROCKS_DSN`, and trino
   uses `SQL_ENGINE_TRINO_DSN`.
7. For vector metadata commands, choose provider-specific arguments from
   `VECTOR_STORE_PROVIDER`: Chroma uses host/port/collection, StarRocks uses
   the vector DSN plus OpenAI-compatible embedding config, then reads
   `data_ai_rag.rag_documents` with `approx_cosine_similarity`.
8. If a required placeholder expands to an empty or invalid value, ask for
   runtime configuration without asking the user to paste secret values.
9. Do not fabricate SQL. If the user asks for analysis but has not provided SQL,
   first run `data_exploration` or ask focused clarification.

## 4) Credential Safety

- Never inspect, print, or store resolved `SQL_ENGINE_MYSQL_DSN`, `SQL_ENGINE_STARROCKS_DSN`, or
  `SQL_ENGINE_TRINO_DSN` values.
- Never inspect, print, or store resolved `VECTOR_STORE_STARROCKS_DSN` values.
- Never inspect, print, or store resolved `VECTOR_STORE_AUTH_TOKEN` values.
- Never inspect, print, or store resolved `VECTOR_STORE_EMBEDDING_API_KEY` values.
- Never pass selected engine kind in command payloads. Pass it as
  `--engine-kind "$SQL_ENGINE_KIND"` in the Bash command.
- Pass only the selected engine's DSN placeholder as `--dsn`; do not pass
  unrelated engine connection variables to the command.
- Never ask the user to paste passwords, tokens, access keys, or connection
  strings in chat.
- Refer to environment variable names and placeholders, not their values.

## Multi-Turn Behavior

- Carry forward confirmed slots and assumptions from session memory only when
  they are visible or provided by the host runtime for the current session.
- If a new user message changes the business object, metric, time range, or
  engine, re-check affected slots instead of reusing stale context.
- Preserve the final SQL and result interpretation in memory when the host
  runtime supports it.

## 5) Standard Execution Flow

1. Classify intent and boundary.
2. Load `command-map.md`; load clarification or SQL safety references as needed.
3. Build a JSON payload for the selected command.
4. Execute the CLI command.
5. If command fails, load `error-handling.md`.
6. For `sql_analysis`, report `resultFile`, optional `csvFile`, `rowCount`,
   warnings, and elapsed time. Do not paste rows by default.
7. For `answer_result`, read or reference `replyFile` and summarize the answer.

## 6) Failure Flow

1. If command fails, do not claim success.
2. Parse `errorCode` and `error`.
3. Ask only for the missing/corrected input needed.
4. If SQL validation fails, ask for a single read-only `SELECT` or `WITH`.
5. If execution fails, suggest either revising SQL or running
   `data_exploration` to inspect DDL.

## 7) Response Requirements

For clarification:

- Ask one concise question.
- Include suggested options only when they come from exploration or the catalog.
- Prefer business terms over table names.

For analysis results:

- State the result first.
- Include row count or aggregation grain.
- Include SQL used, or explain why it cannot be shown.
- List assumptions, defaults, and caveats.
- Mention when the result is a preview, limited sample, or empty set.

For failures:

- State which step failed: requirement, exploration, validation, execution, or
  result formatting.
- Include the runtime error summary without leaking secrets.
- Provide the next concrete input needed from the user.

## 8) Completion Criteria

Task is complete only when:

- The mapped CLI command was actually executed, or the request was explicitly
  out of scope.
- SQL was validated before execution.
- SQL execution result rows were written to a file, not returned inline.
- Final response includes result artifact paths or a clear failure reason.
