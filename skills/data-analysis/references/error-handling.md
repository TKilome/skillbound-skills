# Error Handling

Use this file when a CLI command exits non-zero or returns JSON with `ok: false`.

## Failure Signal

Treat the operation as failed when either condition is true:

- CLI returns non-zero exit code.
- JSON output includes `"ok": false`.

Do not claim success after a failed command.

## Parse First, Then Act

Extract these fields from the response:

- `errorCode`
- `error`
- `validation.errors`
- `validation.warnings`
- `resultFile` or `replyFile` when present

## Recovery Matrix

| Error Code | Meaning | Recovery Action |
| --- | --- | --- |
| `MissingRequiredArgument` | Command-specific required argument is missing | Ask only for the missing argument. For `sql_analysis`, ask for SQL text. |
| `MissingMetadataSource` | Neither vector store config nor the selected engine DSN is configured | Ask user/admin to configure Chroma (`VECTOR_STORE_PROVIDER`, `VECTOR_STORE_HOST`, `VECTOR_STORE_COLLECTION`), StarRocks vector store (`VECTOR_STORE_PROVIDER`, `VECTOR_STORE_STARROCKS_DSN`), or SQL introspection (`SQL_ENGINE_KIND` plus the matching SQL engine DSN). |
| `MissingVectorStoreConfig` | Vector-backed exploration lacks provider-specific config | For Chroma, provide `--vector-host` and `--vector-collection`. For StarRocks, provide `--vector-dsn`; the table is fixed to data-ai's `data_ai_rag.rag_documents`. |
| `MissingEmbeddingConfig` | StarRocks vector exploration lacks embedding config | Configure `VECTOR_STORE_EMBEDDING_MODEL`, `VECTOR_STORE_EMBEDDING_BASE_URL`, and `VECTOR_STORE_EMBEDDING_API_KEY`, or pass the matching CLI arguments. |
| `UnsupportedVectorStore` | Unsupported vector provider was selected | Use `VECTOR_STORE_PROVIDER=chroma` or `VECTOR_STORE_PROVIDER=starrocks`, or fall back to SQL introspection. |
| `EngineSelectionNotAllowed` | Command payload attempted to choose engine/dialect | Remove engine selection fields from payload. Engine kind must be passed as `--engine-kind "$SQL_ENGINE_KIND"`. |
| `ReadOnlySqlRequired` | SQL failed read-only validation | Show validation errors and ask for a single `SELECT` or `WITH` statement. |
| `SqlExecutionFailed` | SQL failed during execution | Report the database error summary and ask whether to revise SQL or inspect DDL. |
| `MissingResultFile` | `answer_result` lacks `resultFile` | Ask for the result file path from a previous `sql_analysis` call. |
| `ResultFileNotFound` | Result file path does not exist | Ask for a valid path or rerun `sql_analysis`. |
| `DataExplorationFailed` | Vector lookup or SQL introspection failed | Report the error and verify vector store or SQL engine configuration. |
| `SqlAnalysisFailed` | Command wrapper failed before normal SQL result handling | Report the wrapper error and retry only after correcting input/config. |
| `AnswerResultFailed` | Answer artifact creation failed | Verify `resultFile` and output directory permissions. |

## Standard Recovery Flow

1. State which command failed.
2. Include `errorCode` and concise `error`.
3. Include validation errors when present.
4. Ask for exactly the missing or corrected input needed for one retry.
5. Do not print credentials or resolved environment variable values.

## Response Template

```text
Operation failed.
- Command: <command>
- Error: <errorCode>
- Message: <error>

Suggested next step: <single actionable step>
```
