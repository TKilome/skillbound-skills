# SQL Safety

Use this reference for every generated or user-provided SQL statement.

## Validation Rules

- Accept exactly one SQL statement.
- Accept only read-only `SELECT` or `WITH`.
- Reject mutation or administration keywords: `INSERT`, `UPDATE`, `DELETE`,
  `MERGE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`, `CALL`, `EXEC`, `GRANT`,
  `REVOKE`, `USE`, `SET`, `LOAD`, `COPY`, `EXPORT`, `ANALYZE`, `OPTIMIZE`.
- Reject multi-statements, semicolon-separated payloads, shell escapes, and SQL
  comments that hide additional statements.
- Prefer parsed validation with the host runtime SQL parser. Do not rely on
  regular expressions alone.
- Ensure referenced tables belong to the selected engine/session and the
  allowed schema context.

## Query Construction

- Use explicit column lists instead of `SELECT *` unless the user explicitly
  asks for all common fields and the runtime enforces column controls.
- For detail queries, apply deterministic ordering and a row limit.
- For summary queries, group only by requested dimensions and include metric
  definitions in the final answer.
- Avoid joining tables only by guessed field names. Explore schema or ask for
  clarification when join keys are unclear.
- When time zone or natural-day semantics matter, state the assumed time zone.

## Result Handling

- Do not claim a result was produced unless the runtime returned rows or an
  explicit empty result.
- Distinguish `0 rows` from execution failure.
- Show the SQL used unless policy or runtime settings prohibit it.
- Mask direct identifiers when they are not needed for the answer, especially
  phone numbers, emails, external user IDs, address fields, and tokens.
