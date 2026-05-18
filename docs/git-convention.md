# Git Convention

This repository uses Conventional Commits and short-lived topic branches.

## Commit Message Format

```text
<type>(<scope>): <subject>
```

Rules:

- Use lowercase `type` and `scope`.
- Keep the subject concise and imperative.
- Do not end the subject with a period.
- Use the body when the change needs motivation, migration notes, or operational context.

## Types

| Type | Use when |
| --- | --- |
| `feat` | Adding a new skill, command, schema, reference, or user-visible capability. |
| `fix` | Correcting broken skill behavior, schemas, scripts, docs, or packaging. |
| `docs` | Updating README, references, conventions, or other documentation only. |
| `test` | Adding or updating evals, fixtures, or automated tests. |
| `refactor` | Restructuring without changing behavior. |
| `build` | Changing build tools, packaging, generated artifact rules, or dependencies. |
| `ci` | Changing CI workflows or release automation. |
| `chore` | Repository maintenance that does not affect skill behavior. |
| `revert` | Reverting a previous commit. |

## Scopes

Prefer one of these scopes:

- `repo`: repository-level metadata, layout, license, or ignore rules
- `docs`: repository documentation
- `flink-ops`: Apache Flink operations skill
- `sql-development`: future SQL development skill
- `data-analysis`: future data analysis skill
- `schemas`: shared schema conventions
- `evals`: evaluation cases and fixtures
- `scripts`: executable assets inside skills

Add a new scope only when it names a real skill or stable repository area.

## Examples

```text
feat(flink-ops): add checkpoint diagnostics schema
fix(flink-ops): require confirmation before stop job
docs(repo): describe skill safety model
test(flink-ops): add trigger evals for backpressure diagnosis
build(flink-ops): ignore generated maven target output
```

## Branch Names

Use short topic branches:

```text
feat/flink-ops-checkpoints
fix/flink-ops-stop-confirmation
docs/skill-architecture
chore/repo-cleanup
```

## Pull Request Checklist

Before opening or merging a pull request, verify:

- The skill still has exactly one top-level package directory when zipped.
- `SKILL.md` and `skill.yaml` agree on name, trigger intent, and available tools.
- Mutation commands have an explicit confirmation gate.
- Target locking and read-back verification are documented for production operations.
- Generated build outputs, credentials, local paths, and archives are not committed.
- Relevant evals, schemas, or references were updated with the behavior change.

## Breaking Changes

If a change breaks skill names, command names, schemas, or runtime requirements, add a footer:

```text
BREAKING CHANGE: renamed skill from old-name to new-name
```
