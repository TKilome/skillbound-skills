# SkillBound Skills

SkillBound Skills is a collection of reusable skills for SkillBound Agent.
Each skill defines an agent capability and its execution boundary through skill-owned instructions, commands, scripts, references, and safety rules.

A skill sits between an agent's reasoning loop and the external systems it can operate. It turns domain SOPs and executable tools into a controlled capability package: the agent can do more than generic chat, but it must act inside the boundary declared by the skill.

The repository starts with Apache Flink operations and is structured to add more domains later. Data is one domain category, not the repository boundary.

## Design Philosophy

```text
Skills define capabilities and boundaries.
SkillBound Agent enforces them.
```

Memory is the agent's context asset, tools are executable devices, and skills are the operating process that tells the agent when and how to use those devices.

Each skill should provide two things at the same time:

- **Extension**: add domain capabilities the base agent does not have, such as Flink diagnostics, deployment workflows, SQL review, or data analysis routines.
- **Constraint**: narrow the agent's behavior with declared commands, parameter validation, confirmation gates, target boundaries, and verification rules.

This is different from letting an agent directly call arbitrary APIs or shell commands. Direct tool access can execute work, but it does not provide enough operating boundary for production systems. A skill packages the safe path.

## Skill Architecture

A production skill should be designed as a controlled execution layer:

```text
User request
  -> Agent reasoning
  -> Skill trigger and instructions
  -> Declared command/script + validated parameters
  -> External system
  -> Read-back verification
  -> Agent response
```

The important boundary is between agent reasoning and execution. The agent may choose and explain, but execution should go through skill-owned commands or scripts described in `SKILL.md`.

Recommended safety model:

| Mechanism | Purpose |
| --- | --- |
| Confirm gate | Require explicit user confirmation before mutation commands. |
| Target lock | Keep operations scoped to the selected resource, cluster, workspace, namespace, or dataset. |
| Read-back verification | Treat command/API success as provisional and verify the actual resulting state before claiming success. |
| Declared tool surface | Allow only commands/scripts declared by the skill, with explicit parameter validation. |
| Progressive references | Keep `SKILL.md` concise and load detailed playbooks only when needed. |

## Repository Layout

```text
skills/
  flink-ops/          # Apache Flink operations, diagnostics, and deployment workflows
  sql-development/    # Reserved for SQL authoring, review, optimization, and execution skills
  data-analysis/      # Reserved for data analysis and insight-generation skills
```

The layout is intentionally flat: each directory under `skills/` is one uploadable skill package. Use directory names and documentation tables to describe domains instead of adding deep category folders.

## Available Skills

| Skill | Status | Purpose |
| --- | --- | --- |
| `flink-ops` | Initial | Inspect, diagnose, start, stop, and operate Apache Flink jobs through a bundled CLI and declared command surface. |
| `sql-development` | Reserved | Future skill for SQL development, validation, optimization, and execution workflows. |
| `data-analysis` | Initial | Data AI-style requirement clarification, business data exploration, read-only SQL analysis, uploaded table analysis, and result interpretation workflows. |

## Related Repositories

- `skillbound-agent`: the runtime that loads selected skills and enforces skill-defined execution.
- `skillbound-skills`: this repository, containing reusable skills that define agent capabilities and safety boundaries.

## Skill Package Format

Each skill directory should be self-contained:

```text
skill-name/
  SKILL.md       # Agent-facing operating instructions
  references/    # Longer playbooks and reference material
  scripts/       # Optional deterministic tools owned by the skill
  assets/         # Optional dependency manifests, templates, static resources
```

Only `SKILL.md` is required. Put `name`, `description`, and optional `envs` in `SKILL.md` frontmatter. Keep command usage, safety rules, and operating procedures in the Markdown body. Production skills should prefer command-specific validation and references over long inline instructions. Do not commit credentials, generated build output, machine-specific paths, or local environment files.

### Production Skill Methodology

Production skills should follow the method used by high-boundary operations skills such as `alibabacloud-flink-workspace-ops`: the skill must make it hard for the host agent to improvise outside the declared execution path.

`SKILL.md` should contain these sections when the skill operates external systems or executes commands:

- **Mandatory execution rule**: name the only allowed script/CLI entry and explicitly forbid substitute CLIs, ad hoc scripts, notebooks, shell one-offs, mocked output, or pure explanation when execution is required.
- **Scope & Boundaries**: state what is in scope and what is out of scope. Include exact boundary responses for common out-of-scope requests so the host agent does not silently switch domains.
- **Trigger Conditions**: list operation keywords and resource ID patterns that should trigger the skill. Also list hard non-trigger cues.
- **Execution Protocol**: define execute-first behavior, parameter strategy, approval gates, credential safety, read-back verification, failure flow, and completion criteria.
- **Command Quick Reference**: map high-frequency user intents to exact subcommands and operation type (`Read`, `Mutation`, `Destructive`).
- **Resources**: split references into "Load After Trigger" and "Load On Demand" so `SKILL.md` stays concise and agents know what to read next.
- **Assets**: list the main CLI/script entry and dependency manifest.

Required reference files for command-oriented production skills:

```text
references/
  command-map.md                 # Intent-to-command routing, disambiguation, first-call rules
  agent-operating-protocol.md    # Execution entry, parameter strategy, safety gates, completion criteria
  command-catalog.md             # Full command syntax, payload examples, environment/config notes
  error-handling.md              # Error-code recovery matrix and retry/stop rules
```

Use additional references only when needed:

```text
references/
  resource-disclosure.md         # What may be disclosed vs hidden
  verification-method.md         # Read-back verification for mutations
  playbooks/*.md                 # Multi-step workflows
  *-product-model.md             # Domain model and entity relationships
  cli-installation-guide.md      # Runtime/dependency setup
```

For Python-based skills, put dependencies in `assets/requirements.txt` when they are packaged resources, or in `requirement.txt` only when this repository's local workflow already expects that file. Do not commit `venv/`; keep it ignored and rebuild it from the dependency manifest.

### Execution-Boundary Rules

Every executable skill must declare a single approved execution surface:

- Prefer one main CLI under `scripts/`, for example `python scripts/<skill>_ops.py <command> [args]`.
- All real external-system interaction must go through that CLI or explicitly declared scripts.
- The host agent may reason, choose commands, and summarize results, but it must not bypass the skill CLI with direct SDK calls, database clients, cloud CLIs, notebooks, or generated helper scripts.
- If the CLI lacks a capability, the agent must report the missing capability instead of switching tools.
- For command-specific required parameters, validate at the command boundary and return stable error codes. Do not apply unrelated parameter checks to other commands.
- For Python scripts, never assume the caller's current working directory. Resolve resources from `SKILL_DIR` or from the script path, and resolve relative configured paths against the skill directory.

### Safety Method

Classify every command as one of:

| Type | Rule |
| --- | --- |
| Read | Execute directly after required parameters are available. |
| Mutation | Require clear user intent or explicit confirmation; use the skill's canonical confirmation flag when applicable. |
| Destructive | Require explicit destructive intent, confirmation, impact warning, and read-back verification. |

Mutation/destructive skills must implement read-back verification before claiming success. If read-back cannot be performed, the final response must say that verification is incomplete.

### Frontmatter Fields

`SKILL.md` starts with YAML frontmatter:

```yaml
---
name: flink-intelligent-ops
description: Use when the user asks to start, stop, operate, inspect, or diagnose Apache Flink jobs, including deployment-target-specific starts and Flink REST status, exception, and checkpoint checks.
license: Apache-2.0
compatibility: Requires Java 8+. Provider operations may require provider runtime libraries on the classpath and credentials usable by the bundled CLI.
envs:
  - env: JAVA_HOME
    description: Java runtime used to execute the bundled FlinkOps CLI.
---
```

- `name`: required stable skill identifier. Use lowercase letters, digits, and hyphens.
- `description`: required trigger description used by compatible agents to decide when the skill applies.
- `license`: optional human-facing license label.
- `compatibility`: optional runtime and dependency notes for users and administrators.
- `envs`: optional required runtime environment variable declarations. Compatible runtimes can show these in Admin configuration, store configured values, validate them before execution, and inject them into the subprocess environment.

## Runtime Environment Variables

Skills that need runtime environment variables must declare them in `SKILL.md` frontmatter under `envs`:

```yaml
---
name: flink-ops
description: Use when ...
envs:
  - env: JAVA_HOME
    description: Java runtime used to execute bundled CLI commands.
---
```

The `env` field is the required variable name. The `description` field is optional and is shown by compatible runtimes when administrators configure the skill. Do not put real values, DSNs, passwords, tokens, `.env` files, or machine-local credentials in this repository.

In SkillBound Agent, every frontmatter `envs` item is treated as required for command execution. The loading flow is:

1. The runtime parses `SKILL.md` frontmatter `envs` when the skill zip is uploaded.
2. The runtime stores the env declarations and shows them in the Admin page.
3. An administrator configures plaintext values after upload.
4. When a selected skill executes a command, the runtime reloads the selected skill manifest, verifies every declared env has a non-empty configured value, and injects those values into that one subprocess call.
5. If a value is missing, command execution stops with `Skill env is not configured: <ENV_NAME>`.
6. Command audit records store the injected values in `tool_calls.env_json` as plaintext.

`SKILL.md` frontmatter is the source of truth for env declarations. The Markdown body should explain how the agent should reason about runtime prerequisites.

## Using a Skill

1. Review the target skill directory under `skills/`.
2. Build any required runtime assets described by the skill; for example, `flink-ops` uses Maven under `scripts/`.
3. Zip exactly one top-level skill directory.
4. Upload the zip to SkillBound Agent or another compatible skill runtime.
5. Grant permissions and select the skill in a chat session.

Example packaging command:

```bash
cd skills
zip -r flink-ops.zip flink-ops -x '*/target/*' '*/.DS_Store'
```

## Adding a New Skill

1. Create a new directory directly under `skills/`.
2. Add `SKILL.md` with clear trigger conditions, operating rules, input collection rules, safety gates, and result interpretation guidance.
3. Add `name`, `description`, and optional `envs` to `SKILL.md` frontmatter; describe command usage in the Markdown body.
4. Put detailed playbooks or compatibility notes under `references/`.
5. Keep executable assets under `scripts/`, and keep generated outputs ignored.

Before publishing a skill, check that it answers these questions:

- What new capability does this skill add to the agent?
- Which commands or scripts are the only approved execution path?
- Which operations are read-only and which require confirmation?
- How is the target resource locked so the agent cannot drift to another resource?
- How does the skill verify actual state after a mutation?
- What should the agent do when the bundled command cannot complete the task?

## Git Convention

Use Conventional Commits for all changes. See [docs/git-convention.md](docs/git-convention.md).

## License

Apache License 2.0.
