# SkillBound Skills

SkillBound Skills is a collection of reusable skills for SkillBound Agent.
Each skill defines an agent capability and its execution boundary through skill-owned instructions, commands, scripts, schemas, references, and safety rules.

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
- **Constraint**: narrow the agent's behavior with declared commands, input schemas, confirmation gates, target boundaries, and verification rules.

This is different from letting an agent directly call arbitrary APIs or shell commands. Direct tool access can execute work, but it does not provide enough operating boundary for production systems. A skill packages the safe path.

## Skill Architecture

A production skill should be designed as a controlled execution layer:

```text
User request
  -> Agent reasoning
  -> Skill trigger and instructions
  -> Declared command/script + schema
  -> External system
  -> Read-back verification
  -> Agent response
```

The important boundary is between agent reasoning and execution. The agent may choose and explain, but execution should go through skill-owned commands or scripts declared in `skill.yaml`.

Recommended safety model:

| Mechanism | Purpose |
| --- | --- |
| Confirm gate | Require explicit user confirmation before mutation commands. |
| Target lock | Keep operations scoped to the selected resource, cluster, workspace, namespace, or dataset. |
| Read-back verification | Treat command/API success as provisional and verify the actual resulting state before claiming success. |
| Declared tool surface | Allow only commands/scripts declared by the skill, with schemas for auditable inputs. |
| Progressive references | Keep `SKILL.md` concise and load detailed playbooks only when needed. |

## Repository Layout

```text
skills/
  flink-ops/          # Apache Flink operations, diagnostics, and deployment workflows
  sql-development/    # Reserved for SQL authoring, review, optimization, and execution skills
  data-analysis/      # Reserved for data analysis and insight-generation skills
```

The layout is intentionally flat: each directory under `skills/` is one uploadable skill package. Use metadata inside `skill.yaml` and documentation tables to describe domains instead of adding deep category folders.

Example domain metadata:

```yaml
metadata:
  domain: data
```

## Available Skills

| Skill | Status | Purpose |
| --- | --- | --- |
| `flink-ops` | Initial | Inspect, diagnose, start, stop, and operate Apache Flink jobs through a bundled CLI and declared schemas. |
| `sql-development` | Reserved | Future skill for SQL development, validation, optimization, and execution workflows. |
| `data-analysis` | Reserved | Future skill for data exploration, analysis planning, and result interpretation workflows. |

## Related Repositories

- `skillbound-agent`: the runtime that loads selected skills and enforces skill-defined execution.
- `skillbound-skills`: this repository, containing reusable skills that define agent capabilities and safety boundaries.

## Skill Package Format

Each skill directory should be self-contained:

```text
skill-name/
  SKILL.md       # Agent-facing operating instructions
  skill.yaml     # Metadata, runtime, permissions, and tool declarations
  schemas/       # JSON Schemas for tool inputs
  references/    # Longer playbooks and reference material
  evals/         # Optional trigger, safety, and workflow evaluation cases
  scripts/       # Optional deterministic tools owned by the skill
```

Only `SKILL.md` and `skill.yaml` are required, but production skills should prefer schemas and references over long inline instructions. Do not commit credentials, generated build output, machine-specific paths, or local environment files.

## Using a Skill

1. Review the target skill directory under `skills/`.
2. Build any required runtime assets declared in `skill.yaml`; for example, `flink-ops` uses Maven under `scripts/`.
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
3. Add `skill.yaml` with package metadata and declared tools.
4. Put structured tool input schemas under `schemas/`.
5. Put detailed playbooks or compatibility notes under `references/`.
6. Keep executable assets under `scripts/`, and keep generated outputs ignored.
7. Add lightweight eval cases under `evals/` when trigger or safety behavior matters.

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
