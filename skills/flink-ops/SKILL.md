---
name: flink-ops
description: Use when the user asks to start, stop, operate, inspect, or diagnose Apache Flink jobs, including deployment-target-specific starts and Flink REST status, exception, and checkpoint checks.
license: Apache-2.0
compatibility: Requires Java 8+. Provider operations may require provider runtime libraries on the classpath and credentials usable by the bundled CLI.
envs:
- env: SKILL_DIR
  description: Directory containing this SKILL.md, used to locate bundled scripts, assets, and the CLI jar.
- env: JAVA_HOME
  description: Java runtime home used to run the bundled Flink Ops CLI.
- env: FLINK_HOME
  description: Flink runtime home used to load Flink client libraries for provider-specific start commands.
- env: HADOOP_CONF_DIR
  description: Hadoop configuration directory used by YARN provider checks and application-mode starts.
- env: YARN_CONF_DIR
  description: YARN configuration directory used as a fallback for YARN connectivity checks when HADOOP_CONF_DIR is not set.
- env: KUBECONFIG_PATH
  description: Local kubeconfig file path passed to the Kubernetes client.
- env: K8S_NAMESPACE
  description: Kubernetes namespace used for Flink application resources, REST Service, and Ingress checks.
- env: K8S_SERVICE_ACCOUNT
  description: Kubernetes ServiceAccount used by Flink JobManager and TaskManager pods.
metadata:
  domain: aiops
  owner: flink-team
  allowed-tools: Bash Read
---

# Flink Ops

Give a host agent shared Apache Flink operations capability.

**MANDATORY EXECUTION RULE**: When this skill is triggered, use only this
skill's bundled Java CLI:

```bash
com.skill.flinkops.FlinkOpsCli
```

Never use `kubectl`, `flink run-application`, Flink shell scripts, hand-written
Kubernetes manifests, Flink Operator CRDs, cloud CLIs, or ad hoc scripts as
substitutes for skill execution. This rule applies to discovery, preflight,
submission, verification, logs, status, and troubleshooting. If the bundled CLI
cannot answer or perform an action, report the missing capability instead of
switching tools.

The key boundary is:

- Common operations use Flink REST and work for Kubernetes, YARN, standalone, or
  any deployment target that exposes a JobManager REST URL.
- Deployment operations are provider-specific. The MVP implements
  `--deployment-target kubernetes` with Flink native Kubernetes application
  mode.

## Resolve Runtime Paths

Before running any command, resolve `SKILL_DIR` as the directory that contains
this `SKILL.md`. Do not assume the current working directory is the skill
directory.

Examples:

- Codex global install: `$HOME/.codex/skills/flink-intelligent-ops`
- Claude Code global install: `$HOME/.claude/skills/flink-intelligent-ops`
- Project-local install: `<repo>/skills/flink-intelligent-ops`

Resolve runtime dependencies from the user's input, environment, or the selected
deployment provider reference. Do not assume a provider-specific runtime rule
applies to every deployment target.

Common Flink REST reads use the bundled jar directly:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" <read-command> [args]
```

Provider-specific start operations may require extra classpath entries or
environment variables. Load the provider reference before running a provider
start command. Kubernetes start includes the Flink runtime jars on the JVM
classpath.

```bash
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:<provider-runtime-classpath>" \
  com.skill.flinkops.FlinkOpsCli <provider-start-command> [provider args]
```

Kubernetes start must use:

```bash
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*" \
  com.skill.flinkops.FlinkOpsCli k8s_start_job [args]
```

## Scope

Common operations:

- `inspect_cluster --job-manager-url <url> [--deployment-target kubernetes --namespace "$K8S_NAMESPACE"] [--job-id <id>] [--http-host-header <host>] [--report]`
- `get_job_status --job-manager-url <url> [--job-id <id>] [--http-host-header <host>]`
- `diagnose_job --job-manager-url <url> [--job-id <id>] [--http-host-header <host>] [--report]`
- `diagnose_backpressure --job-manager-url <url> --job-id <id> [--vertex-id <vertex>] [--http-host-header <host>] [--report]`
- `get_exceptions --job-manager-url <url> --job-id <id> [--http-host-header <host>]`
- `get_checkpoints --job-manager-url <url> --job-id <id> [--http-host-header <host>]`

Kubernetes provider operations use explicit `k8s_*` commands and do not require
`--deployment-target`:

- `k8s_preflight_start --namespace "$K8S_NAMESPACE" --service-account "$K8S_SERVICE_ACCOUNT" --flink-home "$FLINK_HOME" --kubeconfig-path "$KUBECONFIG_PATH" [--enable-ingress <true|false>]`
- `k8s_check_connectivity --namespace "$K8S_NAMESPACE" --service-account "$K8S_SERVICE_ACCOUNT" --kubeconfig-path "$KUBECONFIG_PATH" [--enable-ingress <true|false>]`
- `yarn_check_connectivity [--hadoop-conf-dir <path>] [--yarn-queue <queue>] [--yarn-provided-lib-dirs <paths>] [--flink-dist-jar <path>]`
- `"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" check_cli_environment --deployment-target <kubernetes|yarn> --flink-home "$FLINK_HOME"`
- `k8s_check_ingress_controller --namespace "$K8S_NAMESPACE"`
  returns namespace-scoped Ingress Controller readiness, IngressClass status,
  and the Controller Service NodePort values when the service is present.
- `k8s_get_node_ips` returns Kubernetes node
  `InternalIP`, `ExternalIP`, and `Hostname` values for constructing agent-side
  NodePort URLs.
- `k8s_render_ingress_controller_yaml --namespace "$K8S_NAMESPACE"`

Packaging operations:

- `k8s_build_image --base-image <image> --local-jar <path> --target-image <image> [--target-jar-path <container path>]`

Deployment operations:

- `k8s_start_job ...`
- `stop_job --job-manager-url <url> --job-id <id> ...`
- Stop uses common Flink REST semantics after the JobManager URL is known and
  does not require a deployment target.
- Add `--verify` to `k8s_start_job` or `stop_job` when the user asks for read-back
  verification. The CLI reads the target REST endpoint after mutation and
  returns `readBackVerification`.
- Mutating operations return `targetLock.targetFingerprint`. Use
  `--expected-target-lock <fingerprint>` on execution after a preview when the
  user wants strict target locking.

For Kubernetes `k8s_start_job`, startup includes mandatory environment preflight and
Ingress creation. Do not ask for all start parameters at once. The CLI checks
the namespace, service account, namespace-scoped Ingress Controller, and
namespace-specific IngressClass before submitting Flink. After submission, it
creates or updates an Ingress for `<name>.flink.k8s.com`.

Kubernetes credentials are path-only. The agent may pass `--kubeconfig-path`
when the user provides a kubeconfig file path; otherwise the CLI defaults to
`~/.kube/config`. The agent must not read or ask the user to paste kubeconfig
content. The CLI loads the file directly into the Kubernetes SDK client and has
no serialization path for kubeconfig content in stdout, stderr, JSON responses,
errors, debug logs, reports, or exception messages.

Ingress can be disabled only when the user explicitly chooses not to expose the
Flink REST/Web UI externally:

```text
--enable-ingress false
```

When disabled, `k8s_start_job` skips Ingress Controller and IngressClass checks,
does not create an Ingress, and returns the internal JobManager REST Service URL
instead.

Out of scope for this version:

- YARN deployment implementation.
- Flink SQL Gateway operations.
- Installing Kubernetes or the Flink operator.
- Deleting Kubernetes resources.
- Auto remediation.

## Execution Modes

- Real Ops Mode: default for real operations. Do not use placeholder IDs,
  namespaces, REST URLs, images, providers, or jar values. Collect missing
  parameters by category. Mutations require explicit confirmation of the final
  `FlinkOpsCli` command before adding `--confirm`.
- Boundary Mode: for out-of-scope requests. Return boundary guidance only and
  do not execute a command.

## Execution Rules

- Execute the bundled Java CLI for read operations and approved mutations. For
  mutations, never execute the real command before parameter collection and
  user confirmation are complete.
- For `k8s_start_job`, the final command must use
  `"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*"`
  with `com.skill.flinkops.FlinkOpsCli`.
- When required parameters are missing, never ask with bare flag names only.
  Explain every requested parameter before asking the user to provide it. Use
  this format for each parameter: flag name, what it means, where the user can
  get it, an example value, and any important caveat. The user should not have
  to infer domain meaning from English CLI flag names.
- Ask for only one category of parameters per user turn. Do not mix readiness
  checks, Ingress remediation choices, image packaging, start configuration, and
  mutation confirmation in the same question. For a fresh deployment request,
  the first question should not ask for injected Kubernetes readiness inputs. Use
  `--namespace "$K8S_NAMESPACE"`, `--service-account "$K8S_SERVICE_ACCOUNT"`,
  optional `--kubeconfig-path "$KUBECONFIG_PATH"`, and
  `--flink-home "$FLINK_HOME"` as runtime placeholders. Do not ask for
  `--name`, `--parallelism`, `--flink-image`, `--main-class`, build image
  choices, or confirmation in the first question.
- For provider starts, first identify the target provider, then load the
  matching provider reference. Kubernetes uses `k8s_*` commands. YARN will use
  `yarn_*` commands when implemented.
- Do not treat provider commands as common commands. `k8s_get_node_ips`,
  `k8s_check_ingress_controller`, `k8s_render_ingress_controller_yaml`,
  `k8s_preflight_start`, `k8s_build_image`, and `k8s_start_job` are
  Kubernetes-provider commands and must not include `--deployment-target`.
  Common Flink REST commands use `--job-manager-url` and do not require a
  deployment target.
- Classify the user's intent before selecting a command. Requests such as
  deploy, start, submit, run, 启动, 部署, 提交, or 运行 with a jar/application
  are deployment intents. For Kubernetes, a new deployment intent maps first to
  the readiness flow when readiness is not visible:
  `k8s_check_ingress_controller`, then `k8s_preflight_start` with the derived
  `--enable-ingress` value, then optionally `k8s_build_image`, then
  `k8s_start_job`.
- Use `get_job_status` only for inspection of an existing JobManager REST URL:
  the user must provide the URL, or the URL must come from a visible successful
  `k8s_start_job` result in the current conversation. Do not infer a REST URL from
  a local jar path, `--flink-home`, examples, or common defaults.
- Use `inspect_cluster` for one-shot inspection requests such as 巡检, health
  check, pre-event check, or "give me a report" when a JobManager REST URL is
  known. With Kubernetes namespace context, it also checks the namespace-scoped
  Ingress Controller and node addresses. With `--report`, summarize collected
  evidence into health status, risk level, findings, root cause candidates, and
  recommendations. The inspection evidence set follows Flink 1.19 focused REST
  paths: `/overview`, `/config`, `/taskmanagers`, `/jobs`, and when a job ID is
  provided, job `status/config/plan`, restart metrics, exceptions, and
  checkpoints.
- Use `diagnose_job --report` when the user wants a diagnosis report instead of
  raw REST evidence. The report flags failed states, root exceptions,
  checkpoint timeouts/failures, exhausted slots, blocked TaskManagers, and
  restart metrics. Use `diagnose_backpressure` for suspected backpressure. It
  follows Flink 1.19 REST paths: `/jobs/:jobid` for vertex discovery,
  `/jobs/:jobid/vertices/:vertexid/backpressure`, vertex subtask metrics for
  backpressured/busy/idle time, exceptions, and checkpoints. If no direct vertex
  or metric-level evidence is available, report `unknown` instead of guessing.
- Do not rely on stale conversation memory for environment readiness. If the
  current visible context does not include a recent successful check result for
  the same namespace, service account, Ingress setting, and runtime path, run
  the relevant `FlinkOpsCli` check again before mutation. Long conversations may
  drop earlier check results.
- For a new deployment request, environment readiness comes before final start
  parameter collection. If the current visible context has no recent successful
  readiness result, do not collect Kubernetes readiness fields from the user. Use
  `--namespace "$K8S_NAMESPACE"`, `--service-account "$K8S_SERVICE_ACCOUNT"`,
  optional `--kubeconfig-path "$KUBECONFIG_PATH"`, and
  `--flink-home "$FLINK_HOME"` because these environment variables will be injected later. Do not ask for `--enable-ingress` up front; derive it from
  the Ingress Controller check.
  Run `k8s_check_ingress_controller --namespace "$K8S_NAMESPACE"` first. If a controller
  exists, run `k8s_check_connectivity` and `check_cli_environment`, then
  `k8s_preflight_start` with `--enable-ingress true`. If it is missing,
  show both CLI-provided remediation options when present: `helmCommands` and
  the kubectl path (`kubectlTemplatePath`, `kubectlManualRenderCommands`, and
  `kubectlManualApplyCommands`). The YAML template is built into the skill under
  `assets/kubernetes/ingress-controller.yaml.tpl`; do not run Helm or
  `kubectl`. Ask whether the user wants to create the namespace-scoped Ingress
  Controller. If the user chooses the kubectl path, do not execute the render
  command, do not write the YAML file, and do not execute `kubectl apply`.
  Recommend the displayed manual render/apply commands for the user or platform
  administrator to run, then wait. The next agent step after the user says they
  applied it is rerunning `k8s_check_ingress_controller`. If the user declines,
  continue with
  `--enable-ingress false`. After readiness is known, collect the remaining
  `k8s_start_job` parameters.
- For start/stop execution after a dry-run preview, preserve the returned
  `targetLock.targetFingerprint` and pass it as `--expected-target-lock` when
  the user wants target drift protection. A mismatch means the namespace, job
  URL, job id, host header, or operation target changed and the CLI must not
  proceed.
- Use `check_cli_environment` for the local CLI, Java, and Flink runtime
  checks needed by the submit chain. `k8s_preflight_start` includes those checks
  in its combined Kubernetes start readiness output.
- Do not use any external CLI for preflight or verification. In particular,
  `kubectl` is prohibited in all scenarios, including harmless-looking commands
  such as `kubectl config current-context`, `kubectl get ns`, `kubectl logs`,
  and `kubectl get events`.
- Do not fallback to an unrelated deployment mechanism when the selected
  provider path fails. Report the exact error and the missing skill capability
  or missing parameter.
- Do not fabricate Kubernetes, Flink, checkpoint, exception, or savepoint state.
- Use `--dry-run` when the user asks for a preview or when no real cluster
  should be touched.
- Read operations do not require confirmation.
- Mutating operations require an explicit pre-execution confirmation in the
  current turn. Direct user intent is not enough by itself. First show the final
  command that will run, then wait for the user to confirm, then execute with
  `--confirm`.
- Never print kubeconfig, tokens, certificates, or cloud credentials.

## Command Quick Reference

| User intent | Command | Type |
|-------------|---------|------|
| Check namespace Ingress Controller and NodePort | `k8s_check_ingress_controller --namespace "$K8S_NAMESPACE"` | Read |
| Check Kubernetes deployment target connectivity | `k8s_check_connectivity --namespace "$K8S_NAMESPACE" --service-account "$K8S_SERVICE_ACCOUNT" --kubeconfig-path "$KUBECONFIG_PATH" --enable-ingress <true|false>` | Read |
| Check YARN deployment target connectivity | `yarn_check_connectivity [--hadoop-conf-dir <path>] [--yarn-queue <queue>] [--yarn-provided-lib-dirs <paths>] [--flink-dist-jar <path>]` | Read |
| Check CLI execution environment | `"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" check_cli_environment --deployment-target <kubernetes|yarn> --flink-home "$FLINK_HOME"` | Read |
| Get Kubernetes node IPs | `k8s_get_node_ips` | Read |
| Render namespace Ingress Controller YAML | `k8s_render_ingress_controller_yaml --namespace "$K8S_NAMESPACE"` | Read |
| Check combined Kubernetes start readiness | `k8s_preflight_start --namespace "$K8S_NAMESPACE" --service-account "$K8S_SERVICE_ACCOUNT" --flink-home "$FLINK_HOME" --kubeconfig-path "$KUBECONFIG_PATH" --enable-ingress <true|false>` | Read |
| Build Flink image with local jar | `k8s_build_image --base-image <image> --local-jar <path> --target-image <image> [--target-jar-path <path>]` | Mutation |
| Start Flink job on Kubernetes | `k8s_start_job ...` | Mutation |
| Stop Flink job | `stop_job --job-manager-url <url> --job-id <id> --stop-mode cancel [--http-host-header <host>]` | Mutation |
| Stop with savepoint | `stop_job --job-manager-url <url> --job-id <id> --stop-mode savepoint --savepoint-dir <path> [--http-host-header <host>]` | Mutation |
| Get status | `get_job_status --job-manager-url <url> [--job-id <id>] [--http-host-header <host>]` | Read |
| Diagnose job | `diagnose_job --job-manager-url <url> [--job-id <id>] [--http-host-header <host>]` | Read |
| Get exceptions | `get_exceptions --job-manager-url <url> --job-id <id> [--http-host-header <host>]` | Read |
| Get checkpoints | `get_checkpoints --job-manager-url <url> --job-id <id> [--http-host-header <host>]` | Read |

Common deployment parameters:

```text
--deployment-target <provider>
--name <Flink application name>
--namespace <Kubernetes namespace>
--service-account <Kubernetes ServiceAccount>
--jar-uri <job jar URI>
--parallelism <parallelism>
--enable-ingress <true|false>
--dynamic-properties <comma-separated Flink key=value entries>
```

Parameter explanations to use when asking the user:

| Parameter | Explain before asking |
|-----------|-----------------------|
| `--deployment-target` | Deployment backend. For this version, use `kubernetes`. |
| `--name` | Flink application name. For Kubernetes this maps to `kubernetes.cluster-id`, affects the REST Service name, and generates `<name>.flink.k8s.com`. Example: `flink-example-streaming`. |
| `--namespace` | Kubernetes namespace where the Flink application, REST Service, and generated Ingress live. It is also the expected namespace boundary for the Ingress Controller. Example: `data-flink`. |
| `--service-account` | Kubernetes ServiceAccount used by Flink JobManager and TaskManager pods. This is not a username/password. It must already have the needed Kubernetes permissions. Example: `flink-sa`. |
| `--kubeconfig-path` | Optional local kubeconfig file path used by the CLI to load Kubernetes credentials. If omitted, the CLI defaults to `~/.kube/config`. Provide only the path, never kubeconfig file contents. Example: `/Users/dongjiaxin/.kube/config`. |
| `--flink-image` | Container image used to run Flink. It must contain the Flink runtime and, if using `local://`, the job jar at the referenced container path. Example: `registry.example.com/flink/orders:1.0.0`. |
| `--jar-uri` | Job jar URI visible to the Flink container. `local://` means a path inside the Flink image/container, not a path on the agent machine. For a user-provided local jar, do not ask the user to choose this value and do not suggest Flink example paths. Derive it from the fixed build image location: `local:///opt/flink/usrlib/<local-jar-file-name>`. Example: `local:///opt/flink/usrlib/orders.jar`. |
| `--parallelism` | Initial job parallelism. Use a positive integer. Example: `4`. |
| `--enable-ingress` | Whether to create an external Ingress for the Flink REST/Web UI. Default is `true`. Use `false` when the namespace has no Ingress Controller and the user does not want to create one. When false, access is internal only, for example `http://<name>-rest.<namespace>.svc.cluster.local:8081`. |
| `--main-class` | Java main class full name for the Flink application. Ask whether the jar manifest already declares the entry class. If not, or if the user wants to override it, collect the full class name. Example: `com.example.OrdersJob`. |
| `--args` | Optional application arguments passed to the Flink job main method. Example: `--env prod --date 2026-05-16`. |
| `--dynamic-properties` | Optional comma-separated Flink configuration overrides. Example: `jobmanager.memory.process.size=1024m,taskmanager.numberOfTaskSlots=2`. |

For new Kubernetes deployment requests, run readiness checks with injected
environment placeholders when no recent readiness result is visible:

```text
我需要先做环境检查，再收集完整启动参数。

说明：`--namespace`、`--service-account`、`--kubeconfig-path` 和 `--flink-home` 的值分别固定使用 `$K8S_NAMESPACE`、`$K8S_SERVICE_ACCOUNT`、`$KUBECONFIG_PATH` 和 `$FLINK_HOME` 环境变量占位，后续环境会注入。
我不会先问 --enable-ingress。会先检查 namespace 内是否已有 Ingress Controller；
如果有，就按启用 Ingress 继续；如果没有，会同时展示 Helm 命令和 kubectl 路径。
kubectl 路径会使用 skill 内置 YAML 模板，并给出 render 命令和 kubectl apply 命令。
不创建时才降级为 --enable-ingress false。
```

Do not append `--name`, `--parallelism`, `--flink-image`, `--main-class`, image
build questions, or mutation confirmation to this first question. Those belong
to later categories after readiness is known.

Run the Ingress Controller check first:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_check_ingress_controller \
  --namespace "$K8S_NAMESPACE"
```

Then run the unified check with the derived Ingress decision:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_check_connectivity \
  --namespace "$K8S_NAMESPACE" \
  --service-account "$K8S_SERVICE_ACCOUNT" \
  --kubeconfig-path "$KUBECONFIG_PATH" \
  --enable-ingress <true|false>

"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" check_cli_environment \
  --deployment-target kubernetes \
  --flink-home "$FLINK_HOME"

"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_preflight_start \
  --namespace "$K8S_NAMESPACE" \
  --service-account "$K8S_SERVICE_ACCOUNT" \
  --flink-home "$FLINK_HOME" \
  --kubeconfig-path "$KUBECONFIG_PATH" \
  --enable-ingress <true|false>
```

Only after this readiness check should the agent ask for the remaining
deployment values such as `--name`, `--flink-image`, `--main-class`, and
`--parallelism`. For a user-provided local jar, derive `--jar-uri` from
`local:///opt/flink/usrlib/<local-jar-file-name>` after `k8s_build_image`; do not
ask the user to choose it.

Common packaging parameters:

```text
--base-image <Flink base image>
--local-jar <jar path on the agent machine>
--target-image <image tag to build>
--target-jar-path <absolute jar path inside the image>
```

Packaging parameter explanations to use when asking the user:

| Parameter | Explain before asking |
|-----------|-----------------------|
| `--base-image` | Existing Flink image to build from. Example: `flink:1.19.3-scala_2.12-java11`. |
| `--local-jar` | Business jar path on the agent machine. This is used only during image build. Example: `/tmp/orders.jar`. |
| `--target-image` | New image tag to create and later use as `--flink-image`. Example: `registry.example.com/flink/orders:1.0.0`. |
| `--target-jar-path` | Optional absolute path inside the new image where the jar will be copied. If omitted, the CLI uses `/opt/flink/usrlib/<local-jar-file-name>`. This path becomes the `local://` jar URI. Example: `/opt/flink/usrlib/orders.jar`. |

Provider-specific parameters live in provider references.

Common REST parameters:

```text
--job-manager-url <Flink JobManager REST URL>
--job-id <Flink job id>
--http-host-header <HTTP Host header for Ingress routing>
```

REST parameter explanations to use when asking the user:

| Parameter | Explain before asking |
|-----------|-----------------------|
| `--job-manager-url` | Reachable Flink JobManager REST/Web URL from where the CLI runs. Ask for this only when the user wants to inspect or operate an existing Flink cluster/job. It is not a default for deployment requests. Example in cluster: `http://orders-rest.data-flink.svc.cluster.local:8081`; example via a user-created local port-forward: `http://localhost:8081`. |
| `--job-id` | Flink job ID shown by the JobManager REST `/jobs` response or Web UI. Required for job-specific exception, checkpoint, and stop operations. |
| `--http-host-header` | Optional HTTP Host header for Ingress routing when the network endpoint is a NodePort instead of the public DNS name. Use this for internal agent access such as `--job-manager-url http://<node-ip>:31626 --http-host-header topspeed-windowing.flink.k8s.com`. |
| `--stop-mode` | Stop behavior. `cancel` stops immediately; `savepoint` stops with a savepoint; `drain` drains supported pipelines and writes a savepoint. |
| `--savepoint-dir` | Durable storage path for savepoints when using `savepoint` or `drain`. Example: `s3://bucket/flink/savepoints` or `hdfs:///flink/savepoints`. |

Runtime check parameters:

```text
--deployment-target kubernetes
--flink-home "$FLINK_HOME"
```

Runtime parameter explanations to use when asking the user:

| Parameter | Explain before asking |
|-----------|-----------------------|
| `--flink-home` | Local Flink installation path used only to check whether provider runtime libraries are available on the CLI classpath. Example: `$FLINK_HOME`. |

Runtime checks run through `k8s_preflight_start`; do not ask users to run a
standalone local runtime diagnostic command.

## Safety Rules

Mutation commands must include `--confirm`, but the agent may add `--confirm`
only after it has shown the final command and the user has explicitly approved
execution in the current conversation. If omitted, the CLI returns a structured
`SafetyCheckRequired` error.

Read commands must never mutate cluster state and do not require `--confirm`.

## Failure Handling

When a command returns `success:false`:

1. Report `error.code` and `error.message`.
2. Do not claim the operation succeeded.
3. If the error is `ValidationError`, ask only for the missing or invalid
   parameter, but include the parameter explanation, source, example, and caveat
   before asking for the value.
4. If the error is `SafetyCheckRequired`, ask for explicit approval before
   retrying with `--confirm`.
5. If provider start fails, use the CLI error to identify missing runtime,
   credentials, permissions, image access, or jar URI. Do not run external CLIs
   to investigate.
6. If Flink REST fails, verify the JobManager URL, network path, and job id.

## References

Load after every trigger:

- `references/agent-operating-protocol.md`

Load when needed:

- `references/command-map.md`
- `references/product-model.md`
- `references/providers/kubernetes.md` when `--deployment-target kubernetes`
- `references/providers/yarn.md` when `--deployment-target yarn`
- `references/safety-rules.md`
- `references/error-handling.md`
