# Agent Operating Protocol

Use this file after the skill is triggered. It defines the default execution
behavior.

## 1) Execution Entry

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" <read-command> [args...]
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_build_image [args...]
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:<provider-runtime-classpath>" \
  com.skill.flinkops.FlinkOpsCli <provider-start-command> [args...]
```

For Kubernetes `k8s_start_job`, the provider runtime classpath is mandatory:

```bash
FLINK_HOME=<user-provided Flink home>
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*" \
  com.skill.flinkops.FlinkOpsCli k8s_start_job [args...]
```

Do not run Kubernetes `k8s_start_job` with `java -jar`. `java -jar` does not add
`$FLINK_HOME/lib/*` to the JVM classpath, so Flink Java Client classes such as
`org.apache.flink.configuration.Configuration` will be missing.

## 2) Tool Boundary

Only the bundled `FlinkOpsCli` may be executed for this skill.

Prohibited in all scenarios:

- `kubectl`
- `$FLINK_HOME/bin/flink`
- `flink run-application`
- `jar tf`, `javap`, or source-tree searches used to reverse-engineer runtime
  state during an operational request
- Flink shell scripts
- hand-written Kubernetes manifests
- Flink Operator CRDs
- cloud CLIs
- ad hoc scripts that bypass `FlinkOpsCli`

This applies to preflight checks, current-context discovery, namespace checks,
RBAC checks, submission, status, logs, events, troubleshooting, and verification.

Do not inspect the user's current workspace to discover example jars, built
artifacts, Kubernetes state, or Flink internals during an operational request.
Use the provider reference templates and user-provided parameters. If a required
parameter is missing, ask for it.

## 3) Operation Classification

- **Read**: `k8s_preflight_start`, `k8s_check_connectivity`, `yarn_check_connectivity`, `check_cli_environment`, `k8s_check_ingress_controller`, `k8s_render_ingress_controller_yaml`, `get_job_status`, `diagnose_job`, `get_exceptions`, `get_checkpoints`
- **Mutating**: `k8s_build_image`, `k8s_start_job`, `stop_job`

## Mode Selection

Use Real Ops Mode unless the user explicitly asks for eval, trigger, safety,
workflow, or JSONC eval processing.

Use Boundary Mode when the request is outside Apache Flink operations or asks
for unsupported external tool fallback.

Use Eval Mode only for validation tasks. Eval Mode can classify placeholder
inputs, but must not claim fabricated cluster state or bypass confirmation.

## 4) Parameter and Confirmation Strategy

1. Classify the user's intent before selecting a command.
   - Deployment intent: deploy/start/submit/run/启动/部署/提交/运行 with a jar
     or application. For Kubernetes, map this to the deployment flow:
     `k8s_check_ingress_controller` first when readiness is not visible, then
     `k8s_preflight_start` with the derived `--enable-ingress` value, then
     optionally `k8s_build_image`, then `k8s_start_job`.
   - Inspection intent: status/diagnose/exceptions/checkpoints/stop for an
     existing JobManager REST endpoint. Map this to REST commands only when the
     user supplies the REST URL or it is visible from a successful `k8s_start_job`
     result in the current conversation.
2. Map the request to one concrete `FlinkOpsCli` command for the next step.
3. For provider start, identify the target provider and load the provider
   reference. Kubernetes uses `k8s_start_job`.
   For Kubernetes, the displayed final command must use `$JAVA_HOME/bin/java -cp` with
   `$FLINK_HOME/lib/*`; do not display or run a `java -jar ... k8s_start_job`
   command.
4. For a local jar that is not already inside the target Flink image, map the
   request to `k8s_build_image` first. Do not run `docker` directly. The resulting
   start `--jar-uri` is derived from the fixed image path
   `local:///opt/flink/usrlib/<local-jar-file-name>`; do not ask the user to
   choose `--jar-uri` and do not suggest `/opt/flink/examples` paths.
5. For Kubernetes start readiness checks, map to:
   1. `k8s_check_ingress_controller --namespace <namespace>`
   2. `k8s_check_connectivity --namespace <namespace> --service-account <service-account> [--kubeconfig-path <path>] --enable-ingress <true|false>`
   3. `check_cli_environment --deployment-target kubernetes --flink-home <flink-home>`
   4. `k8s_preflight_start` as the combined check used immediately before start.
   Do not run external preflight commands.
   Kubernetes credentials are path-only: the agent must never inspect kubeconfig
   contents. It may only pass a local kubeconfig file path to the CLI. If no path
   is provided, the CLI uses `~/.kube/config`.
6. Do not rely on stale conversation memory for readiness. If the current
   visible context does not contain a recent successful check result for the
   same namespace, service account, Ingress setting, and runtime path, rerun the
   relevant `FlinkOpsCli` check before any mutation.
7. For a new deployment request with no visible readiness result, collect only
   the user-owned fields needed for checks first. Do not ask for all
   `k8s_start_job` parameters before the environment check. For Kubernetes, collect
   `namespace`, `service-account`, `flink-home`, and optionally `kubeconfig-path`;
   do not ask the user to choose `enable-ingress` up front.
8. Ask for only one category of parameters per user turn. The categories are:
   readiness inputs, Ingress remediation choice, image packaging inputs, start
   configuration, and mutation confirmation. Do not combine categories. In the
   first response to a fresh deployment request, do not ask for `name`,
   `parallelism`, `flink-image`, `main-class`, build image choices, or
   confirmation.
9. For Kubernetes external access, run
   `k8s_check_ingress_controller --namespace <namespace>` before `k8s_preflight_start`.
   If a namespace-scoped Ingress Controller exists, derive
   `--enable-ingress true`. If it is missing, show the CLI-provided remediation
   options and ask whether the user wants to create it. When the error data has
   `helmCommands`, `kubectlTemplatePath`, `kubectlManualRenderCommands`, and
   `kubectlManualApplyCommands`, show all of them. Do not run Helm or
   `kubectl`. If the user chooses kubectl, do not execute
   `k8s_render_ingress_controller_yaml`, do not redirect output to a YAML file, and
   do not execute `kubectl apply`. Recommend the displayed manual commands for
   the user or platform administrator to run, then wait. After the user says
   they applied it, rerun `k8s_check_ingress_controller`. If the user does not
   create it, derive
   `--enable-ingress false`.
10. Then run `k8s_check_connectivity`, `check_cli_environment`, and
   `k8s_preflight_start` with the derived `--enable-ingress` value.
11. If required parameters are missing, ask only for those fields.
12. Do not ask for bare flag names. For every missing field, explain what it
   means, where the user can get it, an example value, and any important caveat.
13. Do not run external or local discovery commands to infer missing parameters.
14. Treat examples as examples only. Do not promote example REST URLs, jar URIs,
   namespaces, service accounts, image names, or application names into hidden
   defaults.
15. For mutating operations, show the exact final command without executing it
   and ask the user to confirm execution. Do not treat a direct imperative
   request as confirmation.
16. Execute a mutating command with `--confirm` only after the user explicitly
   confirms the displayed command in the current conversation.
17. Use `--dry-run` when the user asks for a preview or when real cluster
   mutation is not allowed.

## 5) Failure Flow

1. If the CLI returns `success:false`, report `error.code` and `error.message`.
2. Do not claim success.
3. Do not investigate with external CLIs.
4. Ask for the missing parameter with an explanation, example, and caveat, or
   report the missing skill capability.

## 6) Completion Criteria

Task is complete only when:

- A mapped `FlinkOpsCli` command was executed, or the exact missing parameters
  were requested.
- `--confirm` was used for mutations only after explicit pre-execution
  confirmation of the displayed command.
- No prohibited external CLI was used.
