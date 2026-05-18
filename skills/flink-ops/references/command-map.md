# Command Map

## Routing Layers

1. Common Flink REST operations do not require `--deployment-target`.
2. Kubernetes provider operations use explicit `k8s_*` commands and do not
   require `--deployment-target`.
3. YARN provider start/preflight use `--deployment-target yarn`; explicit
   `yarn_*` aliases are still planned.
4. `kubernetes` loads `references/providers/kubernetes.md`.
5. `yarn` loads `references/providers/yarn.md` and uses Flink Java Client YARN
   application mode for start/preflight.
6. Unknown providers return `UnsupportedProvider`.

| Intent | Command |
|--------|---------|
| Check namespace-scoped Ingress Controller and NodePort | `k8s_check_ingress_controller --namespace <ns>` |
| Check Kubernetes deployment target connectivity | `k8s_check_connectivity --namespace <ns> --service-account <sa> [--kubeconfig-path <path>] --enable-ingress <derived>` |
| Check YARN deployment target connectivity | `yarn_check_connectivity [--hadoop-conf-dir <path>] [--yarn-queue <queue>] [--yarn-provided-lib-dirs <paths>] [--flink-dist-jar <path>]` |
| Check local CLI execution environment | `check_cli_environment --deployment-target <kubernetes|yarn> --flink-home <path>` |
| Get Kubernetes node IPs for NodePort access | `k8s_get_node_ips` |
| Render namespace-scoped Ingress Controller YAML | `k8s_render_ingress_controller_yaml --namespace <ns>` |
| Check Kubernetes start environment | First run `k8s_check_ingress_controller --namespace <ns>`, derive `--enable-ingress`, run `k8s_check_connectivity`, run `check_cli_environment`, then run `k8s_preflight_start --namespace <ns> --service-account <sa> --flink-home <path> [--kubeconfig-path <path>] --enable-ingress <derived>` |
| Build a Flink image with a local jar | `k8s_build_image --base-image <image> --local-jar <path> --target-image <image> [--target-jar-path <container-path>]` |
| Deploy/start/submit/run a Flink jar on Kubernetes | If no visible readiness result exists, first check Ingress Controller and run `k8s_preflight_start`; if the jar is only local, use `k8s_build_image`; then run `k8s_start_job` after confirmation |
| Start a Flink job on Kubernetes | `k8s_start_job --name <name> --namespace <ns> --service-account <sa> --flink-image <image> --jar-uri <uri> --parallelism <n>` |
| Start a Flink job on YARN application mode | `start_job --deployment-target yarn --name <name> --flink-home <path> --jar-uri <uri> --parallelism <n> [--yarn-queue <queue>]` |
| Start without external Ingress | `k8s_start_job --enable-ingress false ...` |
| Preview start command | Show the full command and ask for confirmation before adding `--confirm` |
| Stop or cancel a job | `stop_job --job-manager-url <url> --job-id <id> --stop-mode cancel` |
| Stop with savepoint | `stop_job --job-manager-url <url> --job-id <id> --stop-mode savepoint --savepoint-dir <path>` |
| Drain stop | `stop_job --job-manager-url <url> --job-id <id> --stop-mode drain --savepoint-dir <path>` |
| One-shot cluster/job inspection | `inspect_cluster --job-manager-url <url> [--deployment-target kubernetes --namespace <ns>] [--job-id <id>] [--http-host-header <host>] [--report]` |
| Get status through Flink REST | `get_job_status --job-manager-url <url> [--job-id <id>] [--http-host-header <host>]` |
| Diagnose through Flink REST | `diagnose_job --job-manager-url <url> [--job-id <id>] [--http-host-header <host>] [--report]` |
| Diagnose suspected backpressure | `diagnose_backpressure --job-manager-url <url> --job-id <id> [--vertex-id <vertex>] [--http-host-header <host>] [--report]` |
| Get job exceptions | `get_exceptions --job-manager-url <url> --job-id <id> [--http-host-header <host>]` |
| Get checkpoint status | `get_checkpoints --job-manager-url <url> --job-id <id> [--http-host-header <host>]` |

REST commands require an existing JobManager REST URL from the user or from a
visible successful `k8s_start_job` result in the current conversation. Do not infer
one from local jar paths, Flink home paths, or examples.

Kubernetes commands are not common commands. Use the explicit `k8s_*` names:
`k8s_get_node_ips`, `k8s_check_ingress_controller`,
`k8s_check_connectivity`, `k8s_render_ingress_controller_yaml`,
`k8s_preflight_start`, `k8s_build_image`, and `k8s_start_job`. Do not add
`--deployment-target` to these aliases. `check_cli_environment` is the
provider-aware local CLI execution environment check. YARN-specific start and
preflight use `--deployment-target yarn`
until explicit `yarn_*` aliases exist.

`inspect_cluster` and `diagnose_job --report` intentionally collect a focused
Flink 1.19 evidence set rather than proxying arbitrary REST APIs:
`/overview`, `/config`, `/taskmanagers`, `/jobs`, job `status/config/plan`,
restart metrics, exceptions, and checkpoints.

For mutation commands, first collect all required parameters, show the final
command, and wait for explicit confirmation. When collecting parameters, do not
ask for bare flag names. Explain each parameter's meaning, where to get it, an
example value, and important caveats before asking. Add `--confirm` only after
that confirmation. Use `--dry-run` only when the user asks for preview or
validation without cluster mutation.

Local CLI, Java, and Flink runtime checks are exposed by
`check_cli_environment` and included in `k8s_preflight_start` for the
combined Kubernetes start check. YARN runtime checks remain part of
`preflight_start --deployment-target yarn`.

Kubernetes credentials are path-only. Pass `--kubeconfig-path <path>` when the
user provides a kubeconfig file path; otherwise the CLI uses `~/.kube/config`.
Never read or pass kubeconfig contents.

When an agent uses the Ingress Controller NodePort as the reachable endpoint,
use `--job-manager-url http://<node-ip>:<http-nodeport>` plus
`--http-host-header <job-name>.flink.k8s.com`. External users can still use the
domain URL directly.

For mutating operations, preserve the returned `targetLock.targetFingerprint`
when the user asks for a preview/dry-run. If the next execution uses
`--expected-target-lock`, the CLI fails with `TargetLockMismatch` when the
target changed.
