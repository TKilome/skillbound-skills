# Command Catalog

## Implemented Commands

### Common Flink REST Commands

These commands require a reachable JobManager REST URL and do not require
`--deployment-target`.

| Command | Category | Provider | Safety |
|---------|----------|----------|--------|
| `get_job_status` | Common REST | none | read |
| `inspect_cluster` | Common REST inspection | none by default | read |
| `diagnose_job` | Common REST | none | read |
| `diagnose_backpressure` | Common REST diagnosis | none | read |
| `get_exceptions` | Common REST | none | read |
| `get_checkpoints` | Common REST | none | read |
| `stop_job` | Common REST mutation | none | mutation |

`inspect_cluster` may include Kubernetes exposure checks only when
`--deployment-target kubernetes --namespace "$K8S_NAMESPACE"` is explicitly
provided.

### Kubernetes Provider Commands

These commands use explicit `k8s_*` names and do not require
`--deployment-target`.

| Command | Category | Provider | Safety |
|---------|----------|----------|--------|
| `k8s_check_ingress_controller` | Provider read | kubernetes | read |
| `k8s_check_connectivity` | Deployment target connectivity read | kubernetes | read |
| `yarn_check_connectivity` | Deployment target connectivity read | yarn | read |
| `check_cli_environment` | Local CLI execution environment read | flink | read |
| `k8s_get_node_ips` | Provider read | kubernetes | read |
| `k8s_render_ingress_controller_yaml` | Provider read | kubernetes | read |
| `k8s_preflight_start` | Provider read | kubernetes | read |
| `k8s_build_image` | Provider packaging | kubernetes | mutation |
| `k8s_start_job` | Provider mutation | kubernetes | mutation |

### YARN Provider Commands

YARN application mode is implemented through `start_job --deployment-target yarn`
and `preflight_start --deployment-target yarn`.

## Inspection and Safety Enhancements

- `inspect_cluster` performs one-shot inspection across Kubernetes exposure
  checks and Flink 1.19 REST evidence when those inputs are provided. It reads
  `/overview`, `/config`, `/taskmanagers`, `/jobs`, and job-specific
  `status/config/plan/metrics/exceptions/checkpoints` endpoints when a job ID
  is provided. Use `--report` to return `healthStatus`, `riskLevel`,
  `findings`, `rootCauseCandidates`, and `recommendations`.
- `diagnose_job --report` returns a structured diagnosis report instead of only
  raw REST responses. It flags failed states, non-null root exceptions,
  checkpoint timeout/failure signals, exhausted slots, blocked TaskManagers, and
  restart metrics.
- `diagnose_backpressure` follows Flink 1.19 REST semantics: it reads
  `/jobs/:jobid`, discovers vertices, then checks
  `/jobs/:jobid/vertices/:vertexid/backpressure` and
  `/jobs/:jobid/vertices/:vertexid/subtasks/metrics` for
  `backPressuredTimeMsPerSecond`, `busyTimeMsPerSecond`, and
  `idleTimeMsPerSecond`, plus exceptions and checkpoints. It returns `unknown`
  when vertex or metric-level evidence is unavailable.
- `k8s_start_job --verify` and `stop_job --verify` perform read-back verification
  after mutation.
- `k8s_start_job` and `stop_job` return `targetLock.targetFingerprint`; pass it back
  as `--expected-target-lock <fingerprint>` to prevent target drift.
- `k8s_check_connectivity` checks the Kubernetes deployment target from a
  kubeconfig file path only. If `--kubeconfig-path` is omitted, the CLI uses
  `~/.kube/config`. Kubeconfig contents are loaded directly into the Kubernetes
  SDK client and are never returned in CLI output.
- `yarn_check_connectivity` checks the YARN deployment target boundary:
  Hadoop/YARN config path, ResourceManager reachability, queue, provided lib
  dirs, distribution jar, HDFS/staging, and security posture. The first
  implementation reports advisory dry-run structure without external CLI use.
- `check_cli_environment` checks the local CLI execution chain:
  `FlinkOpsCli`, Java, `--flink-home`, and runtime classpath.

## Planned Commands or Provider Paths

| Capability | Status |
|------------|--------|
| explicit `yarn_start_job` alias | planned |
| explicit `yarn_preflight_start` alias | planned |
| provider-aware YARN stop | common REST `stop_job` |

## Unsupported

- Flink SQL Gateway operations.
- Flink instance lifecycle operations.
- Cluster installation or upgrade.
- Pod-level troubleshooting outside `FlinkOpsCli`.
- External CLI fallback through `kubectl`, `flink`, or `yarn`.
