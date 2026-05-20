# Safety Rules

Read commands:

- `k8s_preflight_start`
- `k8s_render_ingress_controller_yaml`
- `get_job_status`
- `diagnose_job`
- `get_exceptions`
- `get_checkpoints`

Read commands can run without `--confirm`.

Manual remediation commands returned in error data are not agent-executable
commands. In particular, `helmCommands`, `kubectlManualRenderCommands`, and
`kubectlManualApplyCommands` must be shown to the user, not executed by the
agent. The agent must not use shell redirection to create the YAML file for the
user and must not run `kubectl apply`.

Mutation commands:

- `k8s_build_image`
- `k8s_start_job`
- `stop_job`

Mutation commands must include `--confirm`, but the agent must not add
`--confirm` until after it has shown the exact final command and the user has
explicitly approved execution in the current conversation. The Java CLI enforces
this and returns `SafetyCheckRequired` if the flag is missing.

Kubernetes start uses Flink Java Client APIs and the active Kubernetes
credentials. The user must provide `--name` and `--flink-image`; the agent must
use injected `$K8S_NAMESPACE` and `$K8S_SERVICE_ACCOUNT` for namespace and
service account instead of asking the user for them. The agent must not use
example values or hidden defaults for these fields. For a user-provided local
jar, `--jar-uri` is derived from the fixed image location
`local:///opt/flink/usrlib/<local-jar-file-name>` after `k8s_build_image`, not
chosen by the user. The CLI maps these values to Flink `Configuration`.

For Kubernetes `k8s_start_job`, creating or updating the Flink REST Ingress is part
of the same mutation. It must only happen after explicit confirmation and must
use the Java Kubernetes Client, not `kubectl` or Helm.

If the user explicitly sets `--enable-ingress false`, `k8s_start_job` must not check
Ingress Controller readiness, must not check IngressClass, and must not create
or update Ingress. The job is then accessible only through internal Service DNS
or another user-managed access path.

Destructive commands are out of scope for the first version. Do not delete
savepoints, namespaces, services, pods, PVCs, or other Kubernetes resources with
this skill.

Provider fallback is forbidden. If a provider returns `ProviderNotImplemented`
or an unknown provider returns `UnsupportedProvider`, do not call external
`yarn`, `flink`, `kubectl`, cloud CLIs, or shell scripts to continue the
operation.

Never place credentials in command arguments. Use kubeconfig or in-cluster
configuration provided by the host environment.
