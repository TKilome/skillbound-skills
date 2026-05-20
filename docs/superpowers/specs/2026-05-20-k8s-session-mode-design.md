# Kubernetes Session Mode Design

## Goal

Add StreamPark-style Flink native Kubernetes session mode support to `flink-ops` without changing the existing Kubernetes application mode behavior.

## Current State

Kubernetes deployment currently supports only application mode through `k8s_start_job`.

The existing path builds a `FlinkKubernetesApplicationSpec`, sets `execution.target` to `kubernetes-application`, and submits through:

```text
KubernetesClusterClientFactory
  -> KubernetesClusterDescriptor
  -> deployApplicationCluster
```

Common REST commands already work for any Flink cluster when the JobManager REST URL is known. That includes Kubernetes session clusters after they are created or supplied by the user.

## Design Summary

Session mode should be modeled as two separate lifecycles:

1. Session cluster lifecycle
   Create and stop a long-running Flink Kubernetes session cluster.

2. Session job lifecycle
   Submit jobs into an existing session cluster and inspect or stop them through the common REST commands.

This matches StreamPark's model: create or reuse a session cluster first, then submit one or more jobs into that cluster.

The implementation must continue to use Flink Java Client APIs. It must not use `kubectl`, Flink shell scripts, `flink run`, Flink Operator CRDs, or handwritten Kubernetes manifests for submission.

## Command Model

Keep the existing command unchanged:

```text
k8s_start_job
```

It remains Kubernetes application mode.

Add session-specific commands:

```text
k8s_start_session_cluster
k8s_submit_session_job
k8s_get_session_cluster
k8s_stop_session_cluster
```

Command intent:

- `k8s_start_session_cluster`: create a native Kubernetes session cluster.
- `k8s_submit_session_job`: submit a job jar to an existing session cluster.
- `k8s_get_session_cluster`: retrieve and validate an existing session cluster by cluster id and namespace.
- `k8s_stop_session_cluster`: shut down an existing session cluster.

Do not overload `k8s_start_job` with a mode flag in the first implementation. Dedicated commands avoid ambiguity and preserve existing behavior.

## Java Client Flow

### Start Session Cluster

Use the same classpath style as application mode:

```bash
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*" \
  com.skill.flinkops.FlinkOpsCli k8s_start_session_cluster ...
```

Internal flow:

```text
KubernetesClusterClientFactory
  -> createClusterDescriptor(configuration)
  -> getClusterSpecification(configuration)
  -> KubernetesClusterDescriptor.deploySessionCluster(clusterSpecification)
  -> ClusterClientProvider.getClusterClient()
  -> clusterId, webInterfaceURL
```

The Flink configuration must set:

```text
execution.target = kubernetes-session
kubernetes.cluster-id = <name>
kubernetes.namespace = <namespace>
kubernetes.service-account = <service-account>
kubernetes.container.image.ref = <flink-image>
```

Resource options should reuse the existing application mode option names:

```text
jobmanager.memory.process.size
taskmanager.memory.process.size
taskmanager.numberOfTaskSlots
parallelism.default
kubernetes.config.file
```

### Submit Session Job

Use:

```text
KubernetesClusterClientFactory
  -> createClusterDescriptor(configuration)
  -> retrieve(clusterId)
  -> ClusterClientProvider.getClusterClient()
  -> PackagedProgram
  -> ClientUtils.submitJob(...)
```

The submit path should use Flink Java Client APIs instead of REST jar upload in the first version. The job jar must be visible to the submitting runtime or resolvable by Flink as a URI. For local user jars in Kubernetes workflows, keep the existing image packaging rule and use:

```text
local:///opt/flink/usrlib/<jar-file-name>
```

The submit implementation should return:

```json
{
  "clusterId": "...",
  "jobId": "...",
  "jobManagerUrl": "...",
  "deploymentTarget": "kubernetes-session"
}
```

### Stop Session Cluster

Retrieve the cluster client and call the Java Client shutdown path:

```text
KubernetesClusterClientFactory
  -> createClusterDescriptor(configuration)
  -> retrieve(clusterId)
  -> ClusterClient.shutDownCluster()
```

The stop command is a cluster-level mutation. It is separate from `stop_job`, which remains a job-level REST operation.

## Parameters

### k8s_start_session_cluster

Required:

```text
--name <session cluster id>
--namespace "$K8S_NAMESPACE"
--service-account "$K8S_SERVICE_ACCOUNT"
--flink-image <image>
--flink-home "$FLINK_HOME"
```

Optional:

```text
--kubeconfig-path "$KUBECONFIG_PATH"
--jobmanager-memory <memory>
--taskmanager-memory <memory>
--taskmanager-slots <n>
--parallelism <n>
--dynamic-properties <k=v,...>
--enable-ingress <true|false>
--verify
--expected-target-lock <fingerprint>
```

`--name` maps to `kubernetes.cluster-id` and is also the session cluster id used by submit and stop commands.

### k8s_submit_session_job

Required:

```text
--cluster-id <session cluster id>
--namespace "$K8S_NAMESPACE"
--flink-home "$FLINK_HOME"
--jar-uri <jar URI visible to the submit/runtime path>
--parallelism <n>
```

Optional:

```text
--kubeconfig-path "$KUBECONFIG_PATH"
--main-class <class>
--args <job args>
--savepoint-path <path>
--dynamic-properties <k=v,...>
--job-manager-url <url>
--http-host-header <host>
--verify
--expected-target-lock <fingerprint>
```

`--main-class` may be omitted only when the jar manifest contains the correct entry class or when Flink can infer it.

### k8s_get_session_cluster

Required:

```text
--cluster-id <session cluster id>
--namespace "$K8S_NAMESPACE"
--flink-home "$FLINK_HOME"
```

Optional:

```text
--kubeconfig-path "$KUBECONFIG_PATH"
--job-manager-url <url>
--http-host-header <host>
--report
```

### k8s_stop_session_cluster

Required:

```text
--cluster-id <session cluster id>
--namespace "$K8S_NAMESPACE"
--flink-home "$FLINK_HOME"
```

Optional:

```text
--kubeconfig-path "$KUBECONFIG_PATH"
--expected-target-lock <fingerprint>
--verify
```

## Ingress and REST Access

For `k8s_start_session_cluster`, reuse the existing namespace-scoped Ingress model:

```text
<cluster-id>.flink.k8s.com
```

The generated backend service should target the session cluster REST service:

```text
<cluster-id>-rest
```

If `--enable-ingress false`, return the internal REST URL:

```text
http://<cluster-id>-rest.<namespace>.svc.cluster.local:8081
```

For `k8s_submit_session_job`, do not create a new Ingress. Use the cluster-level REST URL returned by session cluster creation, supplied by the user, or derived from the same session cluster service naming rule for internal verification.

## Files To Change

Create:

```text
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/provider/kubernetes/FlinkKubernetesSessionClusterSpec.java
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/provider/kubernetes/FlinkKubernetesSessionJobSpec.java
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/provider/kubernetes/FlinkNativeKubernetesSessionDeployer.java
skills/flink-ops/references/providers/kubernetes-session.md
skills/flink-ops/schemas/k8s_start_session_cluster.schema.json
skills/flink-ops/schemas/k8s_submit_session_job.schema.json
skills/flink-ops/schemas/k8s_get_session_cluster.schema.json
skills/flink-ops/schemas/k8s_stop_session_cluster.schema.json
```

Modify:

```text
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/cli/CommandNames.java
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/FlinkOpsCli.java
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/workflow/StartJobWorkflow.java
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/provider/kubernetes/KubernetesIngressSpec.java
skills/flink-ops/scripts/src/main/java/com/skill/flinkops/flink/rest/FlinkRuntimeInspector.java
skills/flink-ops/scripts/src/test/java/com/skill/flinkops/FlinkOpsCliTest.java
skills/flink-ops/SKILL.md
skills/flink-ops/references/command-map.md
skills/flink-ops/references/command-catalog.md
skills/flink-ops/references/product-model.md
skills/flink-ops/references/providers/kubernetes.md
```

`StartJobWorkflow` may keep command orchestration initially, but if the file becomes too broad, split session-specific methods into `KubernetesSessionWorkflow`.

## Validation and Safety

All session mutations require the existing confirmation flow:

```text
SafetyGate.requireConfirm(...)
```

Dry-run must be supported for:

```text
k8s_start_session_cluster
k8s_submit_session_job
k8s_stop_session_cluster
```

Every mutation must return `targetLock.targetFingerprint`. The target lock should include:

```text
command
namespace
clusterId
jobId when applicable
jobManagerUrl when applicable
```

Classpath errors must mirror the existing application mode error shape:

```text
FlinkRuntimeClasspathMissing
requiredClasspath = $SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*
```

Do not fall back to external CLIs when Java Client APIs fail.

## Testing Plan

Add dry-run tests first:

- `k8s_start_session_cluster --dry-run` returns `deploymentTarget = kubernetes-session`.
- `k8s_start_session_cluster --dry-run` returns `deployMethod = deploySessionCluster`.
- `k8s_submit_session_job --dry-run` returns `clusterId`, `jarUri`, `parallelism`, and `deploymentTarget = kubernetes-session`.
- `k8s_stop_session_cluster --dry-run` returns a target lock and shutdown plan.
- Missing `--cluster-id` for submit and stop returns a validation error.
- Existing `k8s_start_job` tests still return `kubernetes-application`.

Add provider-level tests with fake or overridden deployer methods:

- start session invokes the session cluster deployer path.
- submit session invokes the retrieve-and-submit path.
- stop session invokes the retrieve-and-shutdown path.
- classpath failures are converted to `FlinkRuntimeClasspathMissing`.

Integration testing against a real cluster should be opt-in, controlled by an environment flag, and not required for normal unit test runs.

## Rollout Order

1. Add command names, schemas, docs, and dry-run plans.
2. Implement `FlinkKubernetesSessionClusterSpec`.
3. Implement `k8s_start_session_cluster` dry-run and tests.
4. Implement `FlinkNativeKubernetesSessionDeployer.deploySessionCluster`.
5. Add Ingress handling for session clusters.
6. Implement `FlinkKubernetesSessionJobSpec`.
7. Implement `k8s_submit_session_job` dry-run and tests.
8. Implement retrieve-and-submit through Flink Java Client.
9. Implement `k8s_get_session_cluster`.
10. Implement `k8s_stop_session_cluster`.
11. Update SKILL and provider references so agents route application and session mode correctly.

## Non-Goals

Do not implement these in the first version:

- Flink Operator session deployments.
- External `kubectl` discovery or deletion.
- REST jar upload as the primary submit path.
- Automatic discovery of every session cluster in a namespace.
- Dynamic artifact upload to object storage.
- Multi-job batch submission.
- Session cluster autoscaling policy management.

## Self-Review

- The design preserves existing application mode semantics.
- The design introduces dedicated session commands instead of a broad mode flag.
- The Java Client path mirrors StreamPark's two-stage session model.
- Safety rules, dry-run, confirmation, and target locks are included.
- Scope is focused enough for one implementation plan.
