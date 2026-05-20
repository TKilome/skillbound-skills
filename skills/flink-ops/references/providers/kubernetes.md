# Kubernetes Provider

Status: implemented

Use this reference for explicit Kubernetes provider commands named `k8s_*`.

## Runtime

Kubernetes start uses the bundled CLI plus Flink runtime libraries:

```bash
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*" \
  com.skill.flinkops.FlinkOpsCli k8s_start_job [args]
```

Do not use these in any Kubernetes provider scenario, including discovery,
preflight, submission, verification, logs, status, and troubleshooting:

- `kubectl`
- hand-written Kubernetes manifests
- Flink Operator CRDs
- `flink run-application`

The implementation follows the StreamPark-style Flink Java Client path:

```text
KubernetesClusterClientFactory
  -> KubernetesClusterDescriptor
  -> deployApplicationCluster
```

Use `k8s_check_ingress_controller --namespace "$K8S_NAMESPACE"` to check the
namespace-scoped Ingress Controller before starting a Flink job. This check
decides the `--enable-ingress` value; do not ask the user for that value before
checking. `k8s_start_job` still runs a final protective check before submission
because cluster state can change.

When the namespace-scoped Controller Service exists, `k8s_check_ingress_controller`
also returns `ingressControllerService` with Service `type` and port details,
including `nodePort` for `http` and `https`. Use this output to answer
questions such as "Ingress Controller 的 NodePort 端口是多少？". Do not use
`kubectl get svc` for this.

For agent-side REST access without DNS, use the NodePort endpoint with the
Ingress host header:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" get_job_status \
  --job-manager-url http://<node-ip>:<http-nodeport> \
  --http-host-header <job-name>.flink.k8s.com
```

External users should continue to use:

```text
https://<job-name>.flink.k8s.com
```

Use `k8s_get_node_ips` to read Kubernetes node addresses through the Java
Kubernetes Client when the agent needs a `<node-ip>` for NodePort access. Prefer
`InternalIP` for an agent running on the cluster network; use `ExternalIP` only
when the agent is outside that network.

Use `k8s_preflight_start --namespace "$K8S_NAMESPACE"
--service-account "$K8S_SERVICE_ACCOUNT" --flink-home "$FLINK_HOME" --kubeconfig-path "$KUBECONFIG_PATH" --enable-ingress
<derived>` for Kubernetes start readiness checks after the Ingress Controller
decision is known. Do not run external preflight commands.

Do not assume an earlier Ingress Controller or runtime check is still known if
it is not present in the current visible conversation context. If the check
result may have been lost during a long conversation, rerun the deployment
readiness flow: `k8s_check_ingress_controller --namespace "$K8S_NAMESPACE"`, then
`k8s_preflight_start` with the derived `--enable-ingress` value. Do not substitute
another command for deployment readiness.

For a new Kubernetes deployment request, do not start by asking for all
`k8s_start_job` parameters. Ask for only one category of parameters per user turn.
Do not ask for injected readiness inputs. Use these environment variables for
the readiness checks:

```text
$K8S_NAMESPACE
$K8S_SERVICE_ACCOUNT
$KUBECONFIG_PATH
$FLINK_HOME
```

Then run:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_check_ingress_controller \
  --namespace "$K8S_NAMESPACE"
```

Do not ask for `name`, `parallelism`, `flink-image`, `main-class`, build image
choices, or command confirmation in this first question. Those are separate
categories and must be asked only after readiness is known.

If an Ingress Controller exists, continue with `--enable-ingress true`. If it is
missing, present the remediation YAML/commands returned by the CLI and ask
whether the user wants to create the namespace-scoped Ingress Controller. The
agent must not apply the YAML itself. If the user declines or wants internal
access only, continue with `--enable-ingress false`.

Run:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_preflight_start \
  --namespace "$K8S_NAMESPACE" \
  --service-account "$K8S_SERVICE_ACCOUNT" \
  --flink-home "$FLINK_HOME" \
  --kubeconfig-path "$KUBECONFIG_PATH" \
  --enable-ingress <true|false>
```

Only after this check should the agent collect `--name`, `--flink-image`,
`--main-class`, `--parallelism`, and optional application arguments. For a
user-provided local jar, derive `--jar-uri` from
`local:///opt/flink/usrlib/<local-jar-file-name>` after `k8s_build_image`; do not
ask the user to choose it.

When showing the final start command, use the `$JAVA_HOME/bin/java -cp` form from
this reference and the injected `$FLINK_HOME` value.

A local jar path in the user's request does not imply a local standalone Flink
cluster. For Kubernetes deployment, treat the local path as packaging input:
explain that containers cannot see the agent machine path directly, then use an
image-internal `local:///opt/flink/usrlib/<local-jar-file-name>` jar URI after
`k8s_build_image` before `k8s_start_job`.

## Parameters

Required Kubernetes parameters:

```text
--name <kubernetes.cluster-id>
--namespace "$K8S_NAMESPACE"
--service-account "$K8S_SERVICE_ACCOUNT"
--flink-image <Flink image>
```

Do not invent required Kubernetes parameters from examples. Use injected
`namespace` and `service-account` values from `$K8S_NAMESPACE` and
`$K8S_SERVICE_ACCOUNT`. If `flink-image` or `name` is missing, ask for the
missing fields instead of running local discovery or using hidden defaults. For a
user-provided local jar, do not ask for `jar-uri`; derive it from the fixed
`/opt/flink/usrlib/<jar-file-name>` image path. Do not ask for bare flag names.
Explain each missing parameter before asking for its value.

Use this explanation template for missing Kubernetes start parameters:

```text
启动 Flink Kubernetes 任务还缺这些参数：

1. --name
   含义：Flink application 名称，会映射为 kubernetes.cluster-id。
   影响：REST Service 默认是 <name>-rest，Ingress 域名是 <name>.flink.k8s.com。
   示例：flink-example-streaming

2. --flink-image
   含义：运行 Flink 任务的容器镜像。
   要求：镜像里要有 Flink runtime；如果 jar-uri 使用 local://，业务 jar 也必须在这个镜像内。
   示例：registry.example.com/flink/orders:1.0.0

3. --jar-uri
   含义：Flink 容器内可见的 job jar 路径。
   注意：local:// 是容器内路径，不是你本机路径。
   固定规则：用户提供本机 jar 时，先通过 k8s_build_image 打进镜像，默认放到 /opt/flink/usrlib/<jar文件名>。
   交互要求：不要让用户选择 jar-uri，也不要推荐 /opt/flink/examples 路径；根据本机 jar 文件名自动派生。
   示例：local:///opt/flink/usrlib/orders.jar

4. --parallelism
   含义：Flink job 初始并行度。
   示例：4
```

For a local jar request, the provider must still ask for the real Kubernetes
parameters:

```text
name
flink-image
```

Derive `--jar-uri` from the local jar file name after `k8s_build_image`. Do not ask
the user to choose it. For example, this local jar:

```text
/tmp/orders.jar
```

maps to:

```text
local:///opt/flink/usrlib/orders.jar
```

Do not submit until injected values are present and the user has provided or
approved every required non-injected value.

Common optional parameters:

```text
--main-class <application main class>
--parallelism <parallelism>
--enable-ingress <true|false>
--dynamic-properties <comma-separated Flink key=value entries>
```

`--enable-ingress` defaults to `true`. If the user has no Ingress Controller and
does not want to create one, set `--enable-ingress false`. In that mode,
`k8s_start_job` skips Ingress Controller and IngressClass checks, does not create an
Ingress, and returns the internal REST Service URL:

```text
http://<name>-rest.<namespace>.svc.cluster.local:8081
```

Before starting a custom business jar, ask about the application entry class:

```text
--main-class
含义：Flink application 的 Java 主类全路径。
什么时候需要：如果 jar 的 manifest 没有声明入口类，或者你想显式指定入口类，就必须提供。
如果不确定：请确认你的 jar 是否是可执行 Flink application jar；否则提供完整类名。
示例：com.example.OrdersJob
```

Do not silently omit `--main-class` for local jars unless the user confirms the
jar manifest already contains the correct entry class.

`local://` jar URIs are paths inside the Flink image container, not paths on the
agent machine. If the business jar is built into an image, still pass the
container path. The recommended default business jar directory is
`/opt/flink/usrlib`, so the usual jar URI is
`local:///opt/flink/usrlib/<jar-file-name>`, for example
`local:///opt/flink/usrlib/app.jar`.

## Mandatory Ingress

Kubernetes `k8s_start_job` must create or update an Ingress for the Flink REST
Service. The user namespace is the isolation boundary:

```text
Flink application namespace = Ingress namespace = expected Ingress Controller namespace
```

The default IngressClass is namespace-specific:

```text
<namespace>-nginx
```

The external host is:

```text
<name>.flink.k8s.com
```

The backend service is:

```text
<name>-rest:8081
```

The namespace-scoped Ingress Controller Service created by this skill is:

```text
<namespace>-nginx-controller
```

It is a `NodePort` Service. Kubernetes assigns the concrete `nodePort` values
unless the YAML is edited before apply. Query them with:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_check_ingress_controller \
  --namespace "$K8S_NAMESPACE"
```

The agent-facing URL is `http://<node-ip>:<http-nodeport>`. The user-facing URL
is `https://<name>.flink.k8s.com`. Use
`--http-host-header <name>.flink.k8s.com` when the agent talks to the NodePort
URL.

If the namespace-scoped Ingress Controller is missing, the CLI must not install
it. It returns `IngressControllerNotFound` with suggested setup commands. These
commands are remediation guidance only; do not run Helm or `kubectl` as part of
this skill workflow.

When handling `IngressControllerNotFound`, present both paths when possible:

- `helmCommands`: shorter manual Helm commands for users who have Helm.
- `kubectlTemplatePath`: built-in YAML template file under
  `assets/kubernetes/ingress-controller.yaml.tpl`.
- `kubectlManualRenderCommands`: manual command that renders the built-in
  template for the namespace.
- `kubectlManualApplyCommands`: manual `kubectl apply` and follow-up check
  commands.

Do not show only Helm. The agent must not run `kubectl apply` automatically.
The built-in YAML template should keep the Controller scoped to the user
namespace by setting:

```text
--watch-namespace=<namespace>
--ingress-class=<namespace>-nginx
--controller-class=k8s.io/<namespace>-nginx
```

The recommended YAML should match the namespace-scoped Nginx controller shape:
ServiceAccount, Role, RoleBinding, ClusterRole, ClusterRoleBinding,
IngressClass, Controller Deployment, and Controller Service. The ClusterRole
must include `namespaces` `get` and `ingressclasses` `get/list/watch`. The
namespaced Role must include `discovery.k8s.io` `endpointslices`
`get/list/watch`. The controller container must set `POD_NAME` and
`POD_NAMESPACE` from field refs and must use `--watch-namespace=<namespace>`,
`--ingress-class=<namespace>-nginx`, and
`--controller-class=k8s.io/<namespace>-nginx`.

If the user chooses the kubectl install path, recommend this manual
render/apply flow:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_render_ingress_controller_yaml \
  --namespace "$K8S_NAMESPACE" > /tmp/${K8S_NAMESPACE}-nginx-controller.yaml
kubectl apply -f /tmp/${K8S_NAMESPACE}-nginx-controller.yaml
```

After the user applies it, rerun:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_check_ingress_controller \
  --namespace "$K8S_NAMESPACE"
```

The agent must not create the YAML file, redirect command output to a YAML file,
pipe YAML into `kubectl`, or run `kubectl apply`, even after the user chooses
the kubectl option. The user can run the displayed commands themselves after
reviewing them or ask their platform administrator to apply them. After the user
says they applied the YAML, the agent may rerun `k8s_check_ingress_controller`.

## Local Jar Packaging

If the user has a local jar that is not already inside the Flink image, do not
submit it directly as `local://<agent-machine-path>`. First build a Flink image
with the jar copied into the image by using this skill's `k8s_build_image` command.
Do not run `docker build` directly.

Required packaging parameters:

```text
--base-image <Flink base image>
--local-jar <jar path on the agent machine>
--target-image <image tag to build>
[--target-jar-path <absolute jar path inside the image>]
```

If `--target-jar-path` is omitted, the CLI copies the jar to:

```text
/opt/flink/usrlib/<local-jar-file-name>
```

The resulting `--jar-uri` is:

```text
local:///opt/flink/usrlib/<local-jar-file-name>
```

Packaging template:

```bash
"$JAVA_HOME/bin/java" -jar "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar" k8s_build_image \
  --base-image <user-provided base image> \
  --local-jar <user-provided local jar> \
  --target-image <user-provided target image>
```

Show the final `k8s_build_image` command and get explicit confirmation before
running. Add `--confirm` only after that confirmation. After a successful build,
start the job with:

```text
--flink-image <target image>
--jar-uri local:///opt/flink/usrlib/<local-jar-file-name>
```

## Tool Boundary

`kubectl` is always forbidden for this provider. Do not run it and do not ask
the user to run it as part of this skill workflow.

The only allowed execution surface is this skill's Java CLI. The CLI submits
through Flink Java Client APIs and uses Kubernetes credentials available to that
client. If cluster context, namespace, service account, RBAC, logs, events, or
pod state cannot be verified through the CLI, report that this skill does not
currently expose that capability.

The JobManager URL returned by Kubernetes application mode may be a cluster DNS
name, for example `http://<cluster-id>-rest.<namespace>:8081`. The REST read
commands can use it only when that URL is reachable from the agent process. Do
not use `kubectl port-forward` to make it reachable.

## Local Jar Start Template

```bash
"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*" \
  com.skill.flinkops.FlinkOpsCli k8s_start_job \
  --name <user-provided name> \
  --namespace "$K8S_NAMESPACE" \
  --service-account "$K8S_SERVICE_ACCOUNT" \
  --flink-image <user-provided flink image> \
  --jar-uri local:///opt/flink/usrlib/<local-jar-file-name> \
  --main-class <user-provided-or-confirmed-main-class> \
  --parallelism 1 \
  --dynamic-properties kubernetes.container.image.pull-policy=IfNotPresent,jobmanager.memory.process.size=1024m,taskmanager.memory.process.size=1024m,taskmanager.numberOfTaskSlots=1
```

This is a template, not default production input. Replace every
`<user-provided ...>` placeholder while preserving injected environment
placeholders, show the final command, and get explicit confirmation before
running. Add `--confirm` only after that confirmation.
