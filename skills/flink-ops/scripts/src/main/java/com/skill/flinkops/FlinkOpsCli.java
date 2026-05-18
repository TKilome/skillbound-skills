package com.skill.flinkops;

import com.skill.flinkops.common.ApiResponse;
import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.Json;
import com.skill.flinkops.common.SafetyGate;
import com.skill.flinkops.common.errors.OperationException;
import com.skill.flinkops.common.errors.SafetyException;
import com.skill.flinkops.common.errors.ValidationException;
import com.skill.flinkops.cli.CommandNames;
import com.skill.flinkops.cli.CommandRouter;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;
import com.skill.flinkops.flink.compat.rest.EndpointSpec;
import com.skill.flinkops.flink.compat.rest.RestDialect;
import com.skill.flinkops.flink.rest.FlinkRestClient;
import com.skill.flinkops.flink.rest.FlinkRuntimeInspector;
import com.skill.flinkops.image.FlinkImageBuildSpec;
import com.skill.flinkops.image.FlinkImageBuilder;
import com.skill.flinkops.provider.kubernetes.FlinkKubernetesApplicationSpec;
import com.skill.flinkops.provider.kubernetes.FlinkNativeKubernetesDeployer;
import com.skill.flinkops.provider.kubernetes.KubernetesEnvironmentChecker;
import com.skill.flinkops.provider.kubernetes.KubernetesGateway;
import com.skill.flinkops.provider.kubernetes.KubernetesIngressSpec;
import com.skill.flinkops.provider.kubernetes.KubernetesRemediation;
import com.skill.flinkops.provider.kubernetes.LazyKubernetesGateway;
import com.skill.flinkops.provider.yarn.FlinkYarnApplicationDeployer;
import com.skill.flinkops.provider.yarn.FlinkYarnApplicationSpec;
import com.skill.flinkops.workflow.DiagnoseJobWorkflow;
import com.skill.flinkops.workflow.InspectClusterWorkflow;
import com.skill.flinkops.workflow.StartJobWorkflow;
import com.skill.flinkops.workflow.report.HealthReportBuilder;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.lang.reflect.InvocationTargetException;

public class FlinkOpsCli {
    private static final String PROVIDER_KUBERNETES = "kubernetes";
    private static final String PROVIDER_YARN = "yarn";

    private final FlinkRestClient restClient;
    private final FlinkNativeKubernetesDeployer kubernetesDeployer;
    private final FlinkYarnApplicationDeployer yarnApplicationDeployer;
    private final FlinkImageBuilder imageBuilder;
    private final FlinkRuntimeInspector runtimeInspector;
    private final KubernetesGateway kubernetesGateway;
    private final KubernetesEnvironmentChecker kubernetesChecker;
    private final StartJobWorkflow startJobWorkflow;
    private final InspectClusterWorkflow inspectClusterWorkflow;
    private final DiagnoseJobWorkflow diagnoseJobWorkflow;
    private final HealthReportBuilder healthReportBuilder;

    public FlinkOpsCli(FlinkRestClient restClient) {
        this(restClient, new LazyKubernetesGateway());
    }

    FlinkOpsCli(FlinkRestClient restClient, KubernetesGateway kubernetesGateway) {
        this.restClient = restClient;
        this.kubernetesDeployer = new FlinkNativeKubernetesDeployer();
        this.yarnApplicationDeployer = new FlinkYarnApplicationDeployer();
        this.imageBuilder = new FlinkImageBuilder();
        this.runtimeInspector = new FlinkRuntimeInspector();
        this.kubernetesGateway = kubernetesGateway;
        this.kubernetesChecker = new KubernetesEnvironmentChecker(kubernetesGateway);
        this.healthReportBuilder = new HealthReportBuilder();
        this.startJobWorkflow = new StartJobWorkflow(
                this.kubernetesDeployer,
                this.yarnApplicationDeployer,
                this.imageBuilder,
                this.runtimeInspector,
                this.kubernetesGateway,
                this.kubernetesChecker,
                new StartJobWorkflow.Support() {
                    public Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) throws Exception {
                        return FlinkOpsCli.this.deployKubernetes(spec);
                    }

                    public Map<String, Object> deployYarnApplication(FlinkYarnApplicationSpec spec) throws Exception {
                        return FlinkOpsCli.this.deployYarnApplication(spec);
                    }

                    public Map<String, Object> verifyStarted(String jobManagerUrl, String jobId, String hostHeader) throws Exception {
                        return FlinkOpsCli.this.verifyStarted(jobManagerUrl, jobId, hostHeader);
                    }

                    public Map<String, Object> targetLock(String operation, String namespace, String name, String jobManagerUrl, String jobId, String hostHeader) {
                        return FlinkOpsCli.this.targetLock(operation, namespace, name, jobManagerUrl, jobId, hostHeader);
                    }

                    public void enforceExpectedTargetLock(CommandContext context, Map<String, Object> targetLock) {
                        FlinkOpsCli.this.enforceExpectedTargetLock(context, targetLock);
                    }

                    public void requireImplementedDeploymentTarget(CommandContext context, String expected) {
                        FlinkOpsCli.this.requireImplementedDeploymentTarget(context, expected);
                    }

                    public String normalizeUrl(String value) {
                        return FlinkOpsCli.this.normalizeUrl(value);
                    }

                    public String httpHostHeader(CommandContext context) {
                        return FlinkOpsCli.this.httpHostHeader(context);
                    }
                });
        this.inspectClusterWorkflow = new InspectClusterWorkflow(new InspectClusterWorkflow.Support() {
            public String inspectCluster(CommandContext context) throws Exception {
                return FlinkOpsCli.this.inspectCluster(context);
            }
        });
        this.diagnoseJobWorkflow = new DiagnoseJobWorkflow(new DiagnoseJobWorkflow.Support() {
            public String diagnoseJob(CommandContext context) throws Exception {
                return FlinkOpsCli.this.diagnoseJob(context);
            }

            public String diagnoseBackpressure(CommandContext context) throws Exception {
                return FlinkOpsCli.this.diagnoseBackpressure(context);
            }
        });
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    public static int run(String[] args) {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient());
        return cli.runParsed(args);
    }

    int runParsed(String[] args) {
        CommandContext context = CommandContext.parse(args);
        try {
            String json = dispatch(context);
            if (json != null && !json.isEmpty()) {
                System.out.println(json);
            }
            return 0;
        } catch (OperationException e) {
            System.out.println(ApiResponse.error(operationName(context), e.code(), e.getMessage(), e.data(), e));
            return 1;
        } catch (ValidationException e) {
            System.out.println(ApiResponse.error(operationName(context), "ValidationError", e.getMessage(), e));
            return 1;
        } catch (SafetyException e) {
            System.out.println(ApiResponse.error(operationName(context), "SafetyCheckRequired", e.getMessage(), e));
            return 1;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            System.out.println(ApiResponse.error(
                    operationName(context),
                    cause == null ? "InvocationTargetException" : cause.getClass().getSimpleName(),
                    errorMessage(cause == null ? e : cause),
                    cause == null ? e : cause));
            return 1;
        } catch (Exception e) {
            System.out.println(ApiResponse.error(operationName(context), e.getClass().getSimpleName(), errorMessage(e), e));
            return 1;
        }
    }

    protected String dispatch(CommandContext context) throws Exception {
        return new CommandRouter()
                .register(CommandNames.START_JOB, startJobWorkflow::startJob)
                .register(CommandNames.K8S_START_JOB, startJobWorkflow::startJob)
                .register(CommandNames.BUILD_IMAGE, startJobWorkflow::buildImage)
                .register(CommandNames.K8S_BUILD_IMAGE, startJobWorkflow::buildImage)
                .register(CommandNames.PREFLIGHT_START, startJobWorkflow::preflightStart)
                .register(CommandNames.K8S_PREFLIGHT_START, startJobWorkflow::preflightStart)
                .register(CommandNames.K8S_CHECK_CONNECTIVITY, startJobWorkflow::k8sCheckConnectivity)
                .register(CommandNames.YARN_CHECK_CONNECTIVITY, startJobWorkflow::yarnCheckConnectivity)
                .register(CommandNames.CHECK_CLI_ENVIRONMENT, startJobWorkflow::checkCliEnvironment)
                .register(CommandNames.CHECK_INGRESS_CONTROLLER, this::checkIngressController)
                .register(CommandNames.K8S_CHECK_INGRESS_CONTROLLER, this::checkIngressController)
                .register(CommandNames.GET_NODE_IPS, this::getNodeIps)
                .register(CommandNames.K8S_GET_NODE_IPS, this::getNodeIps)
                .register(CommandNames.INSPECT_CLUSTER, inspectClusterWorkflow::inspectCluster)
                .register(CommandNames.RENDER_INGRESS_CONTROLLER_YAML, this::renderIngressControllerYaml)
                .register(CommandNames.K8S_RENDER_INGRESS_CONTROLLER_YAML, this::renderIngressControllerYaml)
                .register(CommandNames.STOP_JOB, this::stopJob)
                .register(CommandNames.GET_JOB_STATUS, this::getJobStatus)
                .register(CommandNames.DIAGNOSE_JOB, diagnoseJobWorkflow::diagnoseJob)
                .register(CommandNames.GET_EXCEPTIONS, this::getExceptions)
                .register(CommandNames.GET_CHECKPOINTS, this::getCheckpoints)
                .register(CommandNames.DIAGNOSE_BACKPRESSURE, diagnoseJobWorkflow::diagnoseBackpressure)
                .dispatch(context);
    }

    protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) throws Exception {
        try {
            return kubernetesDeployer.deploy(spec);
        } catch (ClassNotFoundException e) {
            if (e.getMessage() != null && e.getMessage().startsWith("org.apache.flink.")) {
                Map<String, Object> data = new LinkedHashMap<String, Object>();
                data.put("missingClass", e.getMessage());
                data.put("requiredClasspath", "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*");
                data.put("exampleCommand", "\"$JAVA_HOME/bin/java\" -cp \"$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*\" com.skill.flinkops.FlinkOpsCli k8s_start_job ...");
                throw new OperationException(
                        "FlinkRuntimeClasspathMissing",
                        "Kubernetes start requires Flink runtime jars on the JVM classpath. Run k8s_start_job with $JAVA_HOME/bin/java -cp including $FLINK_HOME/lib/*; java -jar cannot load these classes.",
                        data);
            }
            throw e;
        }
    }

    protected Map<String, Object> deployYarnApplication(FlinkYarnApplicationSpec spec) throws Exception {
        try {
            return yarnApplicationDeployer.deploy(spec);
        } catch (ClassNotFoundException e) {
            String missing = e.getMessage();
            if (missing != null && (missing.startsWith("org.apache.flink.") || missing.startsWith("org.apache.hadoop."))) {
                Map<String, Object> data = new LinkedHashMap<String, Object>();
                data.put("missingClass", missing);
                data.put("requiredClasspath", "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*:$HADOOP_CONF_DIR");
                data.put("exampleCommand", "\"$JAVA_HOME/bin/java\" -cp \"$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*\" com.skill.flinkops.FlinkOpsCli start_job --deployment-target yarn ...");
                throw new OperationException(
                        "FlinkRuntimeClasspathMissing",
                        "YARN application start requires Flink and Hadoop runtime jars on the JVM classpath. Run with $JAVA_HOME/bin/java -cp including $FLINK_HOME/lib/*; java -jar cannot load these classes.",
                        data);
            }
            throw e;
        }
    }

    private Map<String, Object> skippedIngressCheck() {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("enabled", Boolean.FALSE);
        data.put("message", "Ingress checks skipped by --enable-ingress=false.");
        return data;
    }

    private String checkIngressController(CommandContext context) throws Exception {
        requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        String namespace = context.require("namespace");
        return ApiResponse.success(context.command, checkIngressControllerData(namespace, context.dryRun));
    }

    private String renderIngressControllerYaml(CommandContext context) {
        requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        String namespace = context.require("namespace");
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from(namespace, "ingress-controller-render");
        System.out.println(new KubernetesRemediation().renderIngressControllerYaml(namespace, ingress.ingressClassName));
        return null;
    }

    private Map<String, Object> checkIngressControllerData(String namespace, boolean dryRun) throws Exception {
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from(namespace, "ingress-controller-check");
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("namespace", namespace);
        data.put("ingressClassName", ingress.ingressClassName);
        if (dryRun) {
            data.put("checks", kubernetesChecker.plannedIngressControllerChecks(namespace, ingress));
            return data;
        }
        data.put("checks", kubernetesChecker.checkIngressController(namespace, ingress));
        return data;
    }

    private String getNodeIps(CommandContext context) throws Exception {
        requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("addressTypes", Arrays.asList("InternalIP", "ExternalIP", "Hostname"));
        if (context.dryRun) {
            data.put("planned", Boolean.TRUE);
            data.put("message", "Read Kubernetes Node addresses through Java Kubernetes Client.");
            return ApiResponse.success(context.command, data);
        }
        data.put("nodes", kubernetesGateway.nodeAddresses());
        return ApiResponse.success(context.command, data);
    }

    private String inspectCluster(CommandContext context) throws Exception {
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String namespace = context.option("namespace");
        String deploymentTarget = context.option("deployment-target");
        String jobId = context.option("job-id");
        String hostHeader = httpHostHeader(context);
        boolean report = context.hasFlag("report");

        Map<String, Object> data = new LinkedHashMap<String, Object>();
        Map<String, Object> scope = new LinkedHashMap<String, Object>();
        scope.put("deploymentTarget", deploymentTarget == null ? "" : deploymentTarget);
        scope.put("namespace", namespace == null ? "" : namespace);
        scope.put("jobManagerUrl", jobManagerUrl);
        scope.put("jobId", jobId == null ? "" : jobId);
        putHostHeader(scope, hostHeader);
        data.put("inspectionScope", scope);
        data.put("reportRequested", Boolean.valueOf(report));

        List<String> endpoints = inspectionEndpoints(jobManagerUrl, jobId);
        data.put("endpoints", endpoints);
        if (namespace != null && namespace.trim().length() > 0 && PROVIDER_KUBERNETES.equals(deploymentTarget)) {
            if (context.dryRun) {
                data.put("kubernetes", checkIngressControllerData(namespace, true));
            } else {
                Map<String, Object> kubernetes = new LinkedHashMap<String, Object>();
                kubernetes.put("ingressController", checkIngressControllerData(namespace, false));
                kubernetes.put("nodes", kubernetesGateway.nodeAddresses());
                data.put("kubernetes", kubernetes);
            }
        }
        if (context.dryRun) {
            data.put("healthStatus", "planned");
            data.put("riskLevel", "unknown");
            return ApiResponse.success("inspect_cluster", data);
        }

        Map<String, Object> responses = collectResponses(endpoints, hostHeader);
        data.put("evidence", responses);
        data.putAll(healthReportBuilder.fromEvidence(responses, report ? "Cluster inspection completed." : "Cluster inspection data collected."));
        return ApiResponse.success("inspect_cluster", data);
    }

    private String stopJob(CommandContext context) throws Exception {
        SafetyGate.requireConfirm(context, "stop_job");
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String jobId = context.require("job-id");
        String stopMode = context.option("stop-mode");
        String hostHeader = httpHostHeader(context);
        if (stopMode == null || stopMode.trim().isEmpty()) {
            stopMode = "cancel";
        }
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("jobManagerUrl", jobManagerUrl);
        data.put("jobId", jobId);
        data.put("stopMode", stopMode);
        putHostHeader(data, hostHeader);
        Map<String, Object> targetLock = targetLock("stop_job", null, null, jobManagerUrl, jobId, hostHeader);
        enforceExpectedTargetLock(context, targetLock);
        data.put("targetLock", targetLock);
        String endpoint = stopEndpoint(jobManagerUrl, jobId, stopMode);
        data.put("endpoint", endpoint);
        String requestBody = stopRequestBody(stopMode, context);
        if (requestBody != null) {
            data.put("requestBody", requestBody);
        }
        if (context.dryRun) {
            return ApiResponse.success("stop_job", data);
        }
        if ("cancel".equals(stopMode)) {
            data.put("responseJson", restClient.patch(endpoint, hostHeader));
        } else {
            data.put("responseJson", restClient.postJson(endpoint, requestBody, hostHeader));
        }
        if (context.hasFlag("verify")) {
            data.put("readBackVerification", verifyStopped(jobManagerUrl, jobId, hostHeader));
        }
        return ApiResponse.success("stop_job", data);
    }

    private String getJobStatus(CommandContext context) throws Exception {
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String jobId = context.option("job-id");
        String hostHeader = httpHostHeader(context);
        RestDialect dialect = restDialect(context);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        String endpoint = jobId == null || jobId.trim().isEmpty()
                ? jobManagerUrl + "/jobs"
                : endpoint(jobManagerUrl, dialect.jobStatus(jobId));
        data.put("jobManagerUrl", jobManagerUrl);
        data.put("jobId", jobId == null ? "" : jobId);
        data.put("restDialect", dialect.getClass().getSimpleName());
        data.put("endpoint", endpoint);
        putHostHeader(data, hostHeader);
        if (context.dryRun) {
            return ApiResponse.success("get_job_status", data);
        }
        data.put("responseJson", restClient.get(endpoint, hostHeader));
        return ApiResponse.success("get_job_status", data);
    }

    private String diagnoseJob(CommandContext context) throws Exception {
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String jobId = context.option("job-id");
        String hostHeader = httpHostHeader(context);
        RestDialect dialect = restDialect(context);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("jobManagerUrl", jobManagerUrl);
        data.put("jobId", jobId == null ? "" : jobId);
        data.put("restDialect", dialect.getClass().getSimpleName());
        putHostHeader(data, hostHeader);
        List<String> checks = Arrays.asList(
                "jobmanager overview",
                "job list or job detail",
                "job exceptions when job-id is provided",
                "checkpoint status when job-id is provided");
        data.put("checks", checks);
        List<String> endpoints = diagnosticEndpoints(jobManagerUrl, jobId, dialect);
        data.put("endpoints", endpoints);
        if (context.dryRun) {
            return ApiResponse.success("diagnose_job", data);
        }
        Map<String, Object> responses = collectResponses(endpoints, hostHeader);
        data.put("responses", responses);
        if (context.hasFlag("report")) {
            data.putAll(healthReportBuilder.fromEvidence(responses, "Flink job diagnosis completed."));
        }
        return ApiResponse.success("diagnose_job", data);
    }

    private String diagnoseBackpressure(CommandContext context) throws Exception {
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String jobId = context.require("job-id");
        String vertexId = context.option("vertex-id");
        String hostHeader = httpHostHeader(context);
        List<String> endpoints = backpressureDiscoveryEndpoints(jobManagerUrl, jobId, vertexId);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("jobManagerUrl", jobManagerUrl);
        data.put("jobId", jobId);
        if (vertexId != null && vertexId.trim().length() > 0) {
            data.put("vertexId", vertexId.trim());
        }
        putHostHeader(data, hostHeader);
        data.put("endpoints", endpoints);
        data.put("analysisLimit", "Backpressure V2 uses Flink 1.19 REST job detail, per-vertex backpressure, subtask metrics, exceptions, and checkpoints. It remains conservative when vertices or metrics are unavailable.");
        if (context.dryRun) {
            return ApiResponse.success("diagnose_backpressure", data);
        }
        Map<String, Object> responses = new LinkedHashMap<String, Object>();
        String jobDetailEndpoint = jobManagerUrl + "/jobs/" + jobId;
        String jobDetail = restClient.get(jobDetailEndpoint, hostHeader);
        responses.put(jobDetailEndpoint, jobDetail);

        List<String> vertexIds = new ArrayList<String>();
        if (vertexId != null && vertexId.trim().length() > 0) {
            vertexIds.add(vertexId.trim());
        } else {
            vertexIds.addAll(extractVertexIds(jobDetail));
        }
        data.put("discoveredVertexIds", vertexIds);
        for (String discoveredVertexId : vertexIds) {
            String backpressureEndpoint = jobManagerUrl + "/jobs/" + jobId + "/vertices/" + discoveredVertexId + "/backpressure";
            String metricsEndpoint = vertexMetricsEndpoint(jobManagerUrl, jobId, discoveredVertexId);
            responses.put(backpressureEndpoint, restClient.get(backpressureEndpoint, hostHeader));
            responses.put(metricsEndpoint, restClient.get(metricsEndpoint, hostHeader));
        }
        String exceptionsEndpoint = jobManagerUrl + "/jobs/" + jobId + "/exceptions";
        String checkpointsEndpoint = jobManagerUrl + "/jobs/" + jobId + "/checkpoints";
        responses.put(exceptionsEndpoint, restClient.get(exceptionsEndpoint, hostHeader));
        responses.put(checkpointsEndpoint, restClient.get(checkpointsEndpoint, hostHeader));
        data.put("evidence", responses);
        data.putAll(healthReportBuilder.backpressureReport(responses));
        return ApiResponse.success("diagnose_backpressure", data);
    }

    private String getExceptions(CommandContext context) throws Exception {
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String jobId = context.require("job-id");
        String hostHeader = httpHostHeader(context);
        RestDialect dialect = restDialect(context);
        String endpoint = endpoint(jobManagerUrl, dialect.jobExceptions(jobId));
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("jobManagerUrl", jobManagerUrl);
        data.put("jobId", jobId);
        data.put("restDialect", dialect.getClass().getSimpleName());
        data.put("endpoint", endpoint);
        putHostHeader(data, hostHeader);
        if (!context.dryRun) {
            data.put("responseJson", restClient.get(endpoint, hostHeader));
        }
        return ApiResponse.success("get_exceptions", data);
    }

    private String getCheckpoints(CommandContext context) throws Exception {
        String jobManagerUrl = normalizedJobManagerUrl(context);
        String jobId = context.require("job-id");
        String hostHeader = httpHostHeader(context);
        RestDialect dialect = restDialect(context);
        String endpoint = endpoint(jobManagerUrl, dialect.jobCheckpoints(jobId));
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("jobManagerUrl", jobManagerUrl);
        data.put("jobId", jobId);
        data.put("restDialect", dialect.getClass().getSimpleName());
        data.put("endpoint", endpoint);
        putHostHeader(data, hostHeader);
        if (!context.dryRun) {
            data.put("responseJson", restClient.get(endpoint, hostHeader));
        }
        return ApiResponse.success("get_checkpoints", data);
    }

    private String stopEndpoint(String jobManagerUrl, String jobId, String stopMode) {
        if ("cancel".equals(stopMode)) {
            return jobManagerUrl + "/jobs/" + jobId + "?mode=cancel";
        }
        if ("savepoint".equals(stopMode) || "drain".equals(stopMode)) {
            return jobManagerUrl + "/jobs/" + jobId + "/stop";
        }
        throw new ValidationException("Parameter '--stop-mode' must be cancel, savepoint, or drain.");
    }

    private String stopRequestBody(String stopMode, CommandContext context) {
        if ("savepoint".equals(stopMode)) {
            String savepointDir = context.require("savepoint-dir");
            boolean drain = context.booleanOption("drain", false);
            return "{\"targetDirectory\":\"" + escape(savepointDir) + "\",\"drain\":" + drain + "}";
        }
        if ("drain".equals(stopMode)) {
            String savepointDir = context.require("savepoint-dir");
            return "{\"targetDirectory\":\"" + escape(savepointDir) + "\",\"drain\":true}";
        }
        if ("cancel".equals(stopMode)) {
            return null;
        }
        throw new ValidationException("Parameter '--stop-mode' must be cancel, savepoint, or drain.");
    }

    private String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    protected void requireImplementedDeploymentTarget(CommandContext context, String expected) {
        String target = context.option("deployment-target");
        if (target == null || target.trim().isEmpty()) {
            if (PROVIDER_KUBERNETES.equals(expected) && isKubernetesCommandAlias(context.command)) {
                return;
            }
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("required", "--deployment-target " + expected);
            data.put("provider", expected);
            data.put("commonRestCommands", Arrays.asList(
                    "get_job_status",
                    "inspect_cluster",
                    "diagnose_job",
                    "diagnose_backpressure",
                    "get_exceptions",
                    "get_checkpoints",
                    "stop_job"));
            throw new OperationException(
                    "DeploymentTargetRequired",
                    "This command is provider-specific. Add --deployment-target " + expected + ". Common Flink REST commands do not require --deployment-target.",
                    data);
        }
        if (expected.equals(target)) {
            return;
        }
        if (PROVIDER_YARN.equals(target)) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("provider", PROVIDER_YARN);
            data.put("status", "implemented");
            data.put("unsupportedCommand", context.command);
            data.put("implementedProviders", Arrays.asList(PROVIDER_KUBERNETES));
            throw new OperationException(
                    "ProviderNotImplemented",
                    "YARN does not implement this provider-specific Kubernetes command.",
                    data);
        }
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("provider", target);
        data.put("supportedProviders", Arrays.asList(PROVIDER_KUBERNETES, PROVIDER_YARN));
        throw new OperationException(
                "UnsupportedProvider",
                "Unsupported deployment provider '" + target + "'.",
                data);
    }

    private boolean isKubernetesCommandAlias(String command) {
        return CommandNames.isKubernetesAlias(command);
    }

    private String normalizedJobManagerUrl(CommandContext context) {
        String value = context.require("job-manager-url");
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    protected String httpHostHeader(CommandContext context) {
        String value = context.option("http-host-header");
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    private void putHostHeader(Map<String, Object> data, String hostHeader) {
        if (hostHeader != null && !hostHeader.trim().isEmpty()) {
            data.put("httpHostHeader", hostHeader);
        }
    }

    private List<String> diagnosticEndpoints(String jobManagerUrl, String jobId, RestDialect dialect) {
        if (jobId == null || jobId.trim().isEmpty()) {
            return Arrays.asList(jobManagerUrl + "/overview", jobManagerUrl + "/jobs");
        }
        return Arrays.asList(
                jobManagerUrl + "/overview",
                endpoint(jobManagerUrl, dialect.jobStatus(jobId)),
                jobManagerUrl + "/jobs/" + jobId + "/status",
                jobManagerUrl + "/jobs/" + jobId + "/config",
                jobManagerUrl + "/jobs/" + jobId + "/plan",
                jobManagerUrl + "/jobs/" + jobId + "/metrics?get=numRestarts,fullRestarts",
                endpoint(jobManagerUrl, dialect.jobExceptions(jobId)),
                endpoint(jobManagerUrl, dialect.jobCheckpoints(jobId)));
    }

    private RestDialect restDialect(CommandContext context) {
        return FlinkCompatibilityRegistry.resolve(context.option("flink-version")).restDialect();
    }

    private String endpoint(String jobManagerUrl, EndpointSpec endpoint) {
        return jobManagerUrl + endpoint.path();
    }

    private List<String> inspectionEndpoints(String jobManagerUrl, String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            return Arrays.asList(
                    jobManagerUrl + "/overview",
                    jobManagerUrl + "/config",
                    jobManagerUrl + "/taskmanagers",
                    jobManagerUrl + "/jobs");
        }
        List<String> endpoints = new ArrayList<String>();
        endpoints.add(jobManagerUrl + "/overview");
        endpoints.add(jobManagerUrl + "/config");
        endpoints.add(jobManagerUrl + "/taskmanagers");
        endpoints.add(jobManagerUrl + "/jobs");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId);
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/status");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/config");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/plan");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/metrics?get=numRestarts,fullRestarts");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/exceptions");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/checkpoints");
        return endpoints;
    }

    private Map<String, Object> collectResponses(List<String> endpoints, String hostHeader) throws Exception {
        Map<String, Object> responses = new LinkedHashMap<String, Object>();
        for (String endpoint : endpoints) {
            responses.put(endpoint, restClient.get(endpoint, hostHeader));
        }
        return responses;
    }

    private List<String> backpressureDiscoveryEndpoints(String jobManagerUrl, String jobId, String vertexId) {
        List<String> endpoints = new ArrayList<String>();
        endpoints.add(jobManagerUrl + "/jobs/" + jobId);
        if (vertexId == null || vertexId.trim().isEmpty()) {
            endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/vertices/<vertex-id>/backpressure");
            endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/vertices/<vertex-id>/subtasks/metrics?get=backPressuredTimeMsPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond&agg=min,max,avg");
        } else {
            endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/vertices/" + vertexId.trim() + "/backpressure");
            endpoints.add(vertexMetricsEndpoint(jobManagerUrl, jobId, vertexId.trim()));
        }
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/exceptions");
        endpoints.add(jobManagerUrl + "/jobs/" + jobId + "/checkpoints");
        return endpoints;
    }

    private String vertexMetricsEndpoint(String jobManagerUrl, String jobId, String vertexId) {
        return jobManagerUrl + "/jobs/" + jobId + "/vertices/" + vertexId
                + "/subtasks/metrics?get=backPressuredTimeMsPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond&agg=min,max,avg";
    }

    private List<String> extractVertexIds(String jobDetailJson) {
        List<String> vertexIds = new ArrayList<String>();
        if (jobDetailJson == null) {
            return vertexIds;
        }
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");
        java.util.regex.Matcher matcher = pattern.matcher(jobDetailJson);
        while (matcher.find()) {
            String id = matcher.group(1);
            if (!vertexIds.contains(id) && id.length() > 0) {
                vertexIds.add(id);
            }
        }
        return vertexIds;
    }

    protected Map<String, Object> verifyStarted(String jobManagerUrl, String jobId, String hostHeader) throws Exception {
        Map<String, Object> verification = new LinkedHashMap<String, Object>();
        verification.put("jobManagerUrl", jobManagerUrl);
        List<String> endpoints = jobId == null || jobId.trim().isEmpty()
                ? Arrays.asList(jobManagerUrl + "/overview", jobManagerUrl + "/jobs")
                : Arrays.asList(jobManagerUrl + "/overview", jobManagerUrl + "/jobs/" + jobId);
        Map<String, Object> evidence = collectResponses(endpoints, hostHeader);
        verification.put("evidence", evidence);
        String joined = Json.stringify(evidence).toLowerCase();
        boolean verified = !joined.contains("failed") && !joined.contains("exception") && (joined.contains("running") || joined.contains("taskmanagers"));
        verification.put("verified", Boolean.valueOf(verified));
        verification.put("expected", jobId == null || jobId.trim().isEmpty() ? "JobManager REST reachable and jobs endpoint readable." : "Target job is visible and healthy.");
        return verification;
    }

    private Map<String, Object> verifyStopped(String jobManagerUrl, String jobId, String hostHeader) throws Exception {
        Map<String, Object> verification = new LinkedHashMap<String, Object>();
        verification.put("jobManagerUrl", jobManagerUrl);
        verification.put("jobId", jobId);
        String endpoint = jobManagerUrl + "/jobs/" + jobId;
        String response = restClient.get(endpoint, hostHeader);
        verification.put("endpoint", endpoint);
        verification.put("responseJson", response);
        String lower = response.toLowerCase();
        boolean verified = !lower.contains("\"state\":\"running\"") && !lower.contains("\"status\":\"running\"");
        verification.put("verified", Boolean.valueOf(verified));
        verification.put("expected", "Target job is no longer RUNNING.");
        return verification;
    }

    protected Map<String, Object> targetLock(String operation, String namespace, String name, String jobManagerUrl, String jobId, String hostHeader) {
        Map<String, Object> lock = new LinkedHashMap<String, Object>();
        lock.put("operation", operation);
        if (namespace != null) {
            lock.put("namespace", namespace);
        }
        if (name != null) {
            lock.put("name", name);
        }
        if (jobManagerUrl != null) {
            lock.put("jobManagerUrl", jobManagerUrl);
        }
        if (jobId != null) {
            lock.put("jobId", jobId);
        }
        if (hostHeader != null) {
            lock.put("httpHostHeader", hostHeader);
        }
        lock.put("targetFingerprint", fingerprint(lock));
        return lock;
    }

    protected void enforceExpectedTargetLock(CommandContext context, Map<String, Object> targetLock) {
        String expected = context.option("expected-target-lock");
        if (expected == null || expected.trim().isEmpty()) {
            return;
        }
        String actual = String.valueOf(targetLock.get("targetFingerprint"));
        if (!expected.trim().equals(actual)) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("expectedTargetLock", expected.trim());
            data.put("actualTargetLock", actual);
            data.put("targetLock", targetLock);
            throw new OperationException("TargetLockMismatch", "The requested mutation target does not match the expected target lock.", data);
        }
    }

    private String fingerprint(Map<String, Object> lock) {
        Map<String, Object> copy = new LinkedHashMap<String, Object>(lock);
        copy.remove("targetFingerprint");
        return Integer.toHexString(Json.stringify(copy).hashCode());
    }

    protected String normalizeUrl(String value) {
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static String operationName(CommandContext context) {
        if (context == null || context.command == null || context.command.trim().isEmpty()) {
            return "unknown";
        }
        return context.command;
    }

    private static String errorMessage(Throwable throwable) {
        if (throwable == null) {
            return "Unknown error.";
        }
        String message = throwable.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return throwable.toString();
        }
        return message;
    }
}
