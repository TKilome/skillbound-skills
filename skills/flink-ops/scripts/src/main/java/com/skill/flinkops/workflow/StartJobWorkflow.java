package com.skill.flinkops.workflow;

import com.skill.flinkops.FlinkVersionAdapter;
import com.skill.flinkops.FlinkVersionAdapters;
import com.skill.flinkops.common.ApiResponse;
import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.SafetyGate;
import com.skill.flinkops.common.errors.OperationException;
import com.skill.flinkops.flink.rest.FlinkRuntimeInspector;
import com.skill.flinkops.image.FlinkImageBuildSpec;
import com.skill.flinkops.image.FlinkImageBuilder;
import com.skill.flinkops.provider.kubernetes.FlinkKubernetesApplicationSpec;
import com.skill.flinkops.provider.kubernetes.FlinkNativeKubernetesDeployer;
import com.skill.flinkops.provider.kubernetes.KubernetesEnvironmentChecker;
import com.skill.flinkops.provider.kubernetes.KubernetesGateway;
import com.skill.flinkops.provider.kubernetes.KubernetesIngressSpec;
import com.skill.flinkops.provider.yarn.FlinkYarnApplicationDeployer;
import com.skill.flinkops.provider.yarn.FlinkYarnApplicationSpec;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class StartJobWorkflow {
    public interface Support {
        Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) throws Exception;
        Map<String, Object> deployYarnApplication(FlinkYarnApplicationSpec spec) throws Exception;
        Map<String, Object> verifyStarted(String jobManagerUrl, String jobId, String hostHeader) throws Exception;
        Map<String, Object> targetLock(String operation, String namespace, String name, String jobManagerUrl, String jobId, String hostHeader);
        void enforceExpectedTargetLock(CommandContext context, Map<String, Object> targetLock);
        void requireImplementedDeploymentTarget(CommandContext context, String expected);
        String normalizeUrl(String value);
        String httpHostHeader(CommandContext context);
    }

    private static final String PROVIDER_KUBERNETES = "kubernetes";
    private static final String PROVIDER_YARN = "yarn";

    @SuppressWarnings("unused")
    private final FlinkNativeKubernetesDeployer kubernetesDeployer;
    @SuppressWarnings("unused")
    private final FlinkYarnApplicationDeployer yarnApplicationDeployer;
    private final FlinkImageBuilder imageBuilder;
    private final FlinkRuntimeInspector runtimeInspector;
    private final KubernetesGateway kubernetesGateway;
    private final KubernetesEnvironmentChecker kubernetesChecker;
    private final Support support;

    public StartJobWorkflow(
            FlinkNativeKubernetesDeployer kubernetesDeployer,
            FlinkYarnApplicationDeployer yarnApplicationDeployer,
            FlinkImageBuilder imageBuilder,
            FlinkRuntimeInspector runtimeInspector,
            KubernetesGateway kubernetesGateway,
            KubernetesEnvironmentChecker kubernetesChecker,
            Support support) {
        this.kubernetesDeployer = kubernetesDeployer;
        this.yarnApplicationDeployer = yarnApplicationDeployer;
        this.imageBuilder = imageBuilder;
        this.runtimeInspector = runtimeInspector;
        this.kubernetesGateway = kubernetesGateway;
        this.kubernetesChecker = kubernetesChecker;
        this.support = support;
    }

    public String startJob(CommandContext context) throws Exception {
        SafetyGate.requireConfirm(context, "start_job");
        String target = context.option("deployment-target");
        if (PROVIDER_YARN.equals(target)) {
            return startYarnApplicationJob(context);
        }
        support.requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        FlinkKubernetesApplicationSpec spec = FlinkKubernetesApplicationSpec.from(context);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from(spec.namespace, spec.name);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("name", spec.name);
        data.put("namespace", spec.namespace);
        data.put("serviceAccount", spec.serviceAccount);
        Map<String, Object> targetLock = support.targetLock(context.command, spec.namespace, spec.name, null, null, null);
        support.enforceExpectedTargetLock(context, targetLock);
        data.put("targetLock", targetLock);
        data.put("mode", "flink-kubernetes-application");
        data.putAll(spec.dryRunPlan());
        data.put("plan", spec.dryRunPlan());
        data.put("nextActions", startNextActions(context.dryRun, spec.enableIngress));
        data.put("ingressEnabled", Boolean.valueOf(spec.enableIngress));
        if (spec.enableIngress) {
            data.put("ingress", ingress.plan());
        } else {
            data.put("ingress", disabledIngressPlan(spec));
        }
        if (context.dryRun) {
            Map<String, Object> preflight = new LinkedHashMap<String, Object>();
            preflight.put("deployTargetConnectivity", kubernetesChecker.plannedEnvironment(
                    spec.namespace, spec.serviceAccount, spec.kubeconfigPath, spec.enableIngress, ingress));
            preflight.put("cliExecutionEnvironment", cliExecutionEnvironment(context));
            data.put("preflight", preflight);
            return ApiResponse.success(context.command, data);
        }
        Map<String, Object> deployTargetConnectivity = kubernetesChecker.checkEnvironment(
                spec.namespace, spec.serviceAccount, spec.kubeconfigPath, spec.enableIngress, ingress);
        Map<String, Object> preflight = new LinkedHashMap<String, Object>();
        preflight.put("deployTargetConnectivity", deployTargetConnectivity);
        preflight.put("cliExecutionEnvironment", cliExecutionEnvironment(context));
        data.put("preflight", preflight);
        Map<String, Object> submission = support.deployKubernetes(spec);
        data.put("submission", submission);
        if (spec.enableIngress) {
            try {
                data.put("ingress", kubernetesGateway.upsertIngress(ingress));
            } catch (OperationException e) {
                if ("IngressCreationFailed".equals(e.code())) {
                    Map<String, Object> failureData = new LinkedHashMap<String, Object>();
                    failureData.put("submission", submission);
                    failureData.put("ingress", ingress.plan());
                    throw new OperationException(e.code(), e.getMessage(), failureData);
                }
                throw e;
            }
            data.put("ingressHost", ingress.host);
            data.put("ingressUrl", ingress.url);
            Map<String, Object> ingressAccess = ingressAccess(ingress, deployTargetConnectivity);
            if (!ingressAccess.isEmpty()) {
                data.put("ingressAccess", ingressAccess);
            }
        } else {
            data.put("jobManagerInternalUrl", "http://" + spec.name + "-rest." + spec.namespace + ".svc.cluster.local:8081");
        }
        if (context.hasFlag("verify")) {
            String verificationUrl = context.option("job-manager-url");
            if (verificationUrl == null || verificationUrl.trim().isEmpty()) {
                Object submittedUrl = submission.get("jobManagerUrl");
                verificationUrl = submittedUrl == null ? null : String.valueOf(submittedUrl);
            }
            if (verificationUrl == null || verificationUrl.trim().isEmpty()) {
                verificationUrl = "http://" + spec.name + "-rest." + spec.namespace + ".svc.cluster.local:8081";
            }
            data.put("readBackVerification", support.verifyStarted(support.normalizeUrl(verificationUrl), context.option("job-id"), support.httpHostHeader(context)));
        }
        return ApiResponse.success(context.command, data);
    }

    private String startYarnApplicationJob(CommandContext context) throws Exception {
        FlinkYarnApplicationSpec spec = FlinkYarnApplicationSpec.from(context);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("name", spec.name);
        data.put("provider", PROVIDER_YARN);
        Map<String, Object> targetLock = support.targetLock(context.command, null, spec.name, null, null, null);
        support.enforceExpectedTargetLock(context, targetLock);
        data.put("targetLock", targetLock);
        data.putAll(spec.dryRunPlan());
        data.put("plan", spec.dryRunPlan());
        data.put("nextActions", startNextActions(context.dryRun, false));
        if (context.dryRun) {
            data.put("preflight", runtimeInspector.inspect(context));
            return ApiResponse.success(context.command, data);
        }
        Map<String, Object> submission = support.deployYarnApplication(spec);
        data.put("submission", submission);
        if (context.hasFlag("verify")) {
            String verificationUrl = context.option("job-manager-url");
            if (verificationUrl == null || verificationUrl.trim().isEmpty()) {
                Object submittedUrl = submission.get("jobManagerUrl");
                verificationUrl = submittedUrl == null ? null : String.valueOf(submittedUrl);
            }
            if (verificationUrl != null && !verificationUrl.trim().isEmpty()) {
                data.put("readBackVerification", support.verifyStarted(support.normalizeUrl(verificationUrl), context.option("job-id"), support.httpHostHeader(context)));
            }
        }
        return ApiResponse.success(context.command, data);
    }

    private Map<String, Object> disabledIngressPlan(FlinkKubernetesApplicationSpec spec) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("enabled", Boolean.FALSE);
        data.put("reason", "Ingress disabled by --enable-ingress=false.");
        data.put("jobManagerInternalUrl", "http://" + spec.name + "-rest." + spec.namespace + ".svc.cluster.local:8081");
        return data;
    }

    private Map<String, Object> ingressAccess(KubernetesIngressSpec ingress, Map<String, Object> preflight) {
        Map<String, Object> access = new LinkedHashMap<String, Object>();
        access.put("host", ingress.host);
        access.put("ingressUrl", ingress.url);
        Map<String, Object> service = ingressControllerService(preflight);
        if (service == null || service.isEmpty()) {
            return access;
        }
        access.put("ingressControllerService", service);
        Object httpNodePort = nodePort(service, "http");
        Object httpsNodePort = nodePort(service, "https");
        if (httpNodePort != null) {
            access.put("httpNodePort", httpNodePort);
            access.put("httpUrl", "http://" + ingress.host + ":" + httpNodePort);
        }
        if (httpsNodePort != null) {
            access.put("httpsNodePort", httpsNodePort);
            access.put("httpsUrl", "https://" + ingress.host + ":" + httpsNodePort);
        }
        return access;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> ingressControllerService(Map<String, Object> preflight) {
        Object ingressValue = preflight.get("ingress");
        if (!(ingressValue instanceof Map)) {
            return null;
        }
        Object checksValue = ((Map<String, Object>) ingressValue).get("checks");
        if (!(checksValue instanceof Map)) {
            return null;
        }
        Object serviceCheckValue = ((Map<String, Object>) checksValue).get("ingressControllerService");
        if (!(serviceCheckValue instanceof Map)) {
            return null;
        }
        Object serviceValue = ((Map<String, Object>) serviceCheckValue).get("value");
        if (!(serviceValue instanceof Map)) {
            return null;
        }
        return (Map<String, Object>) serviceValue;
    }

    @SuppressWarnings("unchecked")
    private Object nodePort(Map<String, Object> service, String name) {
        Object portsValue = service.get("ports");
        if (!(portsValue instanceof Iterable)) {
            return null;
        }
        for (Object portValue : (Iterable<Object>) portsValue) {
            if (!(portValue instanceof Map)) {
                continue;
            }
            Map<String, Object> port = (Map<String, Object>) portValue;
            if (name.equals(String.valueOf(port.get("name")))) {
                return port.get("nodePort");
            }
        }
        return null;
    }

    public String buildImage(CommandContext context) throws Exception {
        SafetyGate.requireConfirm(context, "build_image");
        support.requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        FlinkImageBuildSpec spec = FlinkImageBuildSpec.from(context);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.putAll(spec.plan());
        if (context.dryRun) {
            return ApiResponse.success(context.command, data);
        }
        data.put("build", imageBuilder.build(spec));
        return ApiResponse.success(context.command, data);
    }

    public String preflightStart(CommandContext context) throws Exception {
        String target = context.option("deployment-target");
        if (PROVIDER_YARN.equals(target)) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("deploymentTarget", PROVIDER_YARN);
            FlinkVersionAdapter adapter = FlinkVersionAdapters.resolve(context);
            data.put("flinkVersion", adapter.flinkVersion);
            data.put("flinkMajorVersion", adapter.majorVersion);
            data.put("runtime", runtimeInspector.inspect(context));
            data.put("yarn", yarnPreflightPlan(context));
            return ApiResponse.success(context.command, data);
        }
        support.requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        String namespace = context.require("namespace");
        String serviceAccount = context.require("service-account");
        context.require("flink-home");
        boolean enableIngress = context.booleanOption("enable-ingress", true);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from(namespace, "preflight-start-check");

        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("deploymentTarget", "kubernetes");
        data.put("namespace", namespace);
        data.put("serviceAccount", serviceAccount);
        data.put("enableIngress", Boolean.valueOf(enableIngress));

        Map<String, Object> preflight = new LinkedHashMap<String, Object>();
        preflight.put("deployTargetConnectivity", context.dryRun
                ? kubernetesChecker.plannedEnvironment(namespace, serviceAccount, context.option("kubeconfig-path"), enableIngress, ingress)
                : kubernetesChecker.checkEnvironment(namespace, serviceAccount, context.option("kubeconfig-path"), enableIngress, ingress));
        preflight.put("cliExecutionEnvironment", cliExecutionEnvironment(context));
        data.put("preflight", preflight);
        return ApiResponse.success(context.command, data);
    }

    public String checkCliEnvironment(CommandContext context) throws Exception {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("cliExecutionEnvironment", cliExecutionEnvironment(context));
        return ApiResponse.success(context.command, data);
    }

    private Map<String, Object> cliExecutionEnvironment(CommandContext context) throws Exception {
        String flinkHome = context.option("flink-home");
        if (flinkHome == null) {
            flinkHome = "";
        }
        Map<String, Object> environment = new LinkedHashMap<String, Object>();
        environment.put("cli", okValue("FlinkOpsCli"));
        environment.put("java", okValue(System.getProperty("java.version")));
        environment.put("flinkHome", okValue(flinkHome));
        environment.put("runtimeClasspath", runtimeInspector.inspect(context));
        return environment;
    }

    private Map<String, Object> okValue(Object value) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("ok", Boolean.TRUE);
        data.put("value", value);
        return data;
    }

    public String k8sCheckConnectivity(CommandContext context) throws Exception {
        support.requireImplementedDeploymentTarget(context, PROVIDER_KUBERNETES);
        String namespace = context.require("namespace");
        String serviceAccount = context.require("service-account");
        String kubeconfigPath = context.option("kubeconfig-path");
        boolean enableIngress = context.booleanOption("enable-ingress", true);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from(namespace, "environment-check");

        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("deploymentTarget", PROVIDER_KUBERNETES);
        Map<String, Object> connectivity = context.dryRun
                ? kubernetesChecker.plannedEnvironment(namespace, serviceAccount, kubeconfigPath, enableIngress, ingress)
                : kubernetesChecker.checkEnvironment(namespace, serviceAccount, kubeconfigPath, enableIngress, ingress);
        data.put("deployTargetConnectivity", connectivity);
        return ApiResponse.success(context.command, data);
    }

    public String yarnCheckConnectivity(CommandContext context) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("deploymentTarget", PROVIDER_YARN);
        data.put("deployTargetConnectivity", yarnConnectivity(context));
        return ApiResponse.success(context.command, data);
    }

    private Map<String, Object> yarnConnectivity(CommandContext context) {
        Map<String, Object> connectivity = new LinkedHashMap<String, Object>();
        connectivity.put("hadoopConfDir", pendingValue(firstPresent(context.option("hadoop-conf-dir"), "HADOOP_CONF_DIR or YARN_CONF_DIR")));
        connectivity.put("resourceManager", advisoryValue("ResourceManager reachability is checked through Hadoop/YARN Java APIs in a real connectivity implementation."));
        connectivity.put("hdfs", advisoryValue("HDFS and staging directory reachability are provider connectivity concerns."));
        connectivity.put("queue", pendingValue(firstPresent(context.option("yarn-queue"), "default")));
        connectivity.put("providedLibDirs", pendingValue(firstPresent(context.option("yarn-provided-lib-dirs"), "")));
        connectivity.put("flinkDistJar", pendingValue(firstPresent(context.option("flink-dist-jar"), "")));
        connectivity.put("security", advisoryValue("Kerberos or proxy-user validation must use local Hadoop credentials and must not serialize credential material."));
        return connectivity;
    }

    private Map<String, Object> pendingValue(String value) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("ok", Boolean.FALSE);
        data.put("planned", Boolean.TRUE);
        data.put("value", value == null ? "" : value);
        return data;
    }

    private Map<String, Object> advisoryValue(String message) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("ok", Boolean.TRUE);
        data.put("advisory", Boolean.TRUE);
        data.put("message", message);
        return data;
    }

    private String firstPresent(String value, String fallback) {
        if (value != null && !value.trim().isEmpty()) {
            return value;
        }
        return fallback;
    }

    private Map<String, Object> yarnPreflightPlan(CommandContext context) {
        Map<String, Object> yarn = new LinkedHashMap<String, Object>();
        yarn.put("checks", Arrays.asList(
                "Flink runtime jars are available on the JVM classpath.",
                "Hadoop/YARN runtime classes are available on the JVM classpath.",
                "Hadoop configuration is available through HADOOP_CONF_DIR, YARN_CONF_DIR, or classpath.",
                "External yarn and flink CLIs are not used."));
        yarn.put("flinkHome", context.require("flink-home"));
        return yarn;
    }

    private List<String> startNextActions(boolean dryRun, boolean ingressEnabled) {
        if (dryRun) {
            return Arrays.asList(
                    "Review targetLock.targetFingerprint and staged plan.",
                    "Run the same command without --dry-run and include --expected-target-lock when ready.");
        }
        if (ingressEnabled) {
            return Arrays.asList(
                    "Open the returned ingressUrl or NodePort URL with the listed host header when needed.",
                    "Run inspect_cluster with the returned JobManager URL after the job reaches RUNNING.");
        }
        return Arrays.asList(
                "Use the returned internal JobManager service URL from inside the cluster network.",
                "Run inspect_cluster when a reachable JobManager REST URL is available.");
    }
}
