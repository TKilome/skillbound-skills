package com.skill.flinkops;

import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.Json;
import com.skill.flinkops.common.errors.OperationException;
import com.skill.flinkops.cli.CommandNames;
import com.skill.flinkops.cli.CommandRouter;
import com.skill.flinkops.flink.compat.FlinkCompatibility;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;
import com.skill.flinkops.flink.compat.FlinkVersion;
import com.skill.flinkops.flink.compat.rest.DefaultRestDialect;
import com.skill.flinkops.flink.compat.rest.RestDialect;
import com.skill.flinkops.flink.compat.submit.DefaultSubmitDialect;
import com.skill.flinkops.flink.compat.submit.FlinkConfigApplier;
import com.skill.flinkops.flink.compat.submit.OptionRef;
import com.skill.flinkops.flink.compat.submit.SubmitSpec;
import com.skill.flinkops.flink.compat.submit.overrides.Flink114SubmitDialect;
import com.skill.flinkops.flink.rest.FlinkRestClient;
import com.skill.flinkops.image.FlinkImageBuildSpec;
import com.skill.flinkops.provider.kubernetes.FlinkKubernetesApplicationSpec;
import com.skill.flinkops.provider.kubernetes.FlinkNativeKubernetesDeployer;
import com.skill.flinkops.provider.kubernetes.KubernetesEnvironmentChecker;
import com.skill.flinkops.provider.kubernetes.KubernetesGateway;
import com.skill.flinkops.provider.kubernetes.KubernetesIngressSpec;
import com.skill.flinkops.workflow.report.HealthReportBuilder;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FlinkOpsCliTest {
    public static void main(String[] args) throws Exception {
        testMissingCommandReturnsValidationError();
        testDoctorCommandIsUnsupported();
        testCommandRouterSupportsExistingCommandsAndAliases();
        testCommandNamesRecognizesKubernetesAliases();
        testStartJobYarnDryRunReturnsApplicationPlanWithCommonParameters();
        testPreflightStartYarnDryRunChecksRuntime();
        testFlinkVersionNormalizesMajorMinor();
        testCompatibilityRegistryUsesDefaultForModernVersion();
        testCompatibilityRegistryUsesOverrideForFlink114();
        testLegacyVersionAdapterExposesCompatibility();
        testDefaultSubmitDialectAppliesCommonOptions();
        testFlink114SubmitDialectDisablesStopWithDrain();
        testDefaultRestDialectReturnsCommonEndpoints();
        testDetectsFlinkConfigPrefixConflictsBeforeDeployment();
        testStartJobUnknownProviderReturnsUnsupportedProvider();
        testKubernetesReadCommandsRequireDeploymentTarget();
        testKubernetesReadCommandsRejectYarnProvider();
        testK8sAliasesMakeProviderExplicit();
        testOldEnvironmentCommandNamesAreRejected();
        testK8sCheckConnectivityReportsDeployTargetConnectivity();
        testYarnCheckConnectivityDryRunReportsDeployTargetConnectivity();
        testCheckCliEnvironmentDryRunReturnsCliExecutionEnvironment();
        testCommonRestStatusDoesNotRequireProvider();
        testStartJobRequiresConfirm();
        testKubernetesStartRequiresParallelismBeforeSubmission();
        testKubernetesStartRejectsNonPositiveParallelismBeforeSubmission();
        testKubernetesStartRejectsJarUriWithoutSchemeBeforeSubmission();
        testOperationExceptionReturnsSpecificCodeAndData();
        testOperationExceptionIncludesCauseChain();
        testValidationExceptionIncludesCauseChain();
        testInvocationTargetExceptionIncludesNestedCauseChain();
        testBuildImageRequiresConfirm();
        testBuildImageDryRunReturnsDockerfilePlan();
        testBuildImageDryRunDefaultsTargetJarPath();
        testPreflightStartDryRunChecksRuntimeAndIngress();
        testPreflightStartWithIngressDisabledSkipsIngressChecks();
        testStartJobDryRunReturnsFlinkNativeClientPlan();
        testStartJobDryRunPropagatesKubeconfigToFlinkDeploymentConfig();
        testStartJobDryRunIncludesPreflightAndMandatoryIngressPlan();
        testStartJobDryRunIncludesStagedPlanAndNextActions();
        testStartJobDryRunWithIngressDisabledSkipsIngressChecks();
        testStartJobReturnsIngressAccessNodePorts();
        testStartJobWithIngressDisabledSkipsIngressCreation();
        testStartJobWithoutFlinkRuntimeClasspathReturnsSpecificError();
        testCheckIngressControllerDryRunReturnsPlannedChecks();
        testCheckIngressControllerMissingControllerReturnsRemediation();
        testCheckIngressControllerReturnsNodePortService();
        testGetNodeIpsDryRunReturnsPlannedRead();
        testGetNodeIpsReturnsNodeAddresses();
        testRenderIngressControllerYamlOutputsTemplate();
        testKubernetesIngressSpecDerivesHostAndManifest();
        testPreflightMissingNamespaceFailsBeforeStart();
        testPreflightMissingServiceAccountFailsBeforeStart();
        testPreflightMissingIngressControllerReturnsRemediation();
        testPreflightMissingIngressClassFails();
        testStartJobIngressCreationFailureReturnsSpecificError();
        testGetStatusDryRunReturnsFlinkRestEndpoints();
        testGetStatusDryRunIncludesHttpHostHeader();
        testGetStatusSendsHttpHostHeader();
        testInspectClusterDryRunReturnsInspectionPlan();
        testInspectClusterReportCollectsEvidence();
        testDiagnoseDryRunIncludesFlink19EvidenceEndpoints();
        testDiagnoseReportReturnsFindingsAndRecommendations();
        testHealthReportBuilderProducesDeterministicRiskFields();
        testDiagnoseReportFlagsTaskmanagerAndRestartRisks();
        testDiagnoseReportDoesNotTreatZeroFailedCheckpointsAsFailure();
        testDiagnoseBackpressureDryRunReturnsEndpoints();
        testDiagnoseBackpressureReportCollectsEvidence();
        testDiagnoseBackpressureDiscoversVerticesAndDetectsHighPressure();
        testStopVerifyReadsBackTargetStatus();
        testStopIncludesTargetLockAndRejectsMismatch();
        testStartVerifyReadsBackAfterSubmission();
        testDiagnoseDryRunReturnsCommonFlinkRestEndpoints();
        testGetExceptionsDryRunReturnsFlinkRestEndpoint();
        testGetCheckpointsDryRunReturnsFlinkRestEndpoint();
        testStopWithSavepointDryRunUsesSavepointDirectory();
        System.out.println("FlinkOpsCliTest: all tests passed");
    }

    private static void testMissingCommandReturnsValidationError() {
        CliResult result = run();

        assertEquals(1, result.exitCode, "missing command exit code");
        assertContains(result.stdout, "\"success\":false", "missing command success flag");
        assertContains(result.stdout, "\"code\":\"ValidationError\"", "missing command error code");
    }

    private static void testDoctorCommandIsUnsupported() {
        CliResult result = run("doctor", "--deployment-target", "kubernetes", "--dry-run");

        assertEquals(1, result.exitCode, "doctor unsupported exit code");
        assertContains(result.stdout, "\"code\":\"ValidationError\"", "doctor unsupported code");
        assertContains(result.stdout, "Unsupported command 'doctor'", "doctor unsupported message");
    }

    private static void testCommandRouterSupportsExistingCommandsAndAliases() throws Exception {
        CommandRouter router = new CommandRouter();
        router.register("one", context -> "handled-one");
        router.register("two", context -> "handled-two");

        assertEquals("handled-one", router.dispatch(CommandContext.parse(new String[] {"one"})), "router command one");
        assertEquals("handled-two", router.dispatch(CommandContext.parse(new String[] {"two"})), "router command two");
    }

    private static void testCommandNamesRecognizesKubernetesAliases() {
        assertEquals(true, CommandNames.isKubernetesAlias("k8s_start_job"), "k8s start alias");
        assertEquals(true, CommandNames.isKubernetesAlias("k8s_preflight_start"), "k8s preflight alias");
        assertEquals(true, CommandNames.isKubernetesAlias("k8s_get_node_ips"), "k8s node ips alias");
        assertEquals(false, CommandNames.isKubernetesAlias("get_job_status"), "common rest command is not k8s alias");
    }

    private static void testStartJobYarnDryRunReturnsApplicationPlanWithCommonParameters() {
        CliResult result = run(
                "start_job",
                "--deployment-target", "yarn",
                "--name", "orders",
                "--flink-home", "/opt/flink",
                "--flink-version", "1.19.1",
                "--jar-uri", "hdfs:///apps/orders.jar",
                "--main-class", "com.example.OrdersJob",
                "--args", "--env prod",
                "--parallelism", "4",
                "--jobmanager-memory", "1024m",
                "--taskmanager-memory", "2048m",
                "--taskmanager-slots", "2",
                "--dynamic-properties", "state.backend=rocksdb,execution.checkpointing.interval=30s",
                "--savepoint-path", "hdfs:///savepoints/sp-1",
                "--yarn-queue", "root.analytics",
                "--hadoop-user", "flink",
                "--yarn-provided-lib-dirs", "hdfs:///flink/lib,hdfs:///flink/plugins",
                "--flink-dist-jar", "hdfs:///flink/flink-dist-1.19.1.jar",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "start yarn dry run exit code");
        assertContains(result.stdout, "\"deploymentTarget\":\"yarn-application\"", "yarn application target");
        assertContains(result.stdout, "\"deploymentEngine\":\"flink-java-client\"", "yarn java client engine");
        assertContains(result.stdout, "org.apache.flink.yarn.YarnClusterClientFactory", "yarn factory");
        assertContains(result.stdout, "\"submitDialect\":\"DefaultSubmitDialect\"", "yarn submit dialect");
        assertContains(result.stdout, "\"flinkVersion\":\"1.19.1\"", "yarn flink version");
        assertContains(result.stdout, "\"flinkMajorVersion\":\"1.19\"", "yarn flink major version");
        assertContains(result.stdout, "\"yarn.application.queue\":\"root.analytics\"", "yarn queue");
        assertContains(result.stdout, "\"hadoopUser\":\"flink\"", "hadoop proxy user");
        assertContains(result.stdout, "\"pipeline.jars\":[\"hdfs:///apps/orders.jar\"]", "common jar uri");
        assertContains(result.stdout, "\"application.main.class\":\"com.example.OrdersJob\"", "common main class");
        assertContains(result.stdout, "\"application.args\":[\"--env\",\"prod\"]", "common args");
        assertContains(result.stdout, "\"parallelism.default\":4", "common parallelism");
        assertContains(result.stdout, "\"jobmanager.memory.process.size\":\"1024m\"", "common jm memory");
        assertContains(result.stdout, "\"taskmanager.memory.process.size\":\"2048m\"", "common tm memory");
        assertContains(result.stdout, "\"taskmanager.numberOfTaskSlots\":\"2\"", "common tm slots");
        assertContains(result.stdout, "\"state.backend\":\"rocksdb\"", "dynamic property");
        assertContains(result.stdout, "\"execution.checkpointing.interval\":\"30s\"", "dynamic property checkpoint");
        assertContains(result.stdout, "\"execution.savepoint.path\":\"hdfs:///savepoints/sp-1\"", "common savepoint path");
        assertContains(result.stdout, "\"targetLock\"", "yarn target lock");
    }

    private static void testPreflightStartYarnDryRunChecksRuntime() {
        CliResult result = run(
                "preflight_start",
                "--deployment-target", "yarn",
                "--flink-home", "/opt/flink",
                "--flink-version", "1.19.1",
                "--dry-run");

        assertEquals(0, result.exitCode, "preflight yarn dry run exit code");
        assertContains(result.stdout, "\"deploymentTarget\":\"yarn\"", "preflight yarn target");
        assertContains(result.stdout, "\"flinkVersion\":\"1.19.1\"", "preflight yarn flink version");
        assertContains(result.stdout, "org.apache.flink.yarn.YarnClusterClientFactory", "preflight yarn factory check");
        assertContains(result.stdout, "org.apache.hadoop.security.UserGroupInformation", "preflight hadoop ugi check");
    }

    private static void testFlinkVersionNormalizesMajorMinor() {
        FlinkVersion version = FlinkVersion.parse("1.19.1");

        assertEquals("1.19.1", version.raw(), "raw flink version");
        assertEquals("1.19", version.majorMinor(), "major minor flink version");
    }

    private static void testCompatibilityRegistryUsesDefaultForModernVersion() {
        FlinkCompatibility compatibility = FlinkCompatibilityRegistry.resolve("1.19.1");

        assertEquals("1.19", compatibility.version().majorMinor(), "compatibility version");
        assertEquals("DefaultSubmitDialect", compatibility.submitDialect().getClass().getSimpleName(), "default submit dialect");
        assertEquals("DefaultRestDialect", compatibility.restDialect().getClass().getSimpleName(), "default rest dialect");
    }

    private static void testCompatibilityRegistryUsesOverrideForFlink114() {
        FlinkCompatibility compatibility = FlinkCompatibilityRegistry.resolve("1.14.6");

        assertEquals("1.14", compatibility.version().majorMinor(), "flink 1.14 version");
        assertEquals("Flink114SubmitDialect", compatibility.submitDialect().getClass().getSimpleName(), "flink 1.14 submit dialect");
    }

    private static void testLegacyVersionAdapterExposesCompatibility() {
        FlinkVersionAdapter adapter = new FlinkVersionAdapter("1.14.6", "1.14");

        assertEquals("1.14.6", adapter.flinkVersion, "legacy adapter raw version");
        assertEquals("1.14", adapter.majorVersion, "legacy adapter major version");
        assertEquals("Flink114SubmitDialect", adapter.compatibility().submitDialect().getClass().getSimpleName(), "legacy compatibility");
    }

    private static void testDefaultSubmitDialectAppliesCommonOptions() throws Exception {
        RecordingConfigApplier applier = new RecordingConfigApplier();
        Map<String, Object> dynamic = new LinkedHashMap<String, Object>();
        dynamic.put("state.backend", "rocksdb");
        SubmitSpec spec = new SubmitSpec(
                "orders",
                "kubernetes-application",
                "local:///opt/flink/usrlib/orders.jar",
                "com.example.OrdersJob",
                new String[] {"--env", "prod"},
                4,
                "s3://bucket/savepoints/sp-1",
                dynamic);

        new DefaultSubmitDialect().applyCommonOptions(applier, spec);

        assertEquals("orders", String.valueOf(applier.values.get("pipeline.name")), "pipeline name");
        assertEquals("kubernetes-application", String.valueOf(applier.values.get("execution.target")), "execution target");
        assertContains(String.valueOf(applier.values.get("pipeline.jars")), "orders.jar", "pipeline jars");
        assertEquals("com.example.OrdersJob", String.valueOf(applier.values.get("application.main.class")), "main class");
        assertContains(String.valueOf(applier.values.get("application.args")), "--env", "application args");
        assertEquals(4, ((Integer) applier.values.get("parallelism.default")).intValue(), "parallelism");
        assertEquals("s3://bucket/savepoints/sp-1", String.valueOf(applier.values.get("execution.savepoint.path")), "savepoint path");
        assertEquals("rocksdb", String.valueOf(applier.values.get("state.backend")), "dynamic property");
    }

    private static void testFlink114SubmitDialectDisablesStopWithDrain() {
        assertEquals(false, new Flink114SubmitDialect().supportsStopWithDrain(), "flink 1.14 stop with drain support");
        assertEquals(true, new DefaultSubmitDialect().supportsStopWithDrain(), "default stop with drain support");
    }

    private static void testDefaultRestDialectReturnsCommonEndpoints() {
        RestDialect dialect = new DefaultRestDialect();

        assertEquals("GET", dialect.jobStatus("job-1").method(), "job status method");
        assertEquals("/jobs/job-1", dialect.jobStatus("job-1").path(), "job status path");
        assertEquals("/jobs/job-1/exceptions", dialect.jobExceptions("job-1").path(), "exceptions path");
        assertEquals("/jobs/job-1/checkpoints", dialect.jobCheckpoints("job-1").path(), "checkpoints path");
        assertEquals("POST", dialect.triggerSavepoint("job-1").method(), "savepoint method");
        assertEquals("/jobs/job-1/savepoints", dialect.triggerSavepoint("job-1").path(), "savepoint path");
    }

    private static void testDetectsFlinkConfigPrefixConflictsBeforeDeployment() {
        Map<String, String> config = new LinkedHashMap<String, String>();
        config.put("kubernetes.container.image", "flink:1.19");
        config.put("kubernetes.container.image.pull-policy", "IfNotPresent");

        java.util.List<String> conflicts = FlinkNativeKubernetesDeployer.detectPrefixConflicts(config);

        assertEquals(1, conflicts.size(), "prefix conflict count");
        assertContains(conflicts.get(0), "kubernetes.container.image", "prefix conflict parent");
        assertContains(conflicts.get(0), "kubernetes.container.image.pull-policy", "prefix conflict child");
    }

    private static void testStartJobUnknownProviderReturnsUnsupportedProvider() {
        CliResult result = run(
                "start_job",
                "--deployment-target", "standalone",
                "--name", "orders",
                "--jar-uri", "file:///apps/orders.jar",
                "--parallelism", "4",
                "--confirm",
                "--dry-run");

        assertEquals(1, result.exitCode, "start unknown provider exit code");
        assertContains(result.stdout, "\"code\":\"UnsupportedProvider\"", "unknown provider code");
        assertContains(result.stdout, "\"provider\":\"standalone\"", "unknown provider data");
    }

    private static void testKubernetesReadCommandsRequireDeploymentTarget() {
        CliResult result = run(
                "get_node_ips",
                "--dry-run");

        assertEquals(1, result.exitCode, "kubernetes read missing target exit code");
        assertContains(result.stdout, "\"code\":\"DeploymentTargetRequired\"", "kubernetes read missing target code");
        assertContains(result.stdout, "--deployment-target kubernetes", "kubernetes read missing target guidance");
    }

    private static void testKubernetesReadCommandsRejectYarnProvider() {
        CliResult result = run(
                "check_ingress_controller",
                "--deployment-target", "yarn",
                "--namespace", "data-flink",
                "--dry-run");

        assertEquals(1, result.exitCode, "kubernetes read yarn target exit code");
        assertContains(result.stdout, "\"code\":\"ProviderNotImplemented\"", "kubernetes read yarn target code");
        assertContains(result.stdout, "\"provider\":\"yarn\"", "kubernetes read yarn provider");
    }

    private static void testK8sAliasesMakeProviderExplicit() {
        CliResult nodeIps = run(
                "k8s_get_node_ips",
                "--dry-run");

        assertEquals(0, nodeIps.exitCode, "k8s get node ips alias exit code");
        assertContains(nodeIps.stdout, "\"operation\":\"k8s_get_node_ips\"", "k8s get node ips operation");
        assertContains(nodeIps.stdout, "InternalIP", "k8s get node ips data");

        CliResult ingress = run(
                "k8s_check_ingress_controller",
                "--namespace", "data-flink",
                "--dry-run");

        assertEquals(0, ingress.exitCode, "k8s check ingress alias exit code");
        assertContains(ingress.stdout, "\"operation\":\"k8s_check_ingress_controller\"", "k8s check ingress operation");
        assertContains(ingress.stdout, "data-flink-nginx-controller", "k8s ingress planned service");
    }

    private static void testOldEnvironmentCommandNamesAreRejected() {
        CliResult k8s = run(
                "k8s_check_environment",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--kubeconfig-path", "/Users/example/.kube/config",
                "--enable-ingress", "false");

        assertEquals(1, k8s.exitCode, "old k8s environment command exit code");
        assertContains(k8s.stdout, "\"code\":\"ValidationError\"", "old k8s environment command code");
        assertContains(k8s.stdout, "Unsupported command 'k8s_check_environment'", "old k8s environment command message");

        CliResult cli = run(
                "flink_check_submit_environment",
                "--flink-home", "/opt/flink",
                "--dry-run");

        assertEquals(1, cli.exitCode, "old flink submit environment command exit code");
        assertContains(cli.stdout, "\"code\":\"ValidationError\"", "old flink submit environment command code");
        assertContains(cli.stdout, "Unsupported command 'flink_check_submit_environment'", "old flink submit environment command message");
    }

    private static void testK8sCheckConnectivityReportsDeployTargetConnectivity() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway);

        CliResult result = runCli(cli,
                "k8s_check_connectivity",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--kubeconfig-path", "/Users/example/.kube/config",
                "--enable-ingress", "false");

        assertEquals(0, result.exitCode, "k8s check connectivity exit code");
        assertContains(result.stdout, "\"operation\":\"k8s_check_connectivity\"", "k8s connectivity operation");
        assertContains(result.stdout, "\"deployTargetConnectivity\"", "deploy target connectivity section");
        assertContains(result.stdout, "\"credentials\"", "credential metadata");
        assertContains(result.stdout, "\"namespace\"", "namespace check");
        assertDoesNotContain(result.stdout, "\"kubernetesEnvironment\"", "new command should use generic section");
    }

    private static void testYarnCheckConnectivityDryRunReportsDeployTargetConnectivity() {
        CliResult result = run(
                "yarn_check_connectivity",
                "--hadoop-conf-dir", "/etc/hadoop/conf",
                "--yarn-queue", "root.analytics",
                "--yarn-provided-lib-dirs", "hdfs:///flink/lib",
                "--flink-dist-jar", "hdfs:///flink/flink-dist.jar",
                "--dry-run");

        assertEquals(0, result.exitCode, "yarn check connectivity dry run exit code");
        assertContains(result.stdout, "\"operation\":\"yarn_check_connectivity\"", "yarn connectivity operation");
        assertContains(result.stdout, "\"deployTargetConnectivity\"", "deploy target connectivity section");
        assertContains(result.stdout, "\"resourceManager\"", "resource manager check");
        assertContains(result.stdout, "\"queue\"", "queue check");
        assertContains(result.stdout, "root.analytics", "queue value");
    }

    private static void testCheckCliEnvironmentDryRunReturnsCliExecutionEnvironment() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "check_cli_environment",
                "--deployment-target", "kubernetes",
                "--flink-home", "/opt/flink",
                "--dry-run");

        assertEquals(0, result.exitCode, "check cli environment dry run exit code");
        assertContains(result.stdout, "\"operation\":\"check_cli_environment\"", "cli environment operation");
        assertContains(result.stdout, "\"cliExecutionEnvironment\"", "cli execution environment section");
        assertContains(result.stdout, "\"java\"", "java check");
        assertContains(result.stdout, "\"runtimeClasspath\"", "runtime classpath check");
        assertDoesNotContain(result.stdout, "\"flinkSubmitEnvironment\"", "new command should use generic section");
    }

    private static void testCommonRestStatusDoesNotRequireProvider() {
        CliResult result = run(
                "get_job_status",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--dry-run");

        assertEquals(0, result.exitCode, "common rest status exit code");
        assertContains(result.stdout, "\"operation\":\"get_job_status\"", "common rest operation");
        assertDoesNotContain(result.stdout, "deployment-target", "common rest should not require provider");
    }

    private static void testStartJobRequiresConfirm() {
        CliResult result = run(
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "data-flink",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4");

        assertEquals(1, result.exitCode, "start without confirm exit code");
        assertContains(result.stdout, "\"success\":false", "start without confirm success flag");
        assertContains(result.stdout, "\"code\":\"SafetyCheckRequired\"", "start without confirm error code");
    }

    private static void testKubernetesStartRequiresParallelismBeforeSubmission() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway()) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                throw new AssertionError("deploy should not be called when required parameters are missing");
            }
        };

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--confirm");

        assertEquals(1, result.exitCode, "missing parallelism exit code");
        assertContains(result.stdout, "\"code\":\"ValidationError\"", "missing parallelism validation code");
        assertContains(result.stdout, "Parameter '--parallelism' is required.", "missing parallelism message");
    }

    private static void testKubernetesStartRejectsNonPositiveParallelismBeforeSubmission() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway()) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                throw new AssertionError("deploy should not be called when parallelism is invalid");
            }
        };

        CliResult result = runCli(cli,
                "k8s_start_job",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "0",
                "--confirm");

        assertEquals(1, result.exitCode, "invalid parallelism exit code");
        assertContains(result.stdout, "\"code\":\"ValidationError\"", "invalid parallelism validation code");
        assertContains(result.stdout, "Parameter '--parallelism' must be a positive integer.", "invalid parallelism message");
    }

    private static void testKubernetesStartRejectsJarUriWithoutSchemeBeforeSubmission() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway()) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                throw new AssertionError("deploy should not be called when jar uri is invalid");
            }
        };

        CliResult result = runCli(cli,
                "k8s_start_job",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "/opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--confirm");

        assertEquals(1, result.exitCode, "invalid jar uri exit code");
        assertContains(result.stdout, "\"code\":\"ValidationError\"", "invalid jar uri validation code");
        assertContains(result.stdout, "Parameter '--jar-uri' must be a URI visible inside the Flink container", "invalid jar uri message");
    }

    private static void testOperationExceptionReturnsSpecificCodeAndData() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway()) {
            @Override
            protected String dispatch(CommandContext context) {
                Map<String, Object> data = new LinkedHashMap<String, Object>();
                data.put("suggestedCommands", java.util.Arrays.asList("helm upgrade --install data-flink-nginx"));
                throw new OperationException(
                        "IngressControllerNotFound",
                        "No namespace-scoped Ingress Controller was found.",
                        data);
            }
        };

        CliResult result = runCli(cli, "start_job", "--confirm");

        assertEquals(1, result.exitCode, "operation exception exit code");
        assertContains(result.stdout, "\"code\":\"IngressControllerNotFound\"", "operation exception code");
        assertContains(result.stdout, "No namespace-scoped Ingress Controller was found.", "operation exception message");
        assertContains(result.stdout, "helm upgrade --install data-flink-nginx", "operation exception data");
    }

    private static void testOperationExceptionIncludesCauseChain() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway()) {
            protected String dispatch(CommandContext context) {
                throw new OperationException(
                        "ClusterDeploymentException",
                        "Could not create Kubernetes cluster \"orders\".",
                        new RuntimeException("The Flink cluster orders already exists."));
            }
        };

        CliResult result = runCli(cli, "k8s_start_job", "--confirm");

        assertEquals(1, result.exitCode, "operation exception cause chain exit code");
        assertContains(result.stdout, "\"code\":\"ClusterDeploymentException\"", "operation exception code");
        assertContains(result.stdout, "\"causes\":[", "operation exception causes");
        assertContains(result.stdout, "Could not create Kubernetes cluster", "operation exception outer cause");
        assertContains(result.stdout, "The Flink cluster orders already exists.", "operation exception root cause");
    }

    private static void testValidationExceptionIncludesCauseChain() {
        CliResult result = run("k8s_start_job", "--confirm");

        assertEquals(1, result.exitCode, "validation exception cause chain exit code");
        assertContains(result.stdout, "\"code\":\"ValidationError\"", "validation exception code");
        assertContains(result.stdout, "\"causes\":[", "validation exception causes");
        assertContains(result.stdout, "\"type\":\"ValidationException\"", "validation exception type");
    }

    private static void testInvocationTargetExceptionIncludesNestedCauseChain() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway()) {
            protected String dispatch(CommandContext context) throws Exception {
                RuntimeException root = new RuntimeException("Forbidden: service account cannot create pods.");
                RuntimeException cluster = new RuntimeException("Could not create Kubernetes cluster \"orders\".", root);
                throw new InvocationTargetException(cluster);
            }
        };

        CliResult result = runCli(cli, "k8s_start_job", "--confirm");

        assertEquals(1, result.exitCode, "invocation target cause chain exit code");
        assertContains(result.stdout, "\"code\":\"RuntimeException\"", "invocation target unwrapped code");
        assertContains(result.stdout, "\"causes\":[", "invocation target causes");
        assertContains(result.stdout, "Could not create Kubernetes cluster", "invocation target outer cause");
        assertContains(result.stdout, "Forbidden: service account cannot create pods.", "invocation target root cause");
        assertContains(result.stdout, "\"stackTrace\":[", "invocation target stack trace");
    }

    private static void testBuildImageRequiresConfirm() {
        CliResult result = run(
                "build_image",
                "--deployment-target", "kubernetes",
                "--base-image", "flink:1.19.3-scala_2.12-java11",
                "--local-jar", "/tmp/orders.jar",
                "--target-image", "orders-flink:dev",
                "--target-jar-path", "/opt/flink/usrlib/orders.jar");

        assertEquals(1, result.exitCode, "build image without confirm exit code");
        assertContains(result.stdout, "\"success\":false", "build image without confirm success flag");
        assertContains(result.stdout, "\"code\":\"SafetyCheckRequired\"", "build image without confirm error code");
    }

    private static void testBuildImageDryRunReturnsDockerfilePlan() {
        CliResult result = run(
                "build_image",
                "--deployment-target", "kubernetes",
                "--base-image", "flink:1.19.3-scala_2.12-java11",
                "--local-jar", "/tmp/orders.jar",
                "--target-image", "orders-flink:dev",
                "--target-jar-path", "/opt/flink/usrlib/orders.jar",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "build image dry run exit code");
        assertContains(result.stdout, "\"operation\":\"build_image\"", "build image operation");
        assertContains(result.stdout, "FROM flink:1.19.3-scala_2.12-java11", "build image dockerfile base");
        assertContains(result.stdout, "COPY job.jar /opt/flink/usrlib/orders.jar", "build image dockerfile copy");
        assertContains(result.stdout, "local:///opt/flink/usrlib/orders.jar", "build image resulting jar uri");
    }

    private static void testBuildImageDryRunDefaultsTargetJarPath() {
        CliResult result = run(
                "build_image",
                "--deployment-target", "kubernetes",
                "--base-image", "flink:1.19.3-scala_2.12-java11",
                "--local-jar", "/tmp/orders.jar",
                "--target-image", "orders-flink:dev",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "build image default target path dry run exit code");
        assertContains(result.stdout, "\"targetJarPath\":\"/opt/flink/usrlib/orders.jar\"", "default target jar path");
        assertContains(result.stdout, "local:///opt/flink/usrlib/orders.jar", "default resulting jar uri");
    }

    private static void testPreflightStartDryRunChecksRuntimeAndIngress() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "preflight_start",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-home", "/opt/flink",
                "--enable-ingress", "true",
                "--dry-run");

        assertEquals(0, result.exitCode, "preflight start dry run exit code");
        assertContains(result.stdout, "\"operation\":\"preflight_start\"", "preflight operation");
        assertContains(result.stdout, "\"preflight\"", "preflight section");
        assertContains(result.stdout, "\"deployTargetConnectivity\"", "deploy target connectivity section");
        assertContains(result.stdout, "\"cliExecutionEnvironment\"", "cli execution environment section");
        assertContains(result.stdout, "data-flink-nginx", "preflight ingress class");
        assertContains(result.stdout, "\"planned\":true", "preflight dry run planned");
    }

    private static void testPreflightStartWithIngressDisabledSkipsIngressChecks() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.ingressControllerReady = false;
        gateway.ingressClassExists = false;
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway);

        CliResult result = runCli(cli,
                "preflight_start",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-home", "/opt/flink",
                "--enable-ingress", "false",
                "--dry-run");

        assertEquals(0, result.exitCode, "preflight start ingress disabled exit code");
        assertContains(result.stdout, "\"enableIngress\":false", "preflight ingress disabled flag");
        assertContains(result.stdout, "Ingress checks skipped by --enable-ingress=false.", "preflight ingress skipped");
        assertDoesNotContain(result.stdout, "data-flink-nginx", "preflight disabled should not include ingress class");
    }

    private static void testStartJobDryRunReturnsFlinkNativeClientPlan() {
        CliResult result = run(
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "data-flink",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--main-class", "com.example.OrdersJob",
                "--args", "--env prod",
                "--parallelism", "4",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "start dry run exit code");
        assertContains(result.stdout, "\"success\":true", "start dry run success flag");
        assertContains(result.stdout, "\"operation\":\"start_job\"", "start dry run operation");
        assertContains(result.stdout, "flink-java-client", "start dry run deployment engine");
        assertContains(result.stdout, "\"submitDialect\":\"DefaultSubmitDialect\"", "kubernetes submit dialect");
        assertContains(result.stdout, "KubernetesClusterClientFactory", "start dry run client factory");
        assertContains(result.stdout, "deployApplicationCluster", "start dry run deploy method");
        assertContains(result.stdout, "kubernetes.cluster-id", "start dry run cluster id key");
        assertContains(result.stdout, "data-flink", "start dry run namespace and service account");
        assertContains(result.stdout, "kubernetes.container.image", "start dry run image key");
        assertDoesNotContain(result.stdout, "flink run-application", "start dry run must not use flink shell command");
        assertDoesNotContain(result.stdout, "kubectl", "start dry run must not use kubectl");
    }

    private static void testStartJobDryRunPropagatesKubeconfigToFlinkDeploymentConfig() {
        CliResult result = run(
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "data-flink",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--kubeconfig-path", "/Users/example/.kube/config",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "start dry run with kubeconfig exit code");
        assertContains(result.stdout, "\"kubernetes.config.file\":\"/Users/example/.kube/config\"", "flink deployment kubeconfig");
        assertContains(result.stdout, "\"credentials\":{\"source\":\"explicit\",\"path\":\"/Users/example/.kube/config\"", "start preflight kubeconfig");
    }

    private static void testStartJobDryRunIncludesPreflightAndMandatoryIngressPlan() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway);

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "start dry run with ingress exit code");
        assertContains(result.stdout, "\"preflight\"", "start dry run preflight");
        assertContains(result.stdout, "\"ingress\"", "start dry run ingress");
        assertContains(result.stdout, "orders.flink.k8s.com", "start dry run ingress host");
        assertContains(result.stdout, "orders-rest", "start dry run backend service");
        assertEquals(false, gateway.ingressUpserted, "dry run must not create ingress");
    }

    private static void testStartJobDryRunIncludesStagedPlanAndNextActions() {
        CliResult result = run(
                "k8s_start_job",
                "--namespace", "analytics",
                "--service-account", "flink",
                "--name", "orders",
                "--flink-home", "/opt/flink",
                "--flink-version", "1.19.1",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "2",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "staged start dry run exit code");
        assertContains(result.stdout, "\"plan\"", "staged start plan");
        assertContains(result.stdout, "\"preflight\"", "staged start preflight");
        assertContains(result.stdout, "\"deployTargetConnectivity\"", "staged start deploy target connectivity");
        assertContains(result.stdout, "\"cliExecutionEnvironment\"", "staged start cli execution environment");
        assertContains(result.stdout, "\"nextActions\"", "staged start next actions");
    }

    private static void testStartJobDryRunWithIngressDisabledSkipsIngressChecks() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.ingressControllerReady = false;
        gateway.ingressClassExists = false;
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway);

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--enable-ingress", "false",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "start dry run ingress disabled exit code");
        assertContains(result.stdout, "\"ingressEnabled\":false", "ingress disabled flag");
        assertContains(result.stdout, "Ingress disabled by --enable-ingress=false.", "ingress disabled reason");
        assertContains(result.stdout, "orders-rest.data-flink.svc.cluster.local", "internal jm url");
        assertDoesNotContain(result.stdout, "ingressController-check", "ingress controller check should not appear");
    }

    private static void testStartJobReturnsIngressAccessNodePorts() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                Map<String, Object> result = new LinkedHashMap<String, Object>();
                result.put("clusterId", spec.name);
                result.put("jobManagerUrl", "http://orders-rest.data-flink.svc.cluster.local:8081");
                return result;
            }
        };

        CliResult result = runCli(cli,
                "k8s_start_job",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--confirm");

        assertEquals(0, result.exitCode, "start ingress access exit code");
        assertContains(result.stdout, "\"ingressAccess\"", "ingress access section");
        assertContains(result.stdout, "\"host\":\"orders.flink.k8s.com\"", "ingress access host");
        assertContains(result.stdout, "\"httpNodePort\":30080", "ingress access http nodeport");
        assertContains(result.stdout, "\"httpsNodePort\":30443", "ingress access https nodeport");
        assertContains(result.stdout, "\"httpUrl\":\"http://orders.flink.k8s.com:30080\"", "ingress access http url");
        assertContains(result.stdout, "\"httpsUrl\":\"https://orders.flink.k8s.com:30443\"", "ingress access https url");
    }

    private static void testStartJobWithIngressDisabledSkipsIngressCreation() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.ingressControllerReady = false;
        gateway.ingressClassExists = false;
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                Map<String, Object> result = new LinkedHashMap<String, Object>();
                result.put("clusterId", spec.name);
                result.put("jobManagerUrl", "http://orders-rest.data-flink.svc.cluster.local:8081");
                return result;
            }
        };

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--enable-ingress", "false",
                "--confirm");

        assertEquals(0, result.exitCode, "start ingress disabled exit code");
        assertContains(result.stdout, "\"ingressEnabled\":false", "ingress disabled flag");
        assertContains(result.stdout, "\"submission\"", "submission still happens");
        assertContains(result.stdout, "orders-rest.data-flink.svc.cluster.local", "internal jm url");
        assertEquals(false, gateway.ingressUpserted, "ingress disabled must not create ingress");
    }

    private static void testStartJobWithoutFlinkRuntimeClasspathReturnsSpecificError() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--enable-ingress", "false",
                "--confirm");

        assertEquals(1, result.exitCode, "start missing runtime classpath exit code");
        assertContains(result.stdout, "\"code\":\"FlinkRuntimeClasspathMissing\"", "missing runtime classpath code");
        assertContains(result.stdout, "org.apache.flink.configuration.Configuration", "missing runtime class");
        assertContains(result.stdout, "$FLINK_HOME/lib/*", "missing runtime classpath guidance");
    }

    private static void testKubernetesIngressSpecDerivesHostAndManifest() {
        KubernetesIngressSpec spec = KubernetesIngressSpec.from("data-flink", "flink-example-streaming");
        Map<String, Object> plan = spec.plan();

        assertEquals("flink-example-streaming-flink-rest", spec.ingressName, "ingress name");
        assertEquals("flink-example-streaming-rest", spec.serviceName, "ingress service name");
        assertEquals("data-flink-nginx", spec.ingressClassName, "ingress class");
        assertEquals("flink-example-streaming.flink.k8s.com", spec.host, "ingress host");
        assertEquals("https://flink-example-streaming.flink.k8s.com", spec.url, "ingress url");
        assertContains(Json.stringify(plan), "\"path\":\"/\"", "ingress path");
        assertContains(Json.stringify(plan), "\"servicePort\":8081", "ingress service port");
    }

    private static void testCheckIngressControllerDryRunReturnsPlannedChecks() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "check_ingress_controller",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink",
                "--dry-run");

        assertEquals(0, result.exitCode, "check ingress dry run exit code");
        assertContains(result.stdout, "\"operation\":\"check_ingress_controller\"", "check ingress operation");
        assertContains(result.stdout, "data-flink-nginx", "check ingress class");
        assertContains(result.stdout, "\"planned\":true", "check ingress planned");
        assertContains(result.stdout, "data-flink-nginx-controller", "check ingress service planned");
    }

    private static void testCheckIngressControllerMissingControllerReturnsRemediation() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.ingressControllerReady = false;
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway);

        CliResult result = runCli(cli,
                "check_ingress_controller",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink");

        assertEquals(1, result.exitCode, "check ingress missing controller exit code");
        assertContains(result.stdout, "\"code\":\"IngressControllerNotFound\"", "check ingress missing controller code");
        assertContains(result.stdout, "kubectlTemplatePath", "check ingress kubectl template path");
        assertContains(result.stdout, "kubectlManualRenderCommands", "check ingress kubectl render command");
        assertContains(result.stdout, "kubectlManualApplyCommands", "check ingress kubectl apply commands");
        assertContains(result.stdout, "k8s_render_ingress_controller_yaml --namespace data-flink", "check ingress render command namespace");
        assertContains(result.stdout, "kubectl apply -f /tmp/data-flink-nginx-controller.yaml", "check ingress apply command");
    }

    private static void testCheckIngressControllerReturnsNodePortService() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "check_ingress_controller",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink");

        assertEquals(0, result.exitCode, "check ingress nodeport exit code");
        assertContains(result.stdout, "\"ingressControllerService\"", "nodeport service section");
        assertContains(result.stdout, "\"type\":\"NodePort\"", "nodeport service type");
        assertContains(result.stdout, "\"name\":\"http\"", "nodeport http name");
        assertContains(result.stdout, "\"nodePort\":30080", "http nodeport");
        assertContains(result.stdout, "\"name\":\"https\"", "nodeport https name");
        assertContains(result.stdout, "\"nodePort\":30443", "https nodeport");
    }

    private static void testGetNodeIpsDryRunReturnsPlannedRead() {
        CliResult result = run(
                "get_node_ips",
                "--deployment-target", "kubernetes",
                "--dry-run");

        assertEquals(0, result.exitCode, "get node ips dry run exit code");
        assertContains(result.stdout, "\"operation\":\"get_node_ips\"", "get node ips operation");
        assertContains(result.stdout, "\"planned\":true", "get node ips planned");
        assertContains(result.stdout, "InternalIP", "get node ips planned internal ip");
        assertContains(result.stdout, "ExternalIP", "get node ips planned external ip");
    }

    private static void testGetNodeIpsReturnsNodeAddresses() {
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "get_node_ips",
                "--deployment-target", "kubernetes");

        assertEquals(0, result.exitCode, "get node ips exit code");
        assertContains(result.stdout, "\"operation\":\"get_node_ips\"", "get node ips operation");
        assertContains(result.stdout, "\"name\":\"node-a\"", "node name");
        assertContains(result.stdout, "\"internalIP\":\"10.0.0.11\"", "node internal ip");
        assertContains(result.stdout, "\"externalIP\":\"203.0.113.11\"", "node external ip");
        assertContains(result.stdout, "\"hostname\":\"node-a.local\"", "node hostname");
    }

    private static void testRenderIngressControllerYamlOutputsTemplate() {
        CliResult result = run(
                "render_ingress_controller_yaml",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink");

        assertEquals(0, result.exitCode, "render ingress controller yaml exit code");
        assertContains(result.stdout, "kind: ServiceAccount", "render yaml service account");
        assertContains(result.stdout, "namespace: data-flink", "render yaml namespace");
        assertContains(result.stdout, "resources: [\"endpointslices\"]", "render yaml endpoint slices permission");
        assertContains(result.stdout, "resources: [\"namespaces\"]", "render yaml namespace get permission");
        assertContains(result.stdout, "POD_NAME", "render yaml pod name env");
        assertContains(result.stdout, "POD_NAMESPACE", "render yaml pod namespace env");
        assertContains(result.stdout, "--watch-namespace=data-flink", "render yaml watch namespace");
        assertDoesNotContain(result.stdout, "{{NAMESPACE}}", "render yaml no namespace placeholder");
    }

    private static void testPreflightMissingNamespaceFailsBeforeStart() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.namespaceExists = false;
        KubernetesEnvironmentChecker checker = new KubernetesEnvironmentChecker(gateway);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from("data-flink", "orders");

        try {
            checker.check("data-flink", "flink-sa", ingress);
            throw new AssertionError("expected NamespaceNotFound");
        } catch (OperationException e) {
            assertEquals("NamespaceNotFound", e.code(), "missing namespace code");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void testPreflightMissingServiceAccountFailsBeforeStart() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.serviceAccountExists = false;
        KubernetesEnvironmentChecker checker = new KubernetesEnvironmentChecker(gateway);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from("data-flink", "orders");

        try {
            checker.check("data-flink", "flink-sa", ingress);
            throw new AssertionError("expected ServiceAccountNotFound");
        } catch (OperationException e) {
            assertEquals("ServiceAccountNotFound", e.code(), "missing service account code");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void testPreflightMissingIngressControllerReturnsRemediation() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.ingressControllerReady = false;
        KubernetesEnvironmentChecker checker = new KubernetesEnvironmentChecker(gateway);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from("data-flink", "orders");

        try {
            checker.check("data-flink", "flink-sa", ingress);
            throw new AssertionError("expected IngressControllerNotFound");
        } catch (OperationException e) {
            assertEquals("IngressControllerNotFound", e.code(), "missing controller code");
            assertContains(Json.stringify(e.data()), "helm upgrade --install", "controller remediation");
            assertContains(Json.stringify(e.data()), "kubectlTemplatePath", "kubectl template path remediation key");
            assertContains(Json.stringify(e.data()), "kubectlManualRenderCommands", "kubectl render remediation key");
            assertContains(Json.stringify(e.data()), "kubectlManualApplyCommands", "kubectl apply remediation key");
            assertContains(Json.stringify(e.data()), "k8s_render_ingress_controller_yaml --namespace data-flink", "kubectl render command");
            assertContains(Json.stringify(e.data()), "kubectl apply -f /tmp/data-flink-nginx-controller.yaml", "kubectl apply command");
            assertContains(Json.stringify(e.data()), "data-flink-nginx", "controller class remediation");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void testPreflightMissingIngressClassFails() {
        FakeKubernetesGateway gateway = new FakeKubernetesGateway();
        gateway.ingressClassExists = false;
        KubernetesEnvironmentChecker checker = new KubernetesEnvironmentChecker(gateway);
        KubernetesIngressSpec ingress = KubernetesIngressSpec.from("data-flink", "orders");

        try {
            checker.check("data-flink", "flink-sa", ingress);
            throw new AssertionError("expected IngressClassNotFound");
        } catch (OperationException e) {
            assertEquals("IngressClassNotFound", e.code(), "missing ingress class code");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void testStartJobIngressCreationFailureReturnsSpecificError() {
        KubernetesGateway gateway = new FakeKubernetesGateway() {
            public Map<String, Object> upsertIngress(KubernetesIngressSpec spec) {
                throw new OperationException("IngressCreationFailed", "Failed to create Ingress 'orders-flink-rest'.");
            }
        };
        FlinkOpsCli cli = new FlinkOpsCli(new FlinkRestClient(), gateway) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                Map<String, Object> result = new LinkedHashMap<String, Object>();
                result.put("clusterId", spec.name);
                result.put("jobManagerUrl", "http://orders-rest.data-flink.svc.cluster.local:8081");
                return result;
            }
        };

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--confirm");

        assertEquals(1, result.exitCode, "ingress creation failure exit code");
        assertContains(result.stdout, "\"code\":\"IngressCreationFailed\"", "ingress creation failure code");
    }

    private static void testGetStatusDryRunReturnsFlinkRestEndpoints() {
        CliResult result = run(
                "get_job_status",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--dry-run");

        assertEquals(0, result.exitCode, "status dry run exit code");
        assertContains(result.stdout, "\"success\":true", "status dry run success flag");
        assertContains(result.stdout, "\"operation\":\"get_job_status\"", "status dry run operation");
        assertContains(result.stdout, "\"restDialect\":\"DefaultRestDialect\"", "status rest dialect");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1", "status dry run job endpoint");
    }

    private static void testGetStatusDryRunIncludesHttpHostHeader() {
        CliResult result = run(
                "get_job_status",
                "--job-manager-url", "http://127.0.0.1:31626",
                "--job-id", "job-1",
                "--http-host-header", "topspeed-windowing.flink.k8s.com",
                "--dry-run");

        assertEquals(0, result.exitCode, "status host header dry run exit code");
        assertContains(result.stdout, "\"httpHostHeader\":\"topspeed-windowing.flink.k8s.com\"", "status host header dry run");
    }

    private static void testGetStatusSendsHttpHostHeader() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "get_job_status",
                "--job-manager-url", "http://127.0.0.1:31626",
                "--job-id", "job-1",
                "--http-host-header", "topspeed-windowing.flink.k8s.com");

        assertEquals(0, result.exitCode, "status host header real exit code");
        assertEquals("topspeed-windowing.flink.k8s.com", restClient.lastHostHeader, "recorded host header");
        assertContains(result.stdout, "\"responseJson\":\"{\\\"jobs\\\":[]}\"", "recorded response json");
    }

    private static void testInspectClusterDryRunReturnsInspectionPlan() {
        CliResult result = run(
                "inspect_cluster",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--http-host-header", "orders.flink.k8s.com",
                "--report",
                "--dry-run");

        assertEquals(0, result.exitCode, "inspect dry run exit code");
        assertContains(result.stdout, "\"operation\":\"inspect_cluster\"", "inspect operation");
        assertContains(result.stdout, "\"inspectionScope\"", "inspect scope");
        assertContains(result.stdout, "\"reportRequested\":true", "inspect report flag");
        assertContains(result.stdout, "http://jm:8081/overview", "inspect overview endpoint");
        assertContains(result.stdout, "http://jm:8081/config", "inspect dashboard config endpoint");
        assertContains(result.stdout, "http://jm:8081/taskmanagers", "inspect taskmanagers endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/checkpoints", "inspect checkpoints endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/plan", "inspect job plan endpoint");
    }

    private static void testInspectClusterReportCollectsEvidence() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://jm:8081/overview", "{\"taskmanagers\":1,\"slots-total\":4,\"slots-available\":1}");
        restClient.responses.put("http://jm:8081/config", "{\"flink-version\":\"1.19.3\"}");
        restClient.responses.put("http://jm:8081/taskmanagers", "{\"taskmanagers\":[{\"id\":\"tm-1\",\"blocked\":false}]}");
        restClient.responses.put("http://jm:8081/jobs", "{\"jobs\":[{\"id\":\"job-1\",\"status\":\"RUNNING\"}]}");
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"jid\":\"job-1\",\"state\":\"RUNNING\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/status", "{\"status\":{\"id\":\"RUNNING\"}}");
        restClient.responses.put("http://jm:8081/jobs/job-1/config", "{\"parallelism\":4}");
        restClient.responses.put("http://jm:8081/jobs/job-1/plan", "{\"plan\":{\"nodes\":[]}}");
        restClient.responses.put("http://jm:8081/jobs/job-1/metrics?get=numRestarts,fullRestarts", "[{\"id\":\"numRestarts\",\"value\":\"0\"}]");
        restClient.responses.put("http://jm:8081/jobs/job-1/exceptions", "{\"root-exception\":null,\"all-exceptions\":[]}");
        restClient.responses.put("http://jm:8081/jobs/job-1/checkpoints", "{\"latest\":{\"completed\":{\"id\":7}}}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "inspect_cluster",
                "--deployment-target", "kubernetes",
                "--namespace", "data-flink",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--report");

        assertEquals(0, result.exitCode, "inspect report exit code");
        assertContains(result.stdout, "\"healthStatus\":\"healthy\"", "inspect healthy status");
        assertContains(result.stdout, "\"riskLevel\":\"low\"", "inspect low risk");
        assertContains(result.stdout, "\"evidence\"", "inspect evidence");
        assertContains(result.stdout, "\"recommendations\"", "inspect recommendations");
        assertEquals(11, restClient.getCount, "inspect rest call count");
    }

    private static void testDiagnoseDryRunIncludesFlink19EvidenceEndpoints() {
        CliResult result = run(
                "diagnose_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--report",
                "--dry-run");

        assertEquals(0, result.exitCode, "diagnose flink 1.19 dry run exit code");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/status", "diagnose status endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/config", "diagnose config endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/plan", "diagnose plan endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/metrics?get=numRestarts,fullRestarts", "diagnose metrics endpoint");
    }

    private static void testDiagnoseReportReturnsFindingsAndRecommendations() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://jm:8081/overview", "{\"taskmanagers\":1}");
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"jid\":\"job-1\",\"state\":\"FAILED\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/exceptions", "{\"root-exception\":\"checkpoint timeout\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/checkpoints", "{\"latest\":{\"failed\":{\"failure_message\":\"timeout\"}}}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "diagnose_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--report");

        assertEquals(0, result.exitCode, "diagnose report exit code");
        assertContains(result.stdout, "\"healthStatus\":\"critical\"", "diagnose critical status");
        assertContains(result.stdout, "\"rootCauseCandidates\"", "diagnose root cause candidates");
        assertContains(result.stdout, "checkpoint", "diagnose checkpoint finding");
        assertContains(result.stdout, "\"recommendations\"", "diagnose recommendations");
    }

    private static void testHealthReportBuilderProducesDeterministicRiskFields() {
        Map<String, Object> evidence = new LinkedHashMap<String, Object>();
        evidence.put("job", "{\"state\":\"FAILED\",\"root-exception\":\"boom\"}");

        Map<String, Object> report = new HealthReportBuilder().fromEvidence(evidence, "Unit report.");

        assertEquals("critical", String.valueOf(report.get("healthStatus")), "report health status");
        assertEquals("high", String.valueOf(report.get("riskLevel")), "report risk level");
        assertContains(String.valueOf(report.get("findings")), "failure", "report findings");
        assertContains(String.valueOf(report.get("recommendations")), "exception", "report recommendations");
    }

    private static void testDiagnoseReportFlagsTaskmanagerAndRestartRisks() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://jm:8081/overview", "{\"taskmanagers\":1,\"slots-total\":4,\"slots-available\":0}");
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"jid\":\"job-1\",\"state\":\"RUNNING\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/status", "{\"status\":{\"id\":\"RUNNING\"}}");
        restClient.responses.put("http://jm:8081/jobs/job-1/config", "{\"parallelism\":4}");
        restClient.responses.put("http://jm:8081/jobs/job-1/plan", "{\"plan\":{\"nodes\":[]}}");
        restClient.responses.put("http://jm:8081/jobs/job-1/metrics?get=numRestarts,fullRestarts", "[{\"id\":\"numRestarts\",\"value\":\"3\"}]");
        restClient.responses.put("http://jm:8081/jobs/job-1/exceptions", "{\"root-exception\":null}");
        restClient.responses.put("http://jm:8081/jobs/job-1/checkpoints", "{\"latest\":{\"completed\":{\"id\":2}}}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "diagnose_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--report");

        assertEquals(0, result.exitCode, "diagnose risk report exit code");
        assertContains(result.stdout, "\"healthStatus\":\"warning\"", "diagnose warning status");
        assertContains(result.stdout, "No available slots", "diagnose slots finding");
        assertContains(result.stdout, "restart", "diagnose restart finding");
    }

    private static void testDiagnoseReportDoesNotTreatZeroFailedCheckpointsAsFailure() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://jm:8081/overview", "{\"taskmanagers\":1,\"slots-total\":1,\"slots-available\":0,\"jobs-failed\":0}");
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"jid\":\"job-1\",\"state\":\"RUNNING\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/status", "{\"status\":\"RUNNING\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/config", "{\"parallelism\":1}");
        restClient.responses.put("http://jm:8081/jobs/job-1/plan", "{\"plan\":{\"nodes\":[]}}");
        restClient.responses.put("http://jm:8081/jobs/job-1/metrics?get=numRestarts,fullRestarts", "[{\"id\":\"numRestarts\",\"value\":\"0\"},{\"id\":\"fullRestarts\",\"value\":\"0\"}]");
        restClient.responses.put("http://jm:8081/jobs/job-1/exceptions", "{\"root-exception\":null,\"all-exceptions\":[]}");
        restClient.responses.put("http://jm:8081/jobs/job-1/checkpoints", "{\"counts\":{\"total\":0,\"completed\":0,\"failed\":0},\"summary\":{\"checkpointed_size\":{\"min\":0}},\"latest\":{\"completed\":null,\"failed\":null}}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "diagnose_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--report");

        assertEquals(0, result.exitCode, "diagnose zero failed checkpoints exit code");
        assertContains(result.stdout, "\"healthStatus\":\"warning\"", "zero failed checkpoints should not be critical");
        assertDoesNotContain(result.stdout, "Checkpoint evidence contains timeout or failed checkpoint signals.", "zero failed checkpoints should not produce checkpoint failure finding");
    }

    private static void testDiagnoseBackpressureDryRunReturnsEndpoints() {
        CliResult result = run(
                "diagnose_backpressure",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--dry-run");

        assertEquals(0, result.exitCode, "backpressure dry run exit code");
        assertContains(result.stdout, "\"operation\":\"diagnose_backpressure\"", "backpressure operation");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1", "backpressure job detail endpoint");
        assertContains(result.stdout, "/vertices/<vertex-id>/backpressure", "backpressure templated endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/checkpoints", "backpressure checkpoint endpoint");
    }

    private static void testDiagnoseBackpressureReportCollectsEvidence() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"state\":\"RUNNING\"}");
        restClient.responses.put("http://jm:8081/jobs/job-1/vertices", "{\"vertices\":[{\"id\":\"v1\",\"name\":\"sink\",\"status\":\"RUNNING\"}]}");
        restClient.responses.put("http://jm:8081/jobs/job-1/exceptions", "{\"root-exception\":null}");
        restClient.responses.put("http://jm:8081/jobs/job-1/checkpoints", "{\"latest\":{\"completed\":{\"id\":2}}}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "diagnose_backpressure",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--vertex-id", "v1",
                "--report");

        assertEquals(0, result.exitCode, "backpressure report exit code");
        assertContains(result.stdout, "\"healthStatus\":\"unknown\"", "backpressure unknown without metrics");
        assertContains(result.stdout, "\"rootCauseCandidates\"", "backpressure root causes");
        assertContains(result.stdout, "\"evidence\"", "backpressure evidence");
    }

    private static void testDiagnoseBackpressureDiscoversVerticesAndDetectsHighPressure() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"jid\":\"job-1\",\"state\":\"RUNNING\",\"vertices\":[{\"id\":\"v-source\",\"name\":\"source\"},{\"id\":\"v-sink\",\"name\":\"sink\"}]}");
        restClient.responses.put("http://jm:8081/jobs/job-1/vertices/v-source/backpressure", "{\"status\":\"ok\",\"backpressure-level\":\"ok\",\"subtasks\":[{\"subtask\":0,\"backpressure-level\":\"ok\",\"ratio\":0.01,\"busyRatio\":0.2,\"idleRatio\":0.7}]}");
        restClient.responses.put("http://jm:8081/jobs/job-1/vertices/v-source/subtasks/metrics?get=backPressuredTimeMsPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond&agg=min,max,avg", "[{\"id\":\"backPressuredTimeMsPerSecond\",\"max\":\"1.0\",\"avg\":\"0.5\"}]");
        restClient.responses.put("http://jm:8081/jobs/job-1/vertices/v-sink/backpressure", "{\"status\":\"ok\",\"backpressure-level\":\"high\",\"subtasks\":[{\"subtask\":0,\"backpressure-level\":\"high\",\"ratio\":0.99,\"busyRatio\":0.95,\"idleRatio\":0.0}]}");
        restClient.responses.put("http://jm:8081/jobs/job-1/vertices/v-sink/subtasks/metrics?get=backPressuredTimeMsPerSecond,busyTimeMsPerSecond,idleTimeMsPerSecond&agg=min,max,avg", "[{\"id\":\"backPressuredTimeMsPerSecond\",\"max\":\"999.0\",\"avg\":\"800.0\"}]");
        restClient.responses.put("http://jm:8081/jobs/job-1/exceptions", "{\"root-exception\":null}");
        restClient.responses.put("http://jm:8081/jobs/job-1/checkpoints", "{\"latest\":{\"completed\":{\"id\":2}}}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "diagnose_backpressure",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--report");

        assertEquals(0, result.exitCode, "backpressure high report exit code");
        assertContains(result.stdout, "\"healthStatus\":\"critical\"", "backpressure high health");
        assertContains(result.stdout, "v-sink", "backpressure bottleneck vertex");
        assertContains(result.stdout, "backpressure-level", "backpressure endpoint evidence");
        assertContains(result.stdout, "subtasks/metrics", "backpressure metrics endpoint evidence");
    }

    private static void testStopVerifyReadsBackTargetStatus() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.patchResponse = "{\"request\":\"accepted\"}";
        restClient.responses.put("http://jm:8081/jobs/job-1", "{\"jid\":\"job-1\",\"state\":\"CANCELED\"}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway());

        CliResult result = runCli(cli,
                "stop_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--stop-mode", "cancel",
                "--confirm",
                "--verify");

        assertEquals(0, result.exitCode, "stop verify exit code");
        assertContains(result.stdout, "\"targetLock\"", "stop target lock");
        assertContains(result.stdout, "\"readBackVerification\"", "stop readback verification");
        assertContains(result.stdout, "\"verified\":true", "stop verified");
    }

    private static void testStopIncludesTargetLockAndRejectsMismatch() {
        CliResult dryRun = run(
                "stop_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--confirm",
                "--dry-run");

        assertEquals(0, dryRun.exitCode, "stop target lock dry run exit code");
        assertContains(dryRun.stdout, "\"targetLock\"", "stop target lock present");
        assertContains(dryRun.stdout, "\"targetFingerprint\"", "stop target fingerprint present");

        CliResult mismatch = run(
                "stop_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--expected-target-lock", "wrong",
                "--confirm",
                "--dry-run");

        assertEquals(1, mismatch.exitCode, "stop lock mismatch exit code");
        assertContains(mismatch.stdout, "\"code\":\"TargetLockMismatch\"", "target lock mismatch code");
    }

    private static void testStartVerifyReadsBackAfterSubmission() {
        RecordingFlinkRestClient restClient = new RecordingFlinkRestClient();
        restClient.responses.put("http://orders-rest.data-flink.svc.cluster.local:8081/overview", "{\"taskmanagers\":1}");
        restClient.responses.put("http://orders-rest.data-flink.svc.cluster.local:8081/jobs", "{\"jobs\":[{\"id\":\"job-1\",\"status\":\"RUNNING\"}]}");
        FlinkOpsCli cli = new FlinkOpsCli(restClient, new FakeKubernetesGateway()) {
            protected Map<String, Object> deployKubernetes(FlinkKubernetesApplicationSpec spec) {
                Map<String, Object> result = new LinkedHashMap<String, Object>();
                result.put("clusterId", spec.name);
                result.put("jobManagerUrl", "http://orders-rest.data-flink.svc.cluster.local:8081");
                return result;
            }
        };

        CliResult result = runCli(cli,
                "start_job",
                "--deployment-target", "kubernetes",
                "--name", "orders",
                "--namespace", "data-flink",
                "--service-account", "flink-sa",
                "--flink-image", "flink:1.19",
                "--jar-uri", "local:///opt/flink/usrlib/orders.jar",
                "--parallelism", "4",
                "--enable-ingress", "false",
                "--confirm",
                "--verify");

        assertEquals(0, result.exitCode, "start verify exit code");
        assertContains(result.stdout, "\"targetLock\"", "start target lock");
        assertContains(result.stdout, "\"readBackVerification\"", "start readback verification");
        assertContains(result.stdout, "\"verified\":true", "start verified");
    }

    private static void testDiagnoseDryRunReturnsCommonFlinkRestEndpoints() {
        CliResult result = run(
                "diagnose_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--dry-run");

        assertEquals(0, result.exitCode, "diagnose dry run exit code");
        assertContains(result.stdout, "\"success\":true", "diagnose dry run success flag");
        assertContains(result.stdout, "\"restDialect\":\"DefaultRestDialect\"", "diagnose rest dialect");
        assertContains(result.stdout, "http://jm:8081/overview", "diagnose overview endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/exceptions", "diagnose exceptions endpoint");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/checkpoints", "diagnose checkpoints endpoint");
    }

    private static void testGetExceptionsDryRunReturnsFlinkRestEndpoint() {
        CliResult result = run(
                "get_exceptions",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--dry-run");

        assertEquals(0, result.exitCode, "exceptions dry run exit code");
        assertContains(result.stdout, "\"operation\":\"get_exceptions\"", "exceptions operation");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/exceptions", "exceptions endpoint");
    }

    private static void testGetCheckpointsDryRunReturnsFlinkRestEndpoint() {
        CliResult result = run(
                "get_checkpoints",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--dry-run");

        assertEquals(0, result.exitCode, "checkpoints dry run exit code");
        assertContains(result.stdout, "\"operation\":\"get_checkpoints\"", "checkpoints operation");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/checkpoints", "checkpoints endpoint");
    }

    private static void testStopWithSavepointDryRunUsesSavepointDirectory() {
        CliResult result = run(
                "stop_job",
                "--job-manager-url", "http://jm:8081",
                "--job-id", "job-1",
                "--stop-mode", "savepoint",
                "--savepoint-dir", "s3://bucket/flink/savepoints",
                "--confirm",
                "--dry-run");

        assertEquals(0, result.exitCode, "stop savepoint dry run exit code");
        assertContains(result.stdout, "\"success\":true", "stop savepoint success flag");
        assertContains(result.stdout, "http://jm:8081/jobs/job-1/stop", "stop savepoint endpoint");
        assertContains(result.stdout, "s3://bucket/flink/savepoints", "stop savepoint directory value");
        assertContains(result.stdout, "\\\"drain\\\":false", "stop savepoint drain default");
    }

    private static CliResult run(String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        int exitCode;
        try {
            System.setOut(new PrintStream(out, true, "UTF-8"));
            exitCode = FlinkOpsCli.run(args);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            System.setOut(originalOut);
        }
        return new CliResult(exitCode, new String(out.toByteArray(), StandardCharsets.UTF_8));
    }

    private static CliResult runCli(FlinkOpsCli cli, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        int exitCode;
        try {
            System.setOut(new PrintStream(out, true, "UTF-8"));
            exitCode = cli.runParsed(args);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } finally {
            System.setOut(originalOut);
        }
        return new CliResult(exitCode, new String(out.toByteArray(), StandardCharsets.UTF_8));
    }

    private static void assertEquals(int expected, int actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertEquals(boolean expected, boolean actual, String label) {
        if (expected != actual) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertEquals(String expected, String actual, String label) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertContains(String text, String expected, String label) {
        if (!text.contains(expected)) {
            throw new AssertionError(label + ": expected to find [" + expected + "] in [" + text + "]");
        }
    }

    private static void assertDoesNotContain(String text, String unexpected, String label) {
        if (text.contains(unexpected)) {
            throw new AssertionError(label + ": did not expect to find [" + unexpected + "] in [" + text + "]");
        }
    }

    private static final class CliResult {
        private final int exitCode;
        private final String stdout;

        private CliResult(int exitCode, String stdout) {
            this.exitCode = exitCode;
            this.stdout = stdout;
        }
    }

    private static final class RecordingConfigApplier extends FlinkConfigApplier {
        final Map<String, Object> values = new LinkedHashMap<String, Object>();

        RecordingConfigApplier() {
            super(null);
        }

        @Override
        public void set(OptionRef option, Object value) {
            values.put(option.key(), value);
        }

        @Override
        public void setString(String key, String value) {
            values.put(key, value);
        }
    }

    private static class FakeKubernetesGateway implements KubernetesGateway {
        boolean namespaceExists = true;
        boolean serviceAccountExists = true;
        boolean ingressControllerReady = true;
        boolean ingressClassExists = true;
        boolean ingressUpserted = false;

        public Map<String, Object> credentialStatus(String kubeconfigPath) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("source", kubeconfigPath == null || kubeconfigPath.trim().isEmpty() ? "default" : "explicit");
            data.put("path", kubeconfigPath == null || kubeconfigPath.trim().isEmpty()
                    ? System.getProperty("user.home") + "/.kube/config"
                    : kubeconfigPath);
            data.put("apiReachable", Boolean.TRUE);
            return data;
        }

        public boolean namespaceExists(String namespace) {
            return namespaceExists;
        }

        public boolean serviceAccountExists(String namespace, String serviceAccount) {
            return serviceAccountExists;
        }

        public boolean ingressControllerReady(String namespace, String ingressClassName) {
            return ingressControllerReady;
        }

        public Map<String, Object> ingressControllerService(String namespace, String ingressClassName) {
            Map<String, Object> service = new LinkedHashMap<String, Object>();
            service.put("name", ingressClassName + "-controller");
            service.put("namespace", namespace);
            service.put("found", Boolean.TRUE);
            service.put("type", "NodePort");

            java.util.List<Map<String, Object>> ports = new java.util.ArrayList<Map<String, Object>>();
            Map<String, Object> http = new LinkedHashMap<String, Object>();
            http.put("name", "http");
            http.put("port", Integer.valueOf(80));
            http.put("targetPort", "http");
            http.put("nodePort", Integer.valueOf(30080));
            http.put("protocol", "TCP");
            ports.add(http);

            Map<String, Object> https = new LinkedHashMap<String, Object>();
            https.put("name", "https");
            https.put("port", Integer.valueOf(443));
            https.put("targetPort", "https");
            https.put("nodePort", Integer.valueOf(30443));
            https.put("protocol", "TCP");
            ports.add(https);

            service.put("ports", ports);
            return service;
        }

        public java.util.List<Map<String, Object>> nodeAddresses() {
            java.util.List<Map<String, Object>> nodes = new java.util.ArrayList<Map<String, Object>>();
            Map<String, Object> node = new LinkedHashMap<String, Object>();
            node.put("name", "node-a");
            node.put("internalIP", "10.0.0.11");
            node.put("externalIP", "203.0.113.11");
            node.put("hostname", "node-a.local");
            nodes.add(node);
            return nodes;
        }

        public boolean ingressClassExists(String ingressClassName) {
            return ingressClassExists;
        }

        public Map<String, Object> upsertIngress(KubernetesIngressSpec spec) {
            ingressUpserted = true;
            return spec.plan();
        }
    }

    private static final class RecordingFlinkRestClient extends FlinkRestClient {
        final Map<String, String> responses = new LinkedHashMap<String, String>();
        String patchResponse = "{}";
        String lastHostHeader;
        int getCount;

        public String get(String endpoint, String hostHeader) {
            getCount++;
            this.lastHostHeader = hostHeader;
            if (responses.containsKey(endpoint)) {
                return responses.get(endpoint);
            }
            return "{\"jobs\":[]}";
        }

        public String patch(String endpoint, String hostHeader) {
            this.lastHostHeader = hostHeader;
            return patchResponse;
        }

        public String postJson(String endpoint, String body, String hostHeader) {
            this.lastHostHeader = hostHeader;
            return "{\"request\":\"accepted\"}";
        }
    }
}
