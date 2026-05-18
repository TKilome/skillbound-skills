package com.skill.flinkops.provider.kubernetes;

import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.errors.ValidationException;
import com.skill.flinkops.flink.compat.FlinkCompatibility;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;
import com.skill.flinkops.flink.compat.submit.SubmitSpec;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FlinkKubernetesApplicationSpec {
    public final String name;
    public final String namespace;
    public final String serviceAccount;
    public final String flinkImage;
    public final String jarUri;
    public final String mainClass;
    public final String[] args;
    public final int parallelism;
    public final boolean enableIngress;
    public final String flinkVersion;
    public final String kubeconfigPath;
    public final Map<String, Object> flinkConfig;

    private FlinkKubernetesApplicationSpec(
            String name,
            String namespace,
            String serviceAccount,
            String flinkImage,
            String jarUri,
            String mainClass,
            String[] args,
            int parallelism,
            boolean enableIngress,
            String flinkVersion,
            String kubeconfigPath,
            Map<String, Object> flinkConfig) {
        this.name = name;
        this.namespace = namespace;
        this.serviceAccount = serviceAccount;
        this.flinkImage = flinkImage;
        this.jarUri = jarUri;
        this.mainClass = mainClass;
        this.args = args;
        this.parallelism = parallelism;
        this.enableIngress = enableIngress;
        this.flinkVersion = flinkVersion;
        this.kubeconfigPath = kubeconfigPath;
        this.flinkConfig = flinkConfig;
    }

    public static FlinkKubernetesApplicationSpec from(CommandContext context) {
        String name = context.require("name");
        String namespace = context.require("namespace");
        String serviceAccount = context.require("service-account");
        String flinkImage = context.require("flink-image");
        String jarUri = context.require("jar-uri");
        String mainClass = context.option("main-class");
        String[] args = splitArgs(context.option("args"));
        int parallelism = requirePositiveInt(context, "parallelism");
        boolean enableIngress = context.booleanOption("enable-ingress", true);
        String flinkVersion = firstPresent(context.option("flink-version"), "unknown");
        String kubeconfigPath = context.option("kubeconfig-path");
        validateJarUri(jarUri);

        Map<String, Object> conf = new LinkedHashMap<String, Object>();
        conf.put("pipeline.name", name);
        conf.put("execution.target", "kubernetes-application");
        conf.put("kubernetes.cluster-id", name);
        conf.put("kubernetes.namespace", namespace);
        conf.put("kubernetes.service-account", serviceAccount);
        conf.put("kubernetes.container.image.ref", flinkImage);
        conf.put("pipeline.jars", Arrays.asList(jarUri));
        conf.put("parallelism.default", Integer.valueOf(parallelism));
        if (mainClass != null && !mainClass.trim().isEmpty()) {
            conf.put("application.main.class", mainClass);
        }
        if (args.length > 0) {
            conf.put("application.args", Arrays.asList(args));
        }
        putIfPresent(conf, "jobmanager.memory.process.size", context.option("jobmanager-memory"));
        putIfPresent(conf, "taskmanager.memory.process.size", context.option("taskmanager-memory"));
        putIfPresent(conf, "taskmanager.numberOfTaskSlots", context.option("taskmanager-slots"));
        putIfPresent(conf, "kubernetes.config.file", kubeconfigPath);

        String dynamicProperties = context.option("dynamic-properties");
        if (dynamicProperties != null && !dynamicProperties.trim().isEmpty()) {
            String[] entries = dynamicProperties.split(",");
            for (int i = 0; i < entries.length; i++) {
                String entry = entries[i].trim();
                if (!entry.isEmpty()) {
                    int pos = entry.indexOf('=');
                    if (pos <= 0) {
                        throw new ValidationException("Dynamic property must use key=value: " + entry);
                    }
                    conf.put(entry.substring(0, pos), entry.substring(pos + 1));
                }
            }
        }

        return new FlinkKubernetesApplicationSpec(
                name, namespace, serviceAccount, flinkImage, jarUri, mainClass, args, parallelism, enableIngress, flinkVersion, kubeconfigPath, conf);
    }

    public Map<String, Object> dryRunPlan() {
        Map<String, Object> plan = new LinkedHashMap<String, Object>();
        FlinkCompatibility compatibility = FlinkCompatibilityRegistry.resolve(flinkVersion);
        plan.put("deploymentEngine", "flink-java-client");
        plan.put("deploymentTarget", "kubernetes-application");
        plan.put("submitDialect", compatibility.submitDialect().getClass().getSimpleName());
        plan.put("clientFactory", "org.apache.flink.kubernetes.KubernetesClusterClientFactory");
        plan.put("clusterDescriptor", "org.apache.flink.kubernetes.KubernetesClusterDescriptor");
        plan.put("deployMethod", "deployApplicationCluster");
        plan.put("applicationConfiguration", "ApplicationConfiguration.fromConfiguration(flinkConfig)");
        plan.put("flinkConfig", flinkConfig);
        plan.put("enableIngress", Boolean.valueOf(enableIngress));
        return plan;
    }

    public SubmitSpec toSubmitSpec() {
        return new SubmitSpec(
                name,
                "kubernetes-application",
                jarUri,
                mainClass,
                args,
                parallelism,
                value(flinkConfig.get("execution.savepoint.path")),
                flinkConfig);
    }

    private static void putIfPresent(Map<String, Object> conf, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            conf.put(key, value);
        }
    }

    private static int requirePositiveInt(CommandContext context, String name) {
        String value = context.require(name);
        final int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ValidationException("Parameter '--" + name + "' must be an integer.");
        }
        if (parsed < 1) {
            throw new ValidationException("Parameter '--" + name + "' must be a positive integer.");
        }
        return parsed;
    }

    private static void validateJarUri(String jarUri) {
        String value = jarUri.trim();
        int schemeSeparator = value.indexOf("://");
        if (schemeSeparator <= 0 || schemeSeparator + 3 >= value.length()) {
            throw new ValidationException(
                    "Parameter '--jar-uri' must be a URI visible inside the Flink container, such as local:///opt/flink/usrlib/orders.jar.");
        }
    }

    private static String firstPresent(String value, String fallback) {
        return value == null || value.trim().isEmpty() ? fallback : value.trim();
    }

    private String value(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static String[] splitArgs(String args) {
        if (args == null || args.trim().isEmpty()) {
            return new String[0];
        }
        return args.trim().split("\\s+");
    }
}
