package com.skill.flinkops.provider.kubernetes;

import com.skill.flinkops.flink.compat.FlinkCompatibility;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;
import com.skill.flinkops.flink.compat.submit.FlinkConfigApplier;
import com.skill.flinkops.common.errors.OperationException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlinkNativeKubernetesDeployer {
    public Map<String, Object> deploy(FlinkKubernetesApplicationSpec spec) throws Exception {
        Class<?> configurationClass = Class.forName("org.apache.flink.configuration.Configuration");
        Class<?> configOptionClass = Class.forName("org.apache.flink.configuration.ConfigOption");
        Object flinkConfig = configurationClass.getConstructor().newInstance();
        Method setMethod = configurationClass.getMethod("set", configOptionClass, Object.class);

        FlinkCompatibility compatibility = FlinkCompatibilityRegistry.resolve(spec.flinkVersion);
        compatibility.submitDialect().applyCommonOptions(new FlinkConfigApplier(flinkConfig), spec.toSubmitSpec());
        setMethod.invoke(flinkConfig, option("org.apache.flink.kubernetes.configuration.KubernetesConfigOptions", "CLUSTER_ID"), spec.name);
        setMethod.invoke(flinkConfig, option("org.apache.flink.kubernetes.configuration.KubernetesConfigOptions", "NAMESPACE"), spec.namespace);
        setMethod.invoke(flinkConfig, option("org.apache.flink.kubernetes.configuration.KubernetesConfigOptions", "KUBERNETES_SERVICE_ACCOUNT"), spec.serviceAccount);
        setMethod.invoke(flinkConfig, option("org.apache.flink.kubernetes.configuration.KubernetesConfigOptions", "CONTAINER_IMAGE"), spec.flinkImage);
        applyTypedProviderConfig(setMethod, flinkConfig, spec);
        applyAdditionalStringConfig(configurationClass, flinkConfig, spec);
        validateWritableYamlConfiguration(flinkConfig);

        Object factory = Class.forName("org.apache.flink.kubernetes.KubernetesClusterClientFactory")
                .getConstructor()
                .newInstance();
        Method createDescriptor = factory.getClass().getMethod("createClusterDescriptor", configurationClass);
        Method getSpec = factory.getClass().getMethod("getClusterSpecification", configurationClass);
        Object descriptor = createDescriptor.invoke(factory, flinkConfig);
        Object clusterSpec = getSpec.invoke(factory, flinkConfig);

        Class<?> applicationConfigurationClass = Class.forName("org.apache.flink.client.deployment.application.ApplicationConfiguration");
        Method fromConfiguration = applicationConfigurationClass.getMethod("fromConfiguration", configurationClass);
        Object applicationConfiguration = fromConfiguration.invoke(null, flinkConfig);
        Method deploy = descriptor.getClass().getMethod("deployApplicationCluster", clusterSpec.getClass(), applicationConfigurationClass);
        Object provider = deploy.invoke(descriptor, clusterSpec, applicationConfiguration);
        Class<?> clusterClientProviderClass = Class.forName("org.apache.flink.client.program.ClusterClientProvider");
        Object client = clusterClientProviderClass.getMethod("getClusterClient").invoke(provider);

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("clusterId", String.valueOf(client.getClass().getMethod("getClusterId").invoke(client)));
        result.put("jobManagerUrl", String.valueOf(client.getClass().getMethod("getWebInterfaceURL").invoke(client)));
        closeQuietly(client);
        closeQuietly(descriptor);
        return result;
    }

    private Object option(String className, String fieldName) throws Exception {
        Class<?> clazz = Class.forName(className);
        Field field = clazz.getField(fieldName);
        return field.get(null);
    }

    private void applyAdditionalStringConfig(Class<?> configurationClass, Object flinkConfig, FlinkKubernetesApplicationSpec spec) throws Exception {
        Method setString = configurationClass.getMethod("setString", String.class, String.class);
        for (Map.Entry<String, Object> entry : spec.flinkConfig.entrySet()) {
            if (isTypedOption(entry.getKey())) {
                continue;
            }
            Object value = entry.getValue();
            if (value != null) {
                setString.invoke(flinkConfig, entry.getKey(), String.valueOf(value));
            }
        }
    }

    private void applyTypedProviderConfig(Method setMethod, Object flinkConfig, FlinkKubernetesApplicationSpec spec) throws Exception {
        setMemoryIfPresent(setMethod, flinkConfig, "org.apache.flink.configuration.JobManagerOptions", "TOTAL_PROCESS_MEMORY",
                spec.flinkConfig.get("jobmanager.memory.process.size"));
        setMemoryIfPresent(setMethod, flinkConfig, "org.apache.flink.configuration.TaskManagerOptions", "TOTAL_PROCESS_MEMORY",
                spec.flinkConfig.get("taskmanager.memory.process.size"));
        Object taskSlots = spec.flinkConfig.get("taskmanager.numberOfTaskSlots");
        if (taskSlots != null) {
            setMethod.invoke(flinkConfig,
                    option("org.apache.flink.configuration.TaskManagerOptions", "NUM_TASK_SLOTS"),
                    Integer.valueOf(String.valueOf(taskSlots)));
        }
    }

    private void setMemoryIfPresent(Method setMethod, Object flinkConfig, String optionClassName, String optionFieldName, Object value) throws Exception {
        if (value == null) {
            return;
        }
        Class<?> memorySizeClass = Class.forName("org.apache.flink.configuration.MemorySize");
        Method parse = memorySizeClass.getMethod("parse", String.class);
        setMethod.invoke(flinkConfig, option(optionClassName, optionFieldName), parse.invoke(null, String.valueOf(value)));
    }

    private boolean isTypedOption(String key) {
        return "pipeline.name".equals(key)
                || "execution.target".equals(key)
                || "kubernetes.cluster-id".equals(key)
                || "kubernetes.namespace".equals(key)
                || "kubernetes.service-account".equals(key)
                || "kubernetes.container.image".equals(key)
                || "kubernetes.container.image.ref".equals(key)
                || "pipeline.jars".equals(key)
                || "parallelism.default".equals(key)
                || "application.main.class".equals(key)
                || "application.args".equals(key)
                || "jobmanager.memory.process.size".equals(key)
                || "taskmanager.memory.process.size".equals(key)
                || "taskmanager.numberOfTaskSlots".equals(key);
    }

    private void validateWritableYamlConfiguration(Object flinkConfig) throws Exception {
        Method toMap = flinkConfig.getClass().getMethod("toMap");
        @SuppressWarnings("unchecked")
        Map<String, String> config = (Map<String, String>) toMap.invoke(flinkConfig);
        List<String> conflicts = detectPrefixConflicts(config);
        if (!conflicts.isEmpty()) {
            Map<String, Object> data = new LinkedHashMap<String, Object>();
            data.put("conflicts", conflicts);
            data.put("reason", "Flink 1.19 converts the client configuration to cluster-side YAML before creating Kubernetes resources. A flat key cannot also be the parent of another key.");
            throw new OperationException(
                    "FlinkConfigurationKeyConflict",
                    "Flink configuration contains prefix-conflicting keys and cannot be converted to cluster-side YAML.",
                    data);
        }
    }

    public static List<String> detectPrefixConflicts(Map<String, String> config) {
        List<String> keys = new ArrayList<String>(config.keySet());
        Collections.sort(keys);
        List<String> conflicts = new ArrayList<String>();
        for (int i = 0; i < keys.size(); i++) {
            String parent = keys.get(i);
            for (int j = i + 1; j < keys.size(); j++) {
                String child = keys.get(j);
                if (!child.startsWith(parent + ".")) {
                    break;
                }
                conflicts.add(parent + " conflicts with " + child);
            }
        }
        return conflicts;
    }

    private void closeQuietly(Object closeable) {
        try {
            closeable.getClass().getMethod("close").invoke(closeable);
        } catch (Exception ignored) {
            // best effort
        }
    }
}
