package com.skill.flinkops.provider.yarn;

import com.skill.flinkops.common.errors.OperationException;
import com.skill.flinkops.flink.compat.FlinkCompatibility;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;
import com.skill.flinkops.flink.compat.submit.FlinkConfigApplier;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedHashMap;
import java.util.Map;

public final class FlinkYarnApplicationDeployer {
    public Map<String, Object> deploy(FlinkYarnApplicationSpec spec) throws Exception {
        Object result = doAsHadoopUser(spec.hadoopUser, new SubmitAction() {
            public Object run() throws Exception {
                return deployInternal(spec);
            }
        });
        @SuppressWarnings("unchecked")
        Map<String, Object> typed = (Map<String, Object>) result;
        return typed;
    }

    private Map<String, Object> deployInternal(FlinkYarnApplicationSpec spec) throws Exception {
        Class<?> configurationClass = Class.forName("org.apache.flink.configuration.Configuration");
        Class<?> configOptionClass = Class.forName("org.apache.flink.configuration.ConfigOption");
        Object flinkConfig = configurationClass.getConstructor().newInstance();
        Method setMethod = configurationClass.getMethod("set", configOptionClass, Object.class);

        FlinkCompatibility compatibility = FlinkCompatibilityRegistry.resolve(spec.versionAdapter.flinkVersion);
        compatibility.submitDialect().applyCommonOptions(new FlinkConfigApplier(flinkConfig), spec.toSubmitSpec());
        setIfOptionExists(setMethod, flinkConfig, "org.apache.flink.yarn.configuration.YarnConfigOptions", "APPLICATION_NAME", spec.name);
        setIfOptionExists(setMethod, flinkConfig, "org.apache.flink.yarn.configuration.YarnConfigOptions", "APPLICATION_TYPE", value(spec.flinkConfig.get("yarn.application.type")));
        setIfOptionExists(setMethod, flinkConfig, "org.apache.flink.yarn.configuration.YarnConfigOptions", "APPLICATION_TAGS", value(spec.flinkConfig.get("yarn.application.tags")));
        setIfOptionExists(setMethod, flinkConfig, "org.apache.flink.yarn.configuration.YarnConfigOptions", "APPLICATION_QUEUE", spec.yarnQueue);
        setIfOptionExists(setMethod, flinkConfig, "org.apache.flink.yarn.configuration.YarnConfigOptions", "PROVIDED_LIB_DIRS", spec.providedLibDirs);
        setIfOptionExists(setMethod, flinkConfig, "org.apache.flink.yarn.configuration.YarnConfigOptions", "FLINK_DIST_JAR", spec.flinkDistJar);
        applyAdditionalStringConfig(configurationClass, flinkConfig, spec);

        Object factory = Class.forName(spec.versionAdapter.yarnClientFactoryClass())
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

    private Object doAsHadoopUser(String hadoopUser, final SubmitAction action) throws Exception {
        if (hadoopUser == null || hadoopUser.trim().isEmpty()) {
            return action.run();
        }
        Class<?> ugiClass = Class.forName("org.apache.hadoop.security.UserGroupInformation");
        Object current = ugiClass.getMethod("getCurrentUser").invoke(null);
        String currentShortUser = String.valueOf(current.getClass().getMethod("getShortUserName").invoke(current));
        Object effective = current;
        if (!hadoopUser.trim().equals(currentShortUser)) {
            effective = ugiClass.getMethod("createProxyUser", String.class, ugiClass).invoke(null, hadoopUser.trim(), current);
        }
        try {
            return effective.getClass().getMethod("doAs", PrivilegedExceptionAction.class).invoke(
                    effective,
                    new PrivilegedExceptionAction<Object>() {
                        public Object run() throws Exception {
                            return action.run();
                        }
                    });
        } catch (InvocationTargetException e) {
            Throwable cause = e.getTargetException();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }

    private void setIfOptionExists(Method setMethod, Object flinkConfig, String className, String fieldName, Object value) throws Exception {
        if (value == null) {
            return;
        }
        try {
            Class<?> clazz = Class.forName(className);
            Field field = clazz.getField(fieldName);
            setMethod.invoke(flinkConfig, field.get(null), value);
        } catch (NoSuchFieldException ignored) {
            // Version adapters fall back to string keys for fields absent in a Flink minor.
        }
    }

    private void applyAdditionalStringConfig(Class<?> configurationClass, Object flinkConfig, FlinkYarnApplicationSpec spec) throws Exception {
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

    private boolean isTypedOption(String key) {
        return "yarn.provided.lib.dirs".equals(key)
                || "pipeline.jars".equals(key)
                || "application.args".equals(key);
    }

    private String value(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private void closeQuietly(Object closeable) {
        try {
            closeable.getClass().getMethod("close").invoke(closeable);
        } catch (Exception ignored) {
            // best effort
        }
    }

    private interface SubmitAction {
        Object run() throws Exception;
    }
}
