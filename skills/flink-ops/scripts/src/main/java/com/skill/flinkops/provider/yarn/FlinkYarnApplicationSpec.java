package com.skill.flinkops.provider.yarn;

import com.skill.flinkops.FlinkVersionAdapter;
import com.skill.flinkops.FlinkVersionAdapters;
import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.errors.ValidationException;
import com.skill.flinkops.flink.compat.FlinkCompatibility;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;
import com.skill.flinkops.flink.compat.submit.SubmitSpec;
import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlinkYarnApplicationSpec {
    public final String name;
    public final String flinkHome;
    public final String jarUri;
    public final String mainClass;
    public final String[] args;
    public final int parallelism;
    public final String yarnQueue;
    public final String hadoopUser;
    public final String flinkDistJar;
    public final List<String> providedLibDirs;
    public final FlinkVersionAdapter versionAdapter;
    public final Map<String, Object> flinkConfig;

    private FlinkYarnApplicationSpec(
            String name,
            String flinkHome,
            String jarUri,
            String mainClass,
            String[] args,
            int parallelism,
            String yarnQueue,
            String hadoopUser,
            String flinkDistJar,
            List<String> providedLibDirs,
            FlinkVersionAdapter versionAdapter,
            Map<String, Object> flinkConfig) {
        this.name = name;
        this.flinkHome = flinkHome;
        this.jarUri = jarUri;
        this.mainClass = mainClass;
        this.args = args;
        this.parallelism = parallelism;
        this.yarnQueue = yarnQueue;
        this.hadoopUser = hadoopUser;
        this.flinkDistJar = flinkDistJar;
        this.providedLibDirs = providedLibDirs;
        this.versionAdapter = versionAdapter;
        this.flinkConfig = flinkConfig;
    }

    public static FlinkYarnApplicationSpec from(CommandContext context) {
        String name = context.require("name");
        String flinkHome = context.require("flink-home");
        String jarUri = context.require("jar-uri");
        String mainClass = context.option("main-class");
        String[] args = splitArgs(context.option("args"));
        int parallelism = context.intOption("parallelism", 1);
        String yarnQueue = context.option("yarn-queue");
        String hadoopUser = context.option("hadoop-user");
        FlinkVersionAdapter adapter = FlinkVersionAdapters.resolve(context);
        String flinkDistJar = firstPresent(context.option("flink-dist-jar"), defaultFlinkDistJar(flinkHome));
        List<String> providedLibDirs = providedLibDirs(context, flinkHome);

        Map<String, Object> conf = new LinkedHashMap<String, Object>();
        conf.put("pipeline.name", name);
        conf.put("execution.target", adapter.yarnApplicationTarget());
        conf.put("yarn.application.name", name);
        conf.put("yarn.application.type", firstPresent(context.option("application-type"), "Apache Flink"));
        conf.put("yarn.application.tags", firstPresent(context.option("application-tags"), "flink-intelligent-ops"));
        conf.put("pipeline.jars", Arrays.asList(jarUri));
        conf.put("parallelism.default", Integer.valueOf(parallelism));
        conf.put("yarn.provided.lib.dirs", providedLibDirs);
        if (flinkDistJar != null && !flinkDistJar.trim().isEmpty()) {
            conf.put("yarn.flink-dist-jar", flinkDistJar);
        }
        putIfPresent(conf, "yarn.application.queue", yarnQueue);
        putIfPresent(conf, "application.main.class", mainClass);
        if (args.length > 0) {
            conf.put("application.args", Arrays.asList(args));
        }
        putIfPresent(conf, "jobmanager.memory.process.size", context.option("jobmanager-memory"));
        putIfPresent(conf, "taskmanager.memory.process.size", context.option("taskmanager-memory"));
        putIfPresent(conf, "taskmanager.numberOfTaskSlots", context.option("taskmanager-slots"));
        putIfPresent(conf, "execution.savepoint.path", context.option("savepoint-path"));
        applyDynamicProperties(conf, context.option("dynamic-properties"));

        return new FlinkYarnApplicationSpec(
                name,
                flinkHome,
                jarUri,
                mainClass,
                args,
                parallelism,
                yarnQueue,
                hadoopUser,
                flinkDistJar,
                providedLibDirs,
                adapter,
                conf);
    }

    public Map<String, Object> dryRunPlan() {
        Map<String, Object> plan = new LinkedHashMap<String, Object>();
        FlinkCompatibility compatibility = FlinkCompatibilityRegistry.resolve(versionAdapter.flinkVersion);
        plan.put("deploymentEngine", "flink-java-client");
        plan.put("deploymentTarget", versionAdapter.yarnApplicationTarget());
        plan.put("submitDialect", compatibility.submitDialect().getClass().getSimpleName());
        plan.put("clientFactory", versionAdapter.yarnClientFactoryClass());
        plan.put("clusterDescriptor", "org.apache.flink.yarn.YarnClusterDescriptor");
        plan.put("deployMethod", "deployApplicationCluster");
        plan.put("applicationConfiguration", "ApplicationConfiguration.fromConfiguration(flinkConfig)");
        plan.put("entrypoint", versionAdapter.yarnApplicationEntrypoint());
        plan.put("flinkHome", flinkHome);
        plan.put("flinkVersion", versionAdapter.flinkVersion);
        plan.put("flinkMajorVersion", versionAdapter.majorVersion);
        if (hadoopUser != null && !hadoopUser.trim().isEmpty()) {
            plan.put("hadoopUser", hadoopUser);
        }
        plan.put("flinkConfig", flinkConfig);
        return plan;
    }

    public SubmitSpec toSubmitSpec() {
        return new SubmitSpec(
                name,
                versionAdapter.yarnApplicationTarget(),
                jarUri,
                mainClass,
                args,
                parallelism,
                value(flinkConfig.get("execution.savepoint.path")),
                flinkConfig);
    }

    private static List<String> providedLibDirs(CommandContext context, String flinkHome) {
        String value = context.option("yarn-provided-lib-dirs");
        List<String> dirs = new ArrayList<String>();
        if (value != null && !value.trim().isEmpty()) {
            String[] parts = value.split(",");
            for (int i = 0; i < parts.length; i++) {
                String part = parts[i].trim();
                if (!part.isEmpty()) {
                    dirs.add(part);
                }
            }
        }
        if (dirs.isEmpty()) {
            dirs.add(new File(flinkHome, "lib").getAbsolutePath());
        }
        return dirs;
    }

    private static String defaultFlinkDistJar(String flinkHome) {
        File lib = new File(flinkHome, "lib");
        File[] jars = lib.listFiles((dir, name) -> name.startsWith("flink-dist") && name.endsWith(".jar"));
        if (jars == null || jars.length == 0) {
            return null;
        }
        return jars[0].getAbsolutePath();
    }

    private static void applyDynamicProperties(Map<String, Object> conf, String dynamicProperties) {
        if (dynamicProperties == null || dynamicProperties.trim().isEmpty()) {
            return;
        }
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

    private static void putIfPresent(Map<String, Object> conf, String key, String value) {
        if (value != null && !value.trim().isEmpty()) {
            conf.put(key, value);
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
