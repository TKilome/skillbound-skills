package com.skill.flinkops.flink.rest;

import com.skill.flinkops.FlinkVersionAdapter;
import com.skill.flinkops.FlinkVersionAdapters;
import com.skill.flinkops.common.CommandContext;
import com.skill.flinkops.common.errors.ValidationException;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlinkRuntimeInspector {
    public Map<String, Object> inspect(CommandContext context) {
        String target = context.option("deployment-target");
        if (target == null || target.trim().isEmpty()) {
            target = "common";
        }
        if (!"common".equals(target) && !"kubernetes".equals(target) && !"yarn".equals(target)) {
            throw new ValidationException("Parameter '--deployment-target' must be 'kubernetes', 'yarn', or omitted.");
        }

        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("deploymentTarget", target);
        FlinkVersionAdapter adapter = FlinkVersionAdapters.resolve(context);
        data.put("flinkVersion", adapter.flinkVersion);
        data.put("flinkMajorVersion", adapter.majorVersion);
        data.put("checks", checks(context, target));
        data.put("toolBoundary", "Only FlinkOpsCli is allowed; external CLIs are not used by this runtime inspection.");
        return data;
    }

    private List<Map<String, Object>> checks(CommandContext context, String target) {
        List<Map<String, Object>> checks = new ArrayList<Map<String, Object>>();
        checks.add(check("skillCli", true, "FlinkOpsCli command dispatcher is available."));
        checks.add(check("javaRuntime", true, System.getProperty("java.version")));

        String flinkHome = context.option("flink-home");
        if (flinkHome == null || flinkHome.trim().isEmpty()) {
            checks.add(check("flinkRuntimeClasspath", false, "Provide --flink-home for provider runtime classpath checks."));
        } else {
            File libDir = new File(flinkHome, "lib");
            checks.add(check("flinkRuntimeClasspath", libDir.isDirectory(), libDir.getAbsolutePath()));
        }

        if ("kubernetes".equals(target)) {
            checks.add(classCheck("flinkConfiguration", "org.apache.flink.configuration.Configuration", flinkHome));
            checks.add(classCheck("kubernetesJavaClient", "org.apache.flink.kubernetes.KubernetesClusterClientFactory", flinkHome));
            checks.add(classCheck("flinkApplicationConfiguration", "org.apache.flink.client.deployment.application.ApplicationConfiguration", flinkHome));
            checks.add(check("dockerImageBuild", true, "Use k8s_build_image for local jar packaging; do not run docker directly."));
            checks.add(check("kubernetesApiAccess", false, "Checked by Kubernetes readiness commands, not by runtime inspection."));
        }
        if ("yarn".equals(target)) {
            checks.add(classCheck("flinkConfiguration", "org.apache.flink.configuration.Configuration", flinkHome));
            checks.add(classCheck("yarnJavaClient", "org.apache.flink.yarn.YarnClusterClientFactory", flinkHome));
            checks.add(classCheck("flinkApplicationConfiguration", "org.apache.flink.client.deployment.application.ApplicationConfiguration", flinkHome));
            checks.add(classCheck("hadoopUgi", "org.apache.hadoop.security.UserGroupInformation", flinkHome));
            checks.add(check("yarnSubmissionMode", true, "Uses Flink Java client application mode; external yarn and flink CLIs are not used."));
        }

        return checks;
    }

    private Map<String, Object> classCheck(String name, String className, String flinkHome) {
        try {
            ClassLoader loader = flinkHomeClassLoader(flinkHome);
            Class.forName(className, false, loader);
            return check(name, true, className);
        } catch (ClassNotFoundException e) {
            return check(name, false, "Missing class on runtime classpath: " + className);
        } catch (Exception e) {
            return check(name, false, "Unable to inspect Flink runtime classpath: " + e.getMessage());
        }
    }

    private ClassLoader flinkHomeClassLoader(String flinkHome) throws Exception {
        if (flinkHome == null || flinkHome.trim().isEmpty()) {
            return Thread.currentThread().getContextClassLoader();
        }
        File libDir = new File(flinkHome, "lib");
        File[] jars = libDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jars == null || jars.length == 0) {
            return Thread.currentThread().getContextClassLoader();
        }
        URL[] urls = new URL[jars.length];
        for (int i = 0; i < jars.length; i++) {
            urls[i] = jars[i].toURI().toURL();
        }
        return new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
    }

    private Map<String, Object> check(String name, boolean ok, String message) {
        Map<String, Object> check = new LinkedHashMap<String, Object>();
        check.put("name", name);
        check.put("ok", Boolean.valueOf(ok));
        check.put("message", message);
        return check;
    }
}
