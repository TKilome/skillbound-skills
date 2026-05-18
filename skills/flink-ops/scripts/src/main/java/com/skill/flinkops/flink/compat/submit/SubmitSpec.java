package com.skill.flinkops.flink.compat.submit;

import java.util.LinkedHashMap;
import java.util.Map;

public final class SubmitSpec {
    public final String name;
    public final String deploymentTarget;
    public final String jarUri;
    public final String mainClass;
    public final String[] args;
    public final int parallelism;
    public final String savepointPath;
    public final Map<String, Object> dynamicProperties;

    public SubmitSpec(String name, String deploymentTarget, String jarUri, String mainClass, String[] args,
                      int parallelism, String savepointPath, Map<String, Object> dynamicProperties) {
        this.name = name;
        this.deploymentTarget = deploymentTarget;
        this.jarUri = jarUri;
        this.mainClass = mainClass;
        this.args = args == null ? new String[0] : args;
        this.parallelism = parallelism;
        this.savepointPath = savepointPath;
        this.dynamicProperties = dynamicProperties == null
                ? new LinkedHashMap<String, Object>()
                : new LinkedHashMap<String, Object>(dynamicProperties);
    }
}
