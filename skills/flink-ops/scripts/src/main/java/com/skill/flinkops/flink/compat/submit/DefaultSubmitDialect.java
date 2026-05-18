package com.skill.flinkops.flink.compat.submit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class DefaultSubmitDialect implements SubmitDialect {
    public void applyCommonOptions(FlinkConfigApplier config, SubmitSpec spec) throws Exception {
        config.set(pipelineNameOption(), spec.name);
        config.set(deploymentTargetOption(), spec.deploymentTarget);
        config.set(pipelineJarsOption(), Collections.singletonList(spec.jarUri));
        config.set(parallelismOption(), Integer.valueOf(spec.parallelism));
        if (spec.mainClass != null && !spec.mainClass.trim().isEmpty()) {
            config.set(applicationMainClassOption(), spec.mainClass);
        }
        if (spec.args.length > 0) {
            config.set(applicationArgsOption(), Arrays.asList(spec.args));
        }
        if (spec.savepointPath != null && !spec.savepointPath.trim().isEmpty()) {
            config.set(savepointPathOption(), spec.savepointPath);
        }
        for (Map.Entry<String, Object> entry : spec.dynamicProperties.entrySet()) {
            if (entry.getValue() != null && !isCommonOption(entry.getKey())) {
                config.setString(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
    }

    public boolean supportsStopWithDrain() {
        return true;
    }

    protected OptionRef pipelineNameOption() {
        return OptionRef.field("pipeline.name", "org.apache.flink.configuration.PipelineOptions", "NAME");
    }

    protected OptionRef deploymentTargetOption() {
        return OptionRef.field("execution.target", "org.apache.flink.configuration.DeploymentOptions", "TARGET");
    }

    protected OptionRef pipelineJarsOption() {
        return OptionRef.field("pipeline.jars", "org.apache.flink.configuration.PipelineOptions", "JARS");
    }

    protected OptionRef parallelismOption() {
        return OptionRef.field("parallelism.default", "org.apache.flink.configuration.CoreOptions", "DEFAULT_PARALLELISM");
    }

    protected OptionRef applicationMainClassOption() {
        return OptionRef.field("application.main.class", "org.apache.flink.client.deployment.application.ApplicationConfiguration", "APPLICATION_MAIN_CLASS");
    }

    protected OptionRef applicationArgsOption() {
        return OptionRef.field("application.args", "org.apache.flink.client.deployment.application.ApplicationConfiguration", "APPLICATION_ARGS");
    }

    protected OptionRef savepointPathOption() {
        return OptionRef.key("execution.savepoint.path");
    }

    private boolean isCommonOption(String key) {
        return "pipeline.name".equals(key)
                || "execution.target".equals(key)
                || "pipeline.jars".equals(key)
                || "parallelism.default".equals(key)
                || "application.main.class".equals(key)
                || "application.args".equals(key)
                || "execution.savepoint.path".equals(key)
                || "jobmanager.memory.process.size".equals(key)
                || "taskmanager.memory.process.size".equals(key)
                || "taskmanager.numberOfTaskSlots".equals(key);
    }
}
