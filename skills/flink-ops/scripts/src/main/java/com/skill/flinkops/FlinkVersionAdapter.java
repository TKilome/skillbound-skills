package com.skill.flinkops;

import com.skill.flinkops.flink.compat.FlinkCompatibility;
import com.skill.flinkops.flink.compat.FlinkCompatibilityRegistry;

public final class FlinkVersionAdapter {
    public final String flinkVersion;
    public final String majorVersion;

    public FlinkVersionAdapter(String flinkVersion, String majorVersion) {
        this.flinkVersion = flinkVersion;
        this.majorVersion = majorVersion;
    }

    public String yarnApplicationTarget() {
        return "yarn-application";
    }

    public String yarnApplicationEntrypoint() {
        return "org.apache.flink.yarn.entrypoint.YarnApplicationClusterEntryPoint";
    }

    public String yarnClientFactoryClass() {
        return "org.apache.flink.yarn.YarnClusterClientFactory";
    }

    public FlinkCompatibility compatibility() {
        return FlinkCompatibilityRegistry.resolve(flinkVersion);
    }
}
