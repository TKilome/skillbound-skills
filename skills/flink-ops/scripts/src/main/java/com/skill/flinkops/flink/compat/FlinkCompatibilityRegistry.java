package com.skill.flinkops.flink.compat;

import com.skill.flinkops.flink.compat.rest.DefaultRestDialect;
import com.skill.flinkops.flink.compat.submit.DefaultSubmitDialect;
import com.skill.flinkops.flink.compat.submit.SubmitDialect;
import com.skill.flinkops.flink.compat.submit.overrides.Flink114SubmitDialect;

public final class FlinkCompatibilityRegistry {
    private FlinkCompatibilityRegistry() {
    }

    public static FlinkCompatibility resolve(String versionValue) {
        FlinkVersion version = FlinkVersion.parse(versionValue);
        SubmitDialect submitDialect = "1.14".equals(version.majorMinor())
                ? new Flink114SubmitDialect()
                : new DefaultSubmitDialect();
        return new FlinkCompatibility(version, submitDialect, new DefaultRestDialect());
    }
}
