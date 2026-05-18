package com.skill.flinkops.flink.compat.submit;

public interface SubmitDialect {
    void applyCommonOptions(FlinkConfigApplier config, SubmitSpec spec) throws Exception;

    boolean supportsStopWithDrain();
}
