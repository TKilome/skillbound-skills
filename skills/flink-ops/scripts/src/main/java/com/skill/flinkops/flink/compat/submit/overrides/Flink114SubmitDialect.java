package com.skill.flinkops.flink.compat.submit.overrides;

import com.skill.flinkops.flink.compat.submit.DefaultSubmitDialect;

public final class Flink114SubmitDialect extends DefaultSubmitDialect {
    @Override
    public boolean supportsStopWithDrain() {
        return false;
    }
}
