package com.skill.flinkops.flink.compat;

import com.skill.flinkops.flink.compat.rest.RestDialect;
import com.skill.flinkops.flink.compat.submit.SubmitDialect;

public final class FlinkCompatibility {
    private final FlinkVersion version;
    private final SubmitDialect submitDialect;
    private final RestDialect restDialect;

    FlinkCompatibility(FlinkVersion version, SubmitDialect submitDialect, RestDialect restDialect) {
        this.version = version;
        this.submitDialect = submitDialect;
        this.restDialect = restDialect;
    }

    public FlinkVersion version() {
        return version;
    }

    public SubmitDialect submitDialect() {
        return submitDialect;
    }

    public RestDialect restDialect() {
        return restDialect;
    }
}
