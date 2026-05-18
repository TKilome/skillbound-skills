package com.skill.flinkops.flink.compat.rest;

public interface RestDialect {
    EndpointSpec jobStatus(String jobId);

    EndpointSpec jobExceptions(String jobId);

    EndpointSpec jobCheckpoints(String jobId);

    EndpointSpec triggerSavepoint(String jobId);
}
