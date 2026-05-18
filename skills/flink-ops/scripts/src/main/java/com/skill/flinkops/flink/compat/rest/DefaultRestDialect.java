package com.skill.flinkops.flink.compat.rest;

public class DefaultRestDialect implements RestDialect {
    public EndpointSpec jobStatus(String jobId) {
        return new EndpointSpec("GET", "/jobs/" + jobId);
    }

    public EndpointSpec jobExceptions(String jobId) {
        return new EndpointSpec("GET", "/jobs/" + jobId + "/exceptions");
    }

    public EndpointSpec jobCheckpoints(String jobId) {
        return new EndpointSpec("GET", "/jobs/" + jobId + "/checkpoints");
    }

    public EndpointSpec triggerSavepoint(String jobId) {
        return new EndpointSpec("POST", "/jobs/" + jobId + "/savepoints");
    }
}
