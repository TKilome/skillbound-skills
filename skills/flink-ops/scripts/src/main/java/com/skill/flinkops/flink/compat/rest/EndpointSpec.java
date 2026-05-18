package com.skill.flinkops.flink.compat.rest;

public final class EndpointSpec {
    private final String method;
    private final String path;

    EndpointSpec(String method, String path) {
        this.method = method;
        this.path = path;
    }

    public String method() {
        return method;
    }

    public String path() {
        return path;
    }
}
