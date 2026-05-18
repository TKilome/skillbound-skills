package com.skill.flinkops.cli;

public final class CommandNames {
    public static final String START_JOB = "start_job";
    public static final String K8S_START_JOB = "k8s_start_job";
    public static final String BUILD_IMAGE = "build_image";
    public static final String K8S_BUILD_IMAGE = "k8s_build_image";
    public static final String PREFLIGHT_START = "preflight_start";
    public static final String K8S_PREFLIGHT_START = "k8s_preflight_start";
    public static final String K8S_CHECK_CONNECTIVITY = "k8s_check_connectivity";
    public static final String YARN_CHECK_CONNECTIVITY = "yarn_check_connectivity";
    public static final String CHECK_CLI_ENVIRONMENT = "check_cli_environment";
    public static final String CHECK_INGRESS_CONTROLLER = "check_ingress_controller";
    public static final String K8S_CHECK_INGRESS_CONTROLLER = "k8s_check_ingress_controller";
    public static final String GET_NODE_IPS = "get_node_ips";
    public static final String K8S_GET_NODE_IPS = "k8s_get_node_ips";
    public static final String INSPECT_CLUSTER = "inspect_cluster";
    public static final String RENDER_INGRESS_CONTROLLER_YAML = "render_ingress_controller_yaml";
    public static final String K8S_RENDER_INGRESS_CONTROLLER_YAML = "k8s_render_ingress_controller_yaml";
    public static final String STOP_JOB = "stop_job";
    public static final String GET_JOB_STATUS = "get_job_status";
    public static final String DIAGNOSE_JOB = "diagnose_job";
    public static final String GET_EXCEPTIONS = "get_exceptions";
    public static final String GET_CHECKPOINTS = "get_checkpoints";
    public static final String DIAGNOSE_BACKPRESSURE = "diagnose_backpressure";

    private CommandNames() {
    }

    public static boolean isKubernetesAlias(String command) {
        return K8S_START_JOB.equals(command)
                || K8S_BUILD_IMAGE.equals(command)
                || K8S_PREFLIGHT_START.equals(command)
                || K8S_CHECK_CONNECTIVITY.equals(command)
                || K8S_CHECK_INGRESS_CONTROLLER.equals(command)
                || K8S_GET_NODE_IPS.equals(command)
                || K8S_RENDER_INGRESS_CONTROLLER_YAML.equals(command);
    }
}
