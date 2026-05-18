package com.skill.flinkops.provider.kubernetes;

import com.skill.flinkops.common.errors.ValidationException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class KubernetesIngressSpec {
    public static final int REST_PORT = 8081;
    public static final String BASE_DOMAIN = "flink.k8s.com";

    public final String namespace;
    public final String deploymentName;
    public final String ingressName;
    public final String serviceName;
    public final String ingressClassName;
    public final String host;
    public final String url;

    private KubernetesIngressSpec(String namespace, String deploymentName) {
        this.namespace = namespace;
        this.deploymentName = deploymentName;
        this.ingressName = deploymentName + "-flink-rest";
        this.serviceName = deploymentName + "-rest";
        this.ingressClassName = namespace + "-nginx";
        this.host = deploymentName + "." + BASE_DOMAIN;
        this.url = "https://" + host;
    }

    public static KubernetesIngressSpec from(String namespace, String deploymentName) {
        if (namespace == null || namespace.trim().isEmpty()) {
            throw new ValidationException("Parameter '--namespace' is required.");
        }
        if (deploymentName == null || deploymentName.trim().isEmpty()) {
            throw new ValidationException("Parameter '--name' is required.");
        }
        return new KubernetesIngressSpec(namespace, deploymentName);
    }

    public Map<String, Object> plan() {
        Map<String, Object> plan = new LinkedHashMap<String, Object>();
        plan.put("apiVersion", "networking.k8s.io/v1");
        plan.put("kind", "Ingress");
        plan.put("name", ingressName);
        plan.put("namespace", namespace);
        plan.put("ingressClassName", ingressClassName);
        plan.put("host", host);
        plan.put("url", url);
        plan.put("path", "/");
        plan.put("pathType", "Prefix");
        plan.put("serviceName", serviceName);
        plan.put("servicePort", Integer.valueOf(REST_PORT));
        return plan;
    }
}
