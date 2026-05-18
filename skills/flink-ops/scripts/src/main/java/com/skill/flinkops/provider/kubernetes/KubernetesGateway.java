package com.skill.flinkops.provider.kubernetes;

import java.util.Map;

public interface KubernetesGateway {
    Map<String, Object> credentialStatus(String kubeconfigPath) throws Exception;

    boolean namespaceExists(String namespace) throws Exception;

    boolean serviceAccountExists(String namespace, String serviceAccount) throws Exception;

    boolean ingressControllerReady(String namespace, String ingressClassName) throws Exception;

    Map<String, Object> ingressControllerService(String namespace, String ingressClassName) throws Exception;

    java.util.List<Map<String, Object>> nodeAddresses() throws Exception;

    boolean ingressClassExists(String ingressClassName) throws Exception;

    Map<String, Object> upsertIngress(KubernetesIngressSpec spec) throws Exception;
}
