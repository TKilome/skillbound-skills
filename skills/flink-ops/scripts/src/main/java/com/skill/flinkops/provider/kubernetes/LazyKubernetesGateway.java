package com.skill.flinkops.provider.kubernetes;

import java.util.Map;

public final class LazyKubernetesGateway implements KubernetesGateway {
    private KubernetesGateway delegate;

    public Map<String, Object> credentialStatus(String kubeconfigPath) throws Exception {
        return delegate().credentialStatus(kubeconfigPath);
    }

    public boolean namespaceExists(String namespace) throws Exception {
        return delegate().namespaceExists(namespace);
    }

    public boolean serviceAccountExists(String namespace, String serviceAccount) throws Exception {
        return delegate().serviceAccountExists(namespace, serviceAccount);
    }

    public boolean ingressControllerReady(String namespace, String ingressClassName) throws Exception {
        return delegate().ingressControllerReady(namespace, ingressClassName);
    }

    public Map<String, Object> ingressControllerService(String namespace, String ingressClassName) throws Exception {
        return delegate().ingressControllerService(namespace, ingressClassName);
    }

    public java.util.List<Map<String, Object>> nodeAddresses() throws Exception {
        return delegate().nodeAddresses();
    }

    public boolean ingressClassExists(String ingressClassName) throws Exception {
        return delegate().ingressClassExists(ingressClassName);
    }

    public Map<String, Object> upsertIngress(KubernetesIngressSpec spec) throws Exception {
        return delegate().upsertIngress(spec);
    }

    private KubernetesGateway delegate() {
        if (delegate == null) {
            delegate = new JavaKubernetesGateway();
        }
        return delegate;
    }
}
