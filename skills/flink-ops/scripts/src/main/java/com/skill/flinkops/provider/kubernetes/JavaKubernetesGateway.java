package com.skill.flinkops.provider.kubernetes;

import com.skill.flinkops.common.errors.OperationException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1HTTPIngressPath;
import io.kubernetes.client.openapi.models.V1HTTPIngressRuleValue;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1IngressBackend;
import io.kubernetes.client.openapi.models.V1IngressRule;
import io.kubernetes.client.openapi.models.V1IngressServiceBackend;
import io.kubernetes.client.openapi.models.V1IngressSpec;
import io.kubernetes.client.openapi.models.V1NodeAddress;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBackendPort;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.util.Config;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JavaKubernetesGateway implements KubernetesGateway {
    private CoreV1Api core;
    private AppsV1Api apps;
    private NetworkingV1Api networking;
    private String loadedKubeconfigPath;

    public Map<String, Object> credentialStatus(String kubeconfigPath) throws Exception {
        ensureClient(kubeconfigPath);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("source", kubeconfigPath == null || kubeconfigPath.trim().isEmpty() ? "default" : "explicit");
        data.put("path", effectiveKubeconfigPath(kubeconfigPath));
        data.put("apiReachable", Boolean.valueOf(apiReachable()));
        return data;
    }

    public boolean namespaceExists(String namespace) throws Exception {
        try {
            core().readNamespace(namespace, null);
            return true;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    public boolean serviceAccountExists(String namespace, String serviceAccount) throws Exception {
        try {
            core().readNamespacedServiceAccount(serviceAccount, namespace, null);
            return true;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    public boolean ingressControllerReady(String namespace, String ingressClassName) throws Exception {
        try {
            return apps().listNamespacedDeployment(
                            namespace,
                            null,
                            null,
                            null,
                            null,
                            "app.kubernetes.io/name=ingress-nginx",
                            null,
                            null,
                            null,
                            null,
                            null,
                            null)
                    .getItems()
                    .stream()
                    .anyMatch(deployment -> deployment.getStatus() != null
                            && deployment.getStatus().getReadyReplicas() != null
                            && deployment.getStatus().getReadyReplicas().intValue() > 0);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    public boolean ingressClassExists(String ingressClassName) throws Exception {
        try {
            networking().readIngressClass(ingressClassName, null);
            return true;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    public Map<String, Object> ingressControllerService(String namespace, String ingressClassName) throws Exception {
        String serviceName = ingressClassName + "-controller";
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("name", serviceName);
        data.put("namespace", namespace);
        try {
            V1Service service = core().readNamespacedService(serviceName, namespace, null);
            data.put("found", Boolean.TRUE);
            if (service.getSpec() != null) {
                data.put("type", service.getSpec().getType());
                data.put("ports", servicePorts(service.getSpec().getPorts()));
            }
            return data;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                data.put("found", Boolean.FALSE);
                data.put("message", "Ingress Controller Service '" + serviceName + "' was not found.");
                return data;
            }
            throw e;
        }
    }

    public List<Map<String, Object>> nodeAddresses() throws Exception {
        java.util.ArrayList<Map<String, Object>> result = new java.util.ArrayList<Map<String, Object>>();
        core().listNode(null, null, null, null, null, null, null, null, null, null, null)
                .getItems()
                .forEach(node -> {
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("name", node.getMetadata() == null ? "" : node.getMetadata().getName());
                    item.put("internalIP", "");
                    item.put("externalIP", "");
                    item.put("hostname", "");
                    if (node.getStatus() != null && node.getStatus().getAddresses() != null) {
                        for (V1NodeAddress address : node.getStatus().getAddresses()) {
                            if ("InternalIP".equals(address.getType())) {
                                item.put("internalIP", address.getAddress());
                            } else if ("ExternalIP".equals(address.getType())) {
                                item.put("externalIP", address.getAddress());
                            } else if ("Hostname".equals(address.getType())) {
                                item.put("hostname", address.getAddress());
                            }
                        }
                    }
                    result.add(item);
                });
        return result;
    }

    private List<Map<String, Object>> servicePorts(List<V1ServicePort> ports) {
        java.util.ArrayList<Map<String, Object>> result = new java.util.ArrayList<Map<String, Object>>();
        if (ports == null) {
            return result;
        }
        for (V1ServicePort port : ports) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("name", port.getName());
            item.put("port", port.getPort());
            item.put("targetPort", port.getTargetPort() == null ? null : String.valueOf(port.getTargetPort()));
            item.put("nodePort", port.getNodePort());
            item.put("protocol", port.getProtocol());
            result.add(item);
        }
        return result;
    }

    public Map<String, Object> upsertIngress(KubernetesIngressSpec spec) throws Exception {
        V1Ingress ingress = ingress(spec);
        try {
            networking().readNamespacedIngress(spec.ingressName, spec.namespace, null);
            networking().replaceNamespacedIngress(spec.ingressName, spec.namespace, ingress, null, null, null, null);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                networking().createNamespacedIngress(spec.namespace, ingress, null, null, null, null);
            } else {
                throw ingressCreationFailed(spec, e);
            }
        } catch (Exception e) {
            throw new OperationException("IngressCreationFailed",
                    "Failed to create or update Ingress '" + spec.ingressName + "': " + e.getMessage());
        }

        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.putAll(spec.plan());
        data.put("status", "applied");
        return data;
    }

    private V1Ingress ingress(KubernetesIngressSpec spec) {
        V1ServiceBackendPort port = new V1ServiceBackendPort().number(Integer.valueOf(KubernetesIngressSpec.REST_PORT));
        V1IngressServiceBackend service = new V1IngressServiceBackend().name(spec.serviceName).port(port);
        V1IngressBackend backend = new V1IngressBackend().service(service);
        V1HTTPIngressPath path = new V1HTTPIngressPath().path("/").pathType("Prefix").backend(backend);
        V1HTTPIngressRuleValue http = new V1HTTPIngressRuleValue().paths(Arrays.asList(path));
        V1IngressRule rule = new V1IngressRule().host(spec.host).http(http);
        return new V1Ingress()
                .metadata(new V1ObjectMeta().name(spec.ingressName).namespace(spec.namespace))
                .spec(new V1IngressSpec().ingressClassName(spec.ingressClassName).rules(Arrays.asList(rule)));
    }

    private CoreV1Api core() {
        ensureClient();
        return core;
    }

    private AppsV1Api apps() {
        ensureClient();
        return apps;
    }

    private NetworkingV1Api networking() {
        ensureClient();
        return networking;
    }

    private void ensureClient() {
        ensureClient(null);
    }

    private void ensureClient(String kubeconfigPath) {
        String path = effectiveKubeconfigPath(kubeconfigPath);
        if (core != null && path.equals(loadedKubeconfigPath)) {
            return;
        }
        try {
            File file = new File(path);
            if (!file.isFile()) {
                throw new OperationException("KubeconfigNotFound", "Kubeconfig file was not found at path '" + path + "'.");
            }
            FileReader reader = new FileReader(file);
            try {
                ApiClient client = Config.fromConfig(reader);
                this.core = new CoreV1Api(client);
                this.apps = new AppsV1Api(client);
                this.networking = new NetworkingV1Api(client);
                this.loadedKubeconfigPath = path;
            } finally {
                reader.close();
            }
        } catch (OperationException e) {
            throw e;
        } catch (Exception e) {
            throw new OperationException("KubernetesClientUnavailable",
                    "Unable to initialize Java Kubernetes Client from kubeconfig path '" + path + "'.");
        }
    }

    private String effectiveKubeconfigPath(String kubeconfigPath) {
        if (kubeconfigPath != null && !kubeconfigPath.trim().isEmpty()) {
            return kubeconfigPath;
        }
        if (loadedKubeconfigPath != null && !loadedKubeconfigPath.trim().isEmpty()) {
            return loadedKubeconfigPath;
        }
        return System.getProperty("user.home") + "/.kube/config";
    }

    private boolean apiReachable() throws Exception {
        try {
            core().readNamespace("default", null);
            return true;
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return true;
            }
            throw e;
        }
    }

    private OperationException ingressCreationFailed(KubernetesIngressSpec spec, ApiException e) {
        String body = e.getResponseBody();
        String detail = body == null || body.trim().isEmpty() ? e.getMessage() : body;
        return new OperationException("IngressCreationFailed",
                "Failed to create or update Ingress '" + spec.ingressName + "': " + detail);
    }
}
