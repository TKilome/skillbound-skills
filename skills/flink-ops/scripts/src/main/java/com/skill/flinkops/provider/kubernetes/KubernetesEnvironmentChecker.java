package com.skill.flinkops.provider.kubernetes;

import com.skill.flinkops.common.errors.OperationException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class KubernetesEnvironmentChecker {
    private final KubernetesGateway gateway;
    private final KubernetesRemediation remediation;

    public KubernetesEnvironmentChecker(KubernetesGateway gateway) {
        this.gateway = gateway;
        this.remediation = new KubernetesRemediation();
    }

    public Map<String, Object> plannedChecks(String namespace, String serviceAccount, KubernetesIngressSpec ingress) {
        Map<String, Object> checks = new LinkedHashMap<String, Object>();
        checks.put("namespace", pending(namespace));
        checks.put("serviceAccount", pending(serviceAccount));
        checks.put("ingressController", pending(namespace + "/" + ingress.ingressClassName));
        checks.put("ingressControllerService", pending(ingress.ingressClassName + "-controller"));
        checks.put("ingressClass", pending(ingress.ingressClassName));
        checks.put("ingressHost", pending(ingress.host));
        return checks;
    }

    public Map<String, Object> plannedCoreChecks(String namespace, String serviceAccount) {
        Map<String, Object> checks = new LinkedHashMap<String, Object>();
        checks.put("namespace", pending(namespace));
        checks.put("serviceAccount", pending(serviceAccount));
        return checks;
    }

    public Map<String, Object> plannedIngressControllerChecks(String namespace, KubernetesIngressSpec ingress) {
        Map<String, Object> checks = new LinkedHashMap<String, Object>();
        checks.put("namespace", pending(namespace));
        checks.put("ingressController", pending(namespace + "/" + ingress.ingressClassName));
        checks.put("ingressControllerService", pending(ingress.ingressClassName + "-controller"));
        checks.put("ingressClass", pending(ingress.ingressClassName));
        return checks;
    }

    public Map<String, Object> plannedEnvironment(String namespace, String serviceAccount, String kubeconfigPath, boolean enableIngress, KubernetesIngressSpec ingress) {
        Map<String, Object> environment = new LinkedHashMap<String, Object>();
        environment.put("credentials", plannedCredentials(kubeconfigPath));
        environment.put("namespace", pending(namespace));
        environment.put("serviceAccount", pending(serviceAccount));
        environment.put("rbac", advisory("RBAC check is advisory in this phase."));
        environment.put("scheduling", advisory("Scheduling capacity check is advisory in this phase."));
        environment.put("network", advisory("Pod network and Service DNS check is advisory in this phase."));
        environment.put("policy", advisory("ResourceQuota, LimitRange, PodSecurity, and admission checks are advisory in this phase."));
        environment.put("ingress", enableIngress ? plannedIngress(namespace, ingress) : skippedIngress());
        return environment;
    }

    public Map<String, Object> checkEnvironment(String namespace, String serviceAccount, String kubeconfigPath, boolean enableIngress, KubernetesIngressSpec ingress) throws Exception {
        Map<String, Object> environment = new LinkedHashMap<String, Object>();
        environment.put("credentials", gateway.credentialStatus(kubeconfigPath));

        if (!gateway.namespaceExists(namespace)) {
            throw new OperationException("NamespaceNotFound", "Namespace '" + namespace + "' does not exist.");
        }
        environment.put("namespace", ok(namespace));

        if (!gateway.serviceAccountExists(namespace, serviceAccount)) {
            throw new OperationException("ServiceAccountNotFound",
                    "ServiceAccount '" + serviceAccount + "' does not exist in namespace '" + namespace + "'.");
        }
        environment.put("serviceAccount", ok(serviceAccount));
        environment.put("rbac", advisory("RBAC check is advisory in this phase."));
        environment.put("scheduling", advisory("Scheduling capacity check is advisory in this phase."));
        environment.put("network", advisory("Pod network and Service DNS check is advisory in this phase."));
        environment.put("policy", advisory("ResourceQuota, LimitRange, PodSecurity, and admission checks are advisory in this phase."));
        environment.put("ingress", enableIngress ? checkIngressEnvironment(namespace, ingress) : skippedIngress());
        return environment;
    }

    public Map<String, Object> check(String namespace, String serviceAccount, KubernetesIngressSpec ingress) throws Exception {
        Map<String, Object> checks = new LinkedHashMap<String, Object>();

        if (!gateway.namespaceExists(namespace)) {
            throw new OperationException("NamespaceNotFound", "Namespace '" + namespace + "' does not exist.");
        }
        checks.put("namespace", ok(namespace));

        if (!gateway.serviceAccountExists(namespace, serviceAccount)) {
            throw new OperationException("ServiceAccountNotFound",
                    "ServiceAccount '" + serviceAccount + "' does not exist in namespace '" + namespace + "'.");
        }
        checks.put("serviceAccount", ok(serviceAccount));

        if (!gateway.ingressControllerReady(namespace, ingress.ingressClassName)) {
            throw new OperationException("IngressControllerNotFound",
                    "No ready namespace-scoped Ingress Controller was found in namespace '" + namespace + "'.",
                    remediation.ingressController(namespace, ingress.ingressClassName));
        }
        checks.put("ingressController", ok(namespace + "/" + ingress.ingressClassName));
        checks.put("ingressControllerService", ok(gateway.ingressControllerService(namespace, ingress.ingressClassName)));

        if (!gateway.ingressClassExists(ingress.ingressClassName)) {
            throw new OperationException("IngressClassNotFound",
                    "IngressClass '" + ingress.ingressClassName + "' does not exist.");
        }
        checks.put("ingressClass", ok(ingress.ingressClassName));

        checks.put("ingressHost", ok(ingress.host));
        return checks;
    }

    public Map<String, Object> checkCore(String namespace, String serviceAccount) throws Exception {
        Map<String, Object> checks = new LinkedHashMap<String, Object>();

        if (!gateway.namespaceExists(namespace)) {
            throw new OperationException("NamespaceNotFound", "Namespace '" + namespace + "' does not exist.");
        }
        checks.put("namespace", ok(namespace));

        if (!gateway.serviceAccountExists(namespace, serviceAccount)) {
            throw new OperationException("ServiceAccountNotFound",
                    "ServiceAccount '" + serviceAccount + "' does not exist in namespace '" + namespace + "'.");
        }
        checks.put("serviceAccount", ok(serviceAccount));
        return checks;
    }

    public Map<String, Object> checkIngressController(String namespace, KubernetesIngressSpec ingress) throws Exception {
        Map<String, Object> checks = new LinkedHashMap<String, Object>();

        if (!gateway.namespaceExists(namespace)) {
            throw new OperationException("NamespaceNotFound", "Namespace '" + namespace + "' does not exist.");
        }
        checks.put("namespace", ok(namespace));

        if (!gateway.ingressControllerReady(namespace, ingress.ingressClassName)) {
            throw new OperationException("IngressControllerNotFound",
                    "No ready namespace-scoped Ingress Controller was found in namespace '" + namespace + "'.",
                    remediation.ingressController(namespace, ingress.ingressClassName));
        }
        checks.put("ingressController", ok(namespace + "/" + ingress.ingressClassName));
        checks.put("ingressControllerService", ok(gateway.ingressControllerService(namespace, ingress.ingressClassName)));

        if (!gateway.ingressClassExists(ingress.ingressClassName)) {
            throw new OperationException("IngressClassNotFound",
                    "IngressClass '" + ingress.ingressClassName + "' does not exist.");
        }
        checks.put("ingressClass", ok(ingress.ingressClassName));
        return checks;
    }

    private Map<String, Object> ok(Object value) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("ok", Boolean.TRUE);
        data.put("value", value);
        return data;
    }

    private Map<String, Object> pending(String value) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("ok", Boolean.FALSE);
        data.put("planned", Boolean.TRUE);
        data.put("value", value);
        return data;
    }

    private Map<String, Object> plannedCredentials(String kubeconfigPath) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("source", kubeconfigPath == null || kubeconfigPath.trim().isEmpty() ? "default" : "explicit");
        data.put("path", kubeconfigPath == null || kubeconfigPath.trim().isEmpty()
                ? System.getProperty("user.home") + "/.kube/config"
                : kubeconfigPath);
        data.put("apiReachable", Boolean.FALSE);
        data.put("planned", Boolean.TRUE);
        return data;
    }

    private Map<String, Object> plannedIngress(String namespace, KubernetesIngressSpec ingress) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("enabled", Boolean.TRUE);
        data.put("ingressClassName", ingress.ingressClassName);
        data.put("checks", plannedIngressControllerChecks(namespace, ingress));
        return data;
    }

    private Map<String, Object> checkIngressEnvironment(String namespace, KubernetesIngressSpec ingress) throws Exception {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("enabled", Boolean.TRUE);
        data.put("ingressClassName", ingress.ingressClassName);
        data.put("checks", checkIngressController(namespace, ingress));
        return data;
    }

    private Map<String, Object> skippedIngress() {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("enabled", Boolean.FALSE);
        data.put("message", "Ingress checks skipped by --enable-ingress=false.");
        return data;
    }

    private Map<String, Object> advisory(String message) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("ok", Boolean.TRUE);
        data.put("advisory", Boolean.TRUE);
        data.put("message", message);
        return data;
    }
}
