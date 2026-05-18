package com.skill.flinkops.provider.kubernetes;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

public final class KubernetesRemediation {
    public Map<String, Object> ingressController(String namespace, String ingressClassName) {
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("namespace", namespace);
        data.put("ingressClassName", ingressClassName);
        data.put("message", "Install a namespace-scoped Nginx Ingress Controller before starting the Flink job. These are manual remediation options; the CLI does not run Helm or kubectl.");
        data.put("helmCommands", Arrays.asList(
                "helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx",
                "helm upgrade --install " + ingressClassName + " ingress-nginx/ingress-nginx"
                        + " --namespace " + namespace
                        + " --set controller.ingressClassResource.name=" + ingressClassName
                        + " --set controller.ingressClassResource.controllerValue=k8s.io/" + ingressClassName
                        + " --set controller.watchNamespace=" + namespace));
        String renderedYaml = "/tmp/" + ingressClassName + "-controller.yaml";
        data.put("kubectlTemplatePath", templatePath());
        data.put("kubectlTemplateVariables", templateVariables(namespace, ingressClassName));
        data.put("kubectlManualRenderCommands", Arrays.asList(
                "\"$JAVA_HOME/bin/java\" -jar " + cliJarPath() + " k8s_render_ingress_controller_yaml --namespace " + namespace + " > " + renderedYaml));
        data.put("kubectlManualApplyCommands", Arrays.asList(
                "kubectl apply -f " + renderedYaml,
                "\"$JAVA_HOME/bin/java\" -jar " + cliJarPath() + " k8s_check_ingress_controller --namespace " + namespace));
        return data;
    }

    public String renderIngressControllerYaml(String namespace, String ingressClassName) {
        String controllerValue = "k8s.io/" + ingressClassName;
        String appName = ingressClassName;
        File template = new File(templatePath());
        if (template.isFile()) {
            try {
                return renderTemplate(new String(Files.readAllBytes(template.toPath()), StandardCharsets.UTF_8),
                        namespace, ingressClassName, controllerValue);
            } catch (IOException e) {
                throw new RuntimeException("Unable to read built-in Ingress Controller YAML template: " + template.getAbsolutePath(), e);
            }
        }
        return ""
                + "apiVersion: v1\n"
                + "kind: ServiceAccount\n"
                + "metadata:\n"
                + "  name: " + appName + "\n"
                + "  namespace: " + namespace + "\n"
                + "---\n"
                + "apiVersion: rbac.authorization.k8s.io/v1\n"
                + "kind: Role\n"
                + "metadata:\n"
                + "  name: " + appName + "\n"
                + "  namespace: " + namespace + "\n"
                + "rules:\n"
                + "  - apiGroups: [\"\"]\n"
                + "    resources: [\"configmaps\", \"endpoints\", \"pods\", \"secrets\", \"services\"]\n"
                + "    verbs: [\"get\", \"list\", \"watch\"]\n"
                + "  - apiGroups: [\"discovery.k8s.io\"]\n"
                + "    resources: [\"endpointslices\"]\n"
                + "    verbs: [\"get\", \"list\", \"watch\"]\n"
                + "  - apiGroups: [\"networking.k8s.io\"]\n"
                + "    resources: [\"ingresses\"]\n"
                + "    verbs: [\"get\", \"list\", \"watch\"]\n"
                + "  - apiGroups: [\"networking.k8s.io\"]\n"
                + "    resources: [\"ingresses/status\"]\n"
                + "    verbs: [\"update\"]\n"
                + "  - apiGroups: [\"coordination.k8s.io\"]\n"
                + "    resources: [\"leases\"]\n"
                + "    verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\"]\n"
                + "  - apiGroups: [\"events.k8s.io\"]\n"
                + "    resources: [\"events\"]\n"
                + "    verbs: [\"create\", \"patch\"]\n"
                + "---\n"
                + "apiVersion: rbac.authorization.k8s.io/v1\n"
                + "kind: RoleBinding\n"
                + "metadata:\n"
                + "  name: " + appName + "\n"
                + "  namespace: " + namespace + "\n"
                + "roleRef:\n"
                + "  apiGroup: rbac.authorization.k8s.io\n"
                + "  kind: Role\n"
                + "  name: " + appName + "\n"
                + "subjects:\n"
                + "  - kind: ServiceAccount\n"
                + "    name: " + appName + "\n"
                + "    namespace: " + namespace + "\n"
                + "---\n"
                + "apiVersion: rbac.authorization.k8s.io/v1\n"
                + "kind: ClusterRole\n"
                + "metadata:\n"
                + "  name: " + appName + "\n"
                + "rules:\n"
                + "  - apiGroups: [\"\"]\n"
                + "    resources: [\"namespaces\"]\n"
                + "    verbs: [\"get\"]\n"
                + "  - apiGroups: [\"networking.k8s.io\"]\n"
                + "    resources: [\"ingressclasses\"]\n"
                + "    verbs: [\"get\", \"list\", \"watch\"]\n"
                + "---\n"
                + "apiVersion: rbac.authorization.k8s.io/v1\n"
                + "kind: ClusterRoleBinding\n"
                + "metadata:\n"
                + "  name: " + appName + "\n"
                + "roleRef:\n"
                + "  apiGroup: rbac.authorization.k8s.io\n"
                + "  kind: ClusterRole\n"
                + "  name: " + appName + "\n"
                + "subjects:\n"
                + "  - kind: ServiceAccount\n"
                + "    name: " + appName + "\n"
                + "    namespace: " + namespace + "\n"
                + "---\n"
                + "apiVersion: networking.k8s.io/v1\n"
                + "kind: IngressClass\n"
                + "metadata:\n"
                + "  name: " + ingressClassName + "\n"
                + "spec:\n"
                + "  controller: " + controllerValue + "\n"
                + "---\n"
                + "apiVersion: apps/v1\n"
                + "kind: Deployment\n"
                + "metadata:\n"
                + "  name: " + appName + "-controller\n"
                + "  namespace: " + namespace + "\n"
                + "  labels:\n"
                + "    app.kubernetes.io/name: ingress-nginx\n"
                + "    app.kubernetes.io/instance: " + appName + "\n"
                + "spec:\n"
                + "  replicas: 1\n"
                + "  selector:\n"
                + "    matchLabels:\n"
                + "      app.kubernetes.io/name: ingress-nginx\n"
                + "      app.kubernetes.io/instance: " + appName + "\n"
                + "  template:\n"
                + "    metadata:\n"
                + "      labels:\n"
                + "        app.kubernetes.io/name: ingress-nginx\n"
                + "        app.kubernetes.io/instance: " + appName + "\n"
                + "    spec:\n"
                + "      serviceAccountName: " + appName + "\n"
                + "      containers:\n"
                + "        - name: controller\n"
                + "          image: registry.k8s.io/ingress-nginx/controller:v1.10.1\n"
                + "          env:\n"
                + "            - name: POD_NAME\n"
                + "              valueFrom:\n"
                + "                fieldRef:\n"
                + "                  fieldPath: metadata.name\n"
                + "            - name: POD_NAMESPACE\n"
                + "              valueFrom:\n"
                + "                fieldRef:\n"
                + "                  fieldPath: metadata.namespace\n"
                + "          args:\n"
                + "            - /nginx-ingress-controller\n"
                + "            - --watch-namespace=" + namespace + "\n"
                + "            - --ingress-class=" + ingressClassName + "\n"
                + "            - --controller-class=" + controllerValue + "\n"
                + "            - --election-id=" + appName + "-leader\n"
                + "          ports:\n"
                + "            - name: http\n"
                + "              containerPort: 80\n"
                + "            - name: https\n"
                + "              containerPort: 443\n"
                + "            - name: webhook\n"
                + "              containerPort: 8443\n"
                + "---\n"
                + "apiVersion: v1\n"
                + "kind: Service\n"
                + "metadata:\n"
                + "  name: " + appName + "-controller\n"
                + "  namespace: " + namespace + "\n"
                + "spec:\n"
                + "  type: NodePort\n"
                + "  selector:\n"
                + "    app.kubernetes.io/name: ingress-nginx\n"
                + "    app.kubernetes.io/instance: " + appName + "\n"
                + "  ports:\n"
                + "    - name: http\n"
                + "      port: 80\n"
                + "      targetPort: http\n"
                + "    - name: https\n"
                + "      port: 443\n"
                + "      targetPort: https\n";
    }

    private String renderTemplate(String template, String namespace, String ingressClassName, String controllerValue) {
        return template
                .replace("{{NAMESPACE}}", namespace)
                .replace("{{INGRESS_CLASS_NAME}}", ingressClassName)
                .replace("{{CONTROLLER_VALUE}}", controllerValue);
    }

    private Map<String, Object> templateVariables(String namespace, String ingressClassName) {
        Map<String, Object> variables = new LinkedHashMap<String, Object>();
        variables.put("NAMESPACE", namespace);
        variables.put("INGRESS_CLASS_NAME", ingressClassName);
        variables.put("CONTROLLER_VALUE", "k8s.io/" + ingressClassName);
        return variables;
    }

    private String templatePath() {
        return skillDirPath() + "/assets/kubernetes/ingress-controller.yaml.tpl";
    }

    private String cliJarPath() {
        File location = new File(codeLocationPath());
        if (location.isFile()) {
            return location.getAbsolutePath();
        }
        return skillDirPath() + "/scripts/target/flink-intelligent-ops.jar";
    }

    private String skillDirPath() {
        File location = new File(codeLocationPath());
        if (location.isFile()) {
            File targetDir = location.getParentFile();
            File scriptsDir = targetDir == null ? null : targetDir.getParentFile();
            File skillDir = scriptsDir == null ? null : scriptsDir.getParentFile();
            if (skillDir != null) {
                return skillDir.getAbsolutePath();
            }
        }
        File classesDir = location;
        File targetDir = classesDir.getParentFile();
        File scriptsDir = targetDir == null ? null : targetDir.getParentFile();
        File skillDir = scriptsDir == null ? null : scriptsDir.getParentFile();
        return skillDir == null ? "." : skillDir.getAbsolutePath();
    }

    private String codeLocationPath() {
        try {
            return new File(KubernetesRemediation.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getAbsolutePath();
        } catch (URISyntaxException e) {
            return "scripts/target/flink-intelligent-ops.jar";
        }
    }
}
