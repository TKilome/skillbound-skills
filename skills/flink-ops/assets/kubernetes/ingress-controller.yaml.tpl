apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{INGRESS_CLASS_NAME}}
  namespace: {{NAMESPACE}}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{INGRESS_CLASS_NAME}}
  namespace: {{NAMESPACE}}
rules:
  - apiGroups: [""]
    resources: ["configmaps", "endpoints", "pods", "secrets", "services"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["discovery.k8s.io"]
    resources: ["endpointslices"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingresses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingresses/status"]
    verbs: ["update"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
  - apiGroups: ["events.k8s.io"]
    resources: ["events"]
    verbs: ["create", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{INGRESS_CLASS_NAME}}
  namespace: {{NAMESPACE}}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{INGRESS_CLASS_NAME}}
subjects:
  - kind: ServiceAccount
    name: {{INGRESS_CLASS_NAME}}
    namespace: {{NAMESPACE}}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{INGRESS_CLASS_NAME}}
rules:
  - apiGroups: [""]
    resources: ["namespaces"]
    verbs: ["get"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["ingressclasses"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{INGRESS_CLASS_NAME}}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{INGRESS_CLASS_NAME}}
subjects:
  - kind: ServiceAccount
    name: {{INGRESS_CLASS_NAME}}
    namespace: {{NAMESPACE}}
---
apiVersion: networking.k8s.io/v1
kind: IngressClass
metadata:
  name: {{INGRESS_CLASS_NAME}}
spec:
  controller: {{CONTROLLER_VALUE}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{INGRESS_CLASS_NAME}}-controller
  namespace: {{NAMESPACE}}
  labels:
    app.kubernetes.io/name: ingress-nginx
    app.kubernetes.io/instance: {{INGRESS_CLASS_NAME}}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ingress-nginx
      app.kubernetes.io/instance: {{INGRESS_CLASS_NAME}}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ingress-nginx
        app.kubernetes.io/instance: {{INGRESS_CLASS_NAME}}
    spec:
      serviceAccountName: {{INGRESS_CLASS_NAME}}
      containers:
        - name: controller
          image: registry.k8s.io/ingress-nginx/controller:v1.10.1
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          args:
            - /nginx-ingress-controller
            - --watch-namespace={{NAMESPACE}}
            - --ingress-class={{INGRESS_CLASS_NAME}}
            - --controller-class={{CONTROLLER_VALUE}}
            - --election-id={{INGRESS_CLASS_NAME}}-leader
          ports:
            - name: http
              containerPort: 80
            - name: https
              containerPort: 443
            - name: webhook
              containerPort: 8443
---
apiVersion: v1
kind: Service
metadata:
  name: {{INGRESS_CLASS_NAME}}-controller
  namespace: {{NAMESPACE}}
spec:
  type: NodePort
  selector:
    app.kubernetes.io/name: ingress-nginx
    app.kubernetes.io/instance: {{INGRESS_CLASS_NAME}}
  ports:
    - name: http
      port: 80
      targetPort: http
    - name: https
      port: 443
      targetPort: https
