# Resource Disclosure

Real operations may touch these resources through `FlinkOpsCli`:

- Flink JobManager REST endpoints provided by the user or returned by a
  successful start.
- Kubernetes API resources in the selected namespace for implemented
  Kubernetes provider operations.
- Container image build runtime for `k8s_build_image`.
- Future YARN ResourceManager and submission runtime when the YARN provider is
  implemented.

The skill must not print kubeconfig content, tokens, certificates, cloud
credentials, or environment variables containing secrets.
