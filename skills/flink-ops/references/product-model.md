# Flink Intelligent Ops Product Model

Common Flink operations target the JobManager REST API.

```text
Flink Cluster
  -> JobManager REST
    -> Overview
    -> Jobs
      -> Job detail
      -> Exceptions
      -> Checkpoints
      -> Vertices
```

Deployment operations are provider-specific. The current providers start Flink
native Kubernetes application mode and YARN application mode through Flink Java
Client APIs.

```text
Flink Java Client
  -> KubernetesClusterClientFactory
    -> KubernetesClusterDescriptor
    -> deployApplicationCluster
    -> kubernetes.cluster-id
    -> kubernetes.namespace
    -> kubernetes.service-account
    -> kubernetes.container.image
    -> JobManager / TaskManagers
      -> Job / Checkpoints / Savepoints
```

Important identifiers:

- `name`: value mapped to `kubernetes.cluster-id`.
- `namespace`: value mapped to `kubernetes.namespace`.
- `service-account`: value mapped to `kubernetes.service-account`.
- `jar-uri`: job jar path visible to the Flink image.
- `flink-image`: value mapped to `kubernetes.container.image`.
- `savepoint-dir`: durable external path for savepoints.

The MVP uses StreamPark-style Flink Java Client deployment for Kubernetes start.
Common status and diagnosis use the JobManager REST API after the JobManager URL
is known.

Provider status:

- `kubernetes`: implemented for Flink native Kubernetes application mode.
- `yarn`: implemented for Flink YARN application mode. Common REST operations
  handle status, diagnosis, and stop after a JobManager URL is known.
