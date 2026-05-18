# Stop Job Playbook

1. Prefer common Flink REST stop when the user provides `--job-manager-url`.
   If the URL is an Ingress Controller NodePort, also collect
   `--http-host-header <job-name>.flink.k8s.com`.
2. Collect `--job-id` and `--stop-mode`.
3. For `savepoint` or `drain`, collect `--savepoint-dir`.
4. Show the final `FlinkOpsCli stop_job` command.
5. Wait for explicit confirmation, then execute with `--confirm`.
6. Verify by querying `get_job_status` when the JobManager REST endpoint remains
   reachable.
7. If a provider-specific stop is requested without REST URL, identify provider
   and load the provider reference.
