# Diagnose Job Playbook

1. Require a JobManager REST URL from the user or a visible successful start
   result in the current conversation.
   If using Ingress Controller NodePort internally, use
   `--job-manager-url http://<node-ip>:<http-nodeport>` and
   `--http-host-header <job-name>.flink.k8s.com`.
2. Run `get_job_status`.
3. If a job id is available, run `get_exceptions`.
4. If a job id is available, run `get_checkpoints`.
5. Summarize status, latest exception, checkpoint state, and missing data.
6. Do not infer Kubernetes or YARN state from REST-only diagnostics.
