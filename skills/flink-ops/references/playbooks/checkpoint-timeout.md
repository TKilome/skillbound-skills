# Checkpoint Timeout Playbook

1. Require `--job-manager-url` and `--job-id`.
2. Run `get_checkpoints`.
3. Run `get_exceptions`.
4. Run `diagnose_job`.
5. Report checkpoint status, failure messages, and whether the evidence is
   sufficient for a root-cause hypothesis.
6. Do not inspect pods, logs, or YARN applications outside `FlinkOpsCli`.
