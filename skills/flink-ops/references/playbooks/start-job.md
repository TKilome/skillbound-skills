# Start Job Playbook

1. Classify mode: Real Ops, Eval, or Boundary.
2. Identify provider. If missing, ask for `--deployment-target`.
3. For `kubernetes`, load `references/providers/kubernetes.md`.
4. For `yarn`, load `references/providers/yarn.md` and use YARN application
   mode.
5. For a fresh Kubernetes deployment, collect readiness inputs first:
   `--namespace`, `--service-account`, `--flink-home`.
6. Run `k8s_check_ingress_controller`, derive `--enable-ingress`, then run
   `k8s_preflight_start`.
   If the user asks for the Ingress Controller NodePort, read it from the
   `ingressControllerService.value.ports[*].nodePort` values returned by
   `k8s_check_ingress_controller`.
   For agent-side REST access, use `http://<node-ip>:<http-nodeport>` with
   `--http-host-header <job-name>.flink.k8s.com`. For external users, keep the
   domain URL.
7. If the jar is local to the agent machine, use `k8s_build_image` and derive
   `local:///opt/flink/usrlib/<jar-file-name>`.
8. Collect start configuration. Keep common parameters aligned across providers:
   `--name`, `--jar-uri`, `--main-class`, `--args`, `--parallelism`,
   `--jobmanager-memory`, `--taskmanager-memory`, `--taskmanager-slots`,
   `--dynamic-properties`, and `--savepoint-path`.
9. Show the final `FlinkOpsCli` command. Kubernetes uses `k8s_start_job`; YARN
   uses `start_job --deployment-target yarn`. Real provider submission must use
   `"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*"`
   and must not use `java -jar`.
10. Wait for explicit confirmation, then execute with `--confirm`.
11. Verify the returned JobManager URL or provider-specific result.
