# Build Image Playbook

1. Use this flow when a Kubernetes deployment references a local jar path.
2. Explain that `local://` is a container path, not the agent machine path.
3. Collect `--base-image`, `--local-jar`, and `--target-image`.
4. Derive `--target-jar-path` as `/opt/flink/usrlib/<local-jar-file-name>` when
   the user does not provide it.
5. Show the final `FlinkOpsCli k8s_build_image` command.
6. Wait for explicit confirmation, then execute with `--confirm`.
7. Use the returned `local:///opt/flink/usrlib/<jar-file-name>` for `k8s_start_job`.
