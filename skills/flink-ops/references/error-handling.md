# Error Handling

All command output is a JSON envelope.

Failure shape:

```json
{
  "success": false,
  "operation": "k8s_start_job",
  "error": {
    "code": "ValidationError",
    "message": "Parameter '--namespace' is required."
  },
  "requestId": ""
}
```

Recovery:

| Code | Meaning | Action |
|------|---------|--------|
| `ValidationError` | Missing or invalid parameter | Ask for the exact missing value, but explain what the parameter means, where to get it, an example value, and any caveat before asking |
| `SafetyCheckRequired` | Mutation lacks `--confirm` | Show final command and ask for explicit approval before rerun |
| `RuntimeException` | Flink Java Client deployment failed | Report message; do not investigate with external CLIs |
| `IOException` | Flink REST call failed | Verify JobManager URL, network path, and job id |
| `NamespaceNotFound` | Kubernetes namespace is missing | Ask for a valid namespace or request namespace creation outside this skill |
| `ServiceAccountNotFound` | ServiceAccount is missing in the namespace | Ask for a valid service account or request setup outside this skill |
| `IngressControllerNotFound` | Namespace-scoped Ingress Controller is missing | Show `helmCommands`, `kubectlTemplatePath`, `kubectlManualRenderCommands`, and `kubectlManualApplyCommands` from error data when present; ask whether the user wants to create it manually; do not install it or execute the manual commands |
| `IngressClassNotFound` | Expected namespace-specific IngressClass is missing | Ask for class setup or provide remediation |
| `IngressCreationFailed` | Ingress create/update failed after Flink submission | Report partial failure and include known submission details |
| `FlinkRuntimeClasspathMissing` | Provider start was run without Flink runtime jars on the JVM classpath | Rerun the final start command with `"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*" com.skill.flinkops.FlinkOpsCli ...` |

## Provider Errors

- `ProviderNotImplemented`: the provider is known but does not implement the
  requested provider-specific command.
- `UnsupportedProvider`: the provider is unknown or outside this skill.
- `ProviderParameterMissing`: a provider-specific required parameter is missing.

Never claim success when `success` is `false`.
