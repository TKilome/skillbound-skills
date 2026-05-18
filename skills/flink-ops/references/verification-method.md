# Verification Method

## Read Operations

Read operations are complete when `FlinkOpsCli` returns `success:true` and the
response contains the requested endpoint or returned data.

## Dry Runs

`--dry-run` verifies command construction only. It does not prove cluster state
changed and must not be reported as a successful deployment, stop, or image
build.

## Mutations

Mutations require explicit confirmation and `--confirm`. After a successful
mutation, verify by reading back state when a read path exists.

## Provider Results

`ProviderNotImplemented` means the provider is known but does not implement the
requested command. `UnsupportedProvider` means the request must stop or ask for
a supported provider.

## Failure Results

Never claim success when the CLI returns `success:false`, a non-zero exit code,
or an error code such as `SafetyCheckRequired`, `ValidationError`,
`ProviderNotImplemented`, or `UnsupportedProvider`.
