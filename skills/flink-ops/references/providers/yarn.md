# YARN Provider

Status: implemented for YARN application mode

YARN deployment follows StreamPark-style Flink Java Client application mode.
When `--deployment-target yarn` is used, `start_job` builds a Flink
configuration and calls Flink's YARN Java client APIs. It does not call external
`yarn`, `$FLINK_HOME/bin/flink`, or shell scripts.

## Planned Parameters

- `--flink-home`: local Flink installation used for YARN submission runtime.
- `--flink-version`: optional Flink version used by the compatibility adapter.
- `--name`: Flink application name.
- `--yarn-queue`: target YARN queue.
- `--yarn-provided-lib-dirs`: comma-separated YARN provided library dirs,
  usually HDFS paths.
- `--flink-dist-jar`: Flink distribution jar visible to YARN.
- `--jar-uri`: job jar visible to the submission runtime.
- `--main-class`: application entry class when the jar manifest is not enough.
- `--parallelism`: initial job parallelism.
- `--jobmanager-memory`, `--taskmanager-memory`, `--taskmanager-slots`: common
  resource parameters aligned with Kubernetes start.
- `--savepoint-path`: restore path for stateful starts.
- `--dynamic-properties`: comma-separated Flink configuration overrides.

## Rules

- Do not call `yarn`, `$FLINK_HOME/bin/flink`, or shell scripts as fallback.
- Run with `"$JAVA_HOME/bin/java" -cp "$SKILL_DIR/scripts/target/flink-intelligent-ops.jar:$FLINK_HOME/lib/*"`
  for real submission; `java -jar` cannot load Flink/YARN runtime classes.
- `stop_job` remains a common Flink REST operation and can stop a YARN job when
  the JobManager REST URL and job ID are provided.
- Common Flink REST reads may still be used when the user provides a
  JobManager REST URL.
