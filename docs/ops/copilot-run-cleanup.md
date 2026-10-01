# Copilot run directory cleanup

The service determines `run-<id>` before benchmark launch and removes it after
execution and Docker teardown, including failures and timeouts. Timeouts kill
the benchmark process group before directory deletion.

The janitor sweeps abandoned run directories every 300 seconds, retaining them
for 86400 seconds by default. Configure these with
`COMPACT_BENCH_OUTPUT_CLEANUP_INTERVAL_SECONDS` and
`COMPACT_BENCH_OUTPUT_RETENTION_SECONDS`. Active runs are protected by shared
process locks stored outside the bounded run filesystem. Symlinks are skipped
and deletion failures are logged.

The root honors `SOMA_COPILOT_RUN_ROOT`, then `SOMA_COPILOT_TMP_ROOT`, otherwise
`/tmp/soma-benchmark-copilot-runs` under Python's temporary directory.
`COMPACT_BENCH_DEBUG_PRESERVE_OUTPUTS=true` disables deletion. The service sets
`SOMA_COPILOT_PRESERVE_RUN_DIRS=true` in benchmark subprocesses to disable the
benchmark's competing age-only sweep.

Restart the service to load the changes. On first rollout, stop old workers and
their benchmark processes before starting the new version: old workers do not
hold the new locks. Existing abandoned directories are removed at the retention
age, rather than immediately clearing the full mount. Standalone benchmark
processes must use a separate root because they do not hold service locks.
