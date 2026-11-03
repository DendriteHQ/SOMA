# run-coordinator

Build with the repository root as context. This folder owns the container build.
See [service layout](../README.md) and the [deployment guide](../../docs/ops/docker-deployment.md).

Retryable failed runs are handled by the separate
[`auto-run-restarter`](../auto-run-restarter) worker. This service only seeds,
dispatches, and receives run updates.
