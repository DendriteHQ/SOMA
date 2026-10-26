# auto-run-restarter

This worker independently scans the shared database for retryable failed
SWE-bench runs. It deletes only runs that match the established retry policy;
the run coordinator seeds replacements on its next normal tick. It has no HTTP
port, does not call the sandbox fleet, and is not routed through ingress.

A successful database scan updates its health marker; the container becomes
unhealthy if that marker is stale. Restart counts and audit JSONL are stored in
the dedicated `restarter-state` Docker volume at
`/var/lib/soma/restarts`.

The PostgreSQL advisory lock remains a final safeguard against two restarters,
but Compose starts exactly one `auto-run-restarter` instance and explicitly
disables the old coordinator-embedded loop.

## Lifecycle

Compose starts this worker only after `run-coordinator` is healthy. The worker
immediately performs a scan and repeats it every
`AUTO_RUN_RESTARTER_INTERVAL_SECONDS` (60 seconds by default). Its transaction
holds the configured PostgreSQL advisory lock while selecting and deleting
candidates. A second worker therefore skips a tick while the first owns the
lock, but operators should still run only the Compose-managed instance.

The worker writes `/var/lib/soma/restarts/last_successful_tick` after every
successful database scan. Its Docker health check requires that marker to be
newer than `max(180 seconds, 3 × interval + 30 seconds)`. Repeated database or
query failures leave the container running but unhealthy so monitoring can
alert; Docker's `unless-stopped` policy only restarts an exited process.

## Configuration

The worker needs the same writer-database configuration as the coordinator:
`POSTGRES_DSN`, or `RDS_SECRET_ID` with `RDS_DB_NAME`, `RDS_PORT`, AWS region,
and credentials or instance identity. Compose supplies these from `.env` and
sets `AUTO_RUN_RESTARTER_METADATA_DIR=/var/lib/soma/restarts`; do not override
that path in Compose unless a replacement persistent mount is also supplied.

| Setting | Default | Effect |
| --- | --- | --- |
| `AUTO_RUN_RESTARTER_INTERVAL_SECONDS` | `60` | Delay between completed scans; minimum `5`. |
| `AUTO_RUN_RESTARTER_MIN_RUN_AGE_SECONDS` | `120` | A terminal run must be at least this old; minimum `30`. |
| `AUTO_RUN_RESTARTER_BATCH_SIZE` | `100` | Maximum eligible runs deleted in one scan. |
| `AUTO_RUN_RESTARTER_FETCH_LIMIT` | `batch × 20` | Candidate rows examined before policy filtering. |
| `AUTO_RUN_RESTARTER_MAX_RESTARTS_PER_TASK_HOTKEY_COMPETITION` | `5` | Per-miner, per-task cap; `0` disables that cap. |
| `AUTO_RUN_RESTARTER_DB_POOL_SIZE` / `AUTO_RUN_RESTARTER_DB_MAX_OVERFLOW` | `2` / `2` | Database connection capacity for this worker. |
| `AUTO_RUN_RESTARTER_ADVISORY_LOCK_KEY` | `11426001` | Lock shared by all restarter instances for this database. |
| `AUTO_RUN_RESTARTER_DRY_RUN` | `false` | Logs matching candidates without deleting any runs. |
| `AUTO_RUN_RESTARTER_LOG_LEVEL` | `INFO` | Python logging level. |

The candidate policy itself, including excluded capacity errors and the restart
cap, is listed in [the retry policy source](auto_run_restarter/README.md).

## Operate and inspect

Build from the repository root through Compose:

```bash
SOMA_ENV_FILE=.env docker compose --env-file .env up -d --build auto-run-restarter
```

Inspect it without exposing database credentials:

```bash
SOMA_ENV_FILE=.env docker compose --env-file .env ps auto-run-restarter
SOMA_ENV_FILE=.env docker compose --env-file .env logs --tail=100 -f auto-run-restarter
SOMA_ENV_FILE=.env docker compose --env-file .env exec -T auto-run-restarter \
  cat /var/lib/soma/restarts/last_successful_tick
```

The volume contains `restart_counts.json`, the health marker, and daily audit
files at `/var/lib/soma/restarts/YYYY-MM-DD/restarted_runs.jsonl`. Rebuild or
restart only this worker after a policy change; this does not interrupt gateway
streams or public HTTP endpoints:

```bash
SOMA_ENV_FILE=.env docker compose --env-file .env up -d --build --force-recreate auto-run-restarter
```

For a safe policy check, set `AUTO_RUN_RESTARTER_DRY_RUN=true` in a staging
environment, recreate the worker, and review `matched_runs` in its logs. Return
the setting to `false` and recreate the worker to resume deletion. Do not delete
the `restarter-state` volume during routine restarts because it contains the
persisted per-miner restart caps and audit trail.

See the [deployment guide](../../docs/ops/docker-deployment.md) for the full
stack lifecycle and [container responsibilities](../../docs/ops/container-responsibilities.md)
for ownership boundaries.
