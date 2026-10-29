# Auto Run Restarter

Continuously scans active competitions for retryable failed SWE-bench runs and
deletes them so the orchestrator can seed/dispatch replacements.

For every successfully deleted run, the service appends a JSON line to:

```text
/var/lib/soma/restarts/YYYY-MM-DD/restarted_runs.jsonl
```

That path is bind-mounted from
`$SOMA_RUNTIME_DIR/auto-run-restarter/restarts` for the Compose worker. The
repository-local metadata path below is only for a standalone developer run.

Current restart rules:

- provider model `404` on `http://proxy:8080`
- exact `400 Provider returned error`
- provider `429` new-account rate-limit errors
- proxy DNS failures containing `ENOTFOUND`
- `500 Internal Server Error`
- provider `520` errors
- `502 Bad Gateway`
- Docker network pool exhaustion (`all predefined address pools have been fully subnetted`)
- container startup output accidentally captured in `last_error`
- git clone / fetch / checkout failures for benchmark repos
- missing copilot container `.env`
- proxy `ECONNREFUSED`
- proxy connections closing before a message completes
- unavailable Docker daemon or cgroup startup failure
- sandbox disk full (`No space left on device`)
- isolated Docker daemon (dind) not ready or failing to start the Copilot SWE sandbox
- timeout-like failures only when the run has:
  - no positive token columns
  - no recorded agent steps
- a miner run is restarted at most five times per `(competition, hotkey, task)`;
  the persisted counter is rebuilt from restart metadata on first use

Explicitly excluded:

- `Platform is at capacity...`

Run it with the main SOMA virtualenv:

```bash
cd /root/SOMA-containered
source .venv/bin/activate
PYTHONPATH=mcp_platform_services/auto-run-restarter python -m auto_run_restarter
```

Useful env overrides:

```bash
AUTO_RUN_RESTARTER_INTERVAL_SECONDS=60
AUTO_RUN_RESTARTER_MIN_RUN_AGE_SECONDS=120
AUTO_RUN_RESTARTER_BATCH_SIZE=100
AUTO_RUN_RESTARTER_FETCH_LIMIT=2000
AUTO_RUN_RESTARTER_MAX_RESTARTS_PER_TASK_HOTKEY_COMPETITION=5
AUTO_RUN_RESTARTER_DB_POOL_SIZE=2
AUTO_RUN_RESTARTER_DB_MAX_OVERFLOW=2
AUTO_RUN_RESTARTER_ADVISORY_LOCK_KEY=11426001
AUTO_RUN_RESTARTER_DRY_RUN=false
AUTO_RUN_RESTARTER_LOG_LEVEL=INFO
AUTO_RUN_RESTARTER_ENV_FILE=/root/SOMA-containered/.env
AUTO_RUN_RESTARTER_METADATA_DIR=/root/SOMA-containered/mcp_platform_services/auto-run-restarter/auto_run_restarter/metadata
```
