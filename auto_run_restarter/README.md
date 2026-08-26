# Auto Run Restarter

Continuously scans active competitions for retryable failed SWE-bench runs and
deletes them so the orchestrator can seed/dispatch replacements.

For every successfully deleted run, the service appends a JSON line to:

```text
auto_run_restarter/metadata/YYYY-MM-DD/restarted_runs.jsonl
```

Current restart rules:

- provider model `404` on `http://proxy:8080`
- exact `400 Provider returned error`
- proxy DNS failures containing `ENOTFOUND`
- `500 Internal Server Error`
- `502 Bad Gateway`
- container startup output accidentally captured in `last_error`
- git clone / fetch / checkout failures for benchmark repos
- missing copilot container `.env`
- proxy `ECONNREFUSED`
- timeout-like failures only when the run has:
  - no positive token columns
  - no recorded agent steps

Explicitly excluded:

- `Platform is at capacity...`

Run it with the main SOMA virtualenv:

```bash
cd /root/SOMA
source .venv/bin/activate
python -m auto_run_restarter
```

Useful env overrides:

```bash
AUTO_RUN_RESTARTER_INTERVAL_SECONDS=60
AUTO_RUN_RESTARTER_MIN_RUN_AGE_SECONDS=120
AUTO_RUN_RESTARTER_BATCH_SIZE=100
AUTO_RUN_RESTARTER_FETCH_LIMIT=2000
AUTO_RUN_RESTARTER_DRY_RUN=false
AUTO_RUN_RESTARTER_LOG_LEVEL=INFO
AUTO_RUN_RESTARTER_ENV_FILE=/root/SOMA/mcp_platform/.env
AUTO_RUN_RESTARTER_METADATA_DIR=/root/SOMA/auto_run_restarter/metadata
```
