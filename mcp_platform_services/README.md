# MCP platform service builds

Every deployable container has a directory under `mcp_platform_services/` with a
Dockerfile. Build contexts are the repository root; do not build from inside a
service directory.

- `platform-api`: signed miner/validator APIs and heartbeat.
- `metagraph`: Bittensor connection, subnet snapshot and economics reads.
- `run-coordinator`: dispatch, readiness publication and sandbox reports.
- `auto-run-restarter`: independent retry worker for failed run recovery.
- `results`: frontend/incentive APIs and cached aggregates.
- `gateway`: shared build for the independently drainable `gateway-1` and `gateway-2` LLM streaming proxies.
- `ingress`: stable public paths and ports.
- `common`: shared application package, database access, contracts, blob storage,
  and the FastAPI lifecycle composition root.

## Source ownership

Platform source is organized under each service's `app/` directory:

| Folder | Owns |
| --- | --- |
| `common/app` | configuration, persistence, shared HTTP helpers, contracts, blob storage, metagraph client, and service lifecycle composition |
| `platform-api/app` | miner and validator routes, request signing/heartbeats, script uploads, SSM access, and rate limiting |
| `run-coordinator/app` | sandbox report routes, run dispatch, task publication/synchronization, screening, benchmark handling, and remote sandbox client |
| `auto-run-restarter/auto_run_restarter` | retryable-run scan, restart policy, advisory locking, and restart audit metadata |
| `results/app` | frontend and incentive routes, aggregates, scores, complexity calculations, and top-miner approval |
| `metagraph/app` | Bittensor chain connection, snapshot runner, and its internal read route |
| `gateway/gateway` | LLM proxy, provider/key resolution, request accounting, and drain controls |

The application packages use Python namespace paths so imports remain `app.*`.
Each image copies only its own package and `common/app`. API and results also copy
the coordinator package where competition-classification helpers are still
imported. API additionally copies the results package to generate the legacy
combined OpenAPI schema; it does not start result routes or workers. These
remaining in-process imports are deliberately retained until they are replaced by
an internal versioned contract.

See [deployment, configuration and cutover instructions](../docs/ops/docker-deployment.md).
