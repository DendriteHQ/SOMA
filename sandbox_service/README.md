# sandbox_service

Runs compact-bench runs (Copilot and Claude Code agents) dispatched by the platform
and reports each result back to it. One host, one API, one container per run.

```
docker compose ─► sandbox-api (:8001, host network)
                     │  docker run, one per run
                     ▼
                  sandbox-runner ─► benchmark-solve ─► the run's compose stack
                                                       (agent, proxy, compression-service,
                                                        dind, swe-exec-broker)
platform ─POST /run_compact_bench_task─► sandbox-api ─POST report─► platform
```

Both are the same image (`soma-sandbox:<tag>`). The API (`main.py`) accepts runs,
enforces the host's concurrency limit, starts a runner per run, collects its
result and delivers it to the platform. The runner (`python -m app.runner`)
executes exactly one run and exits.

## Agents

| `agent_name` | agent image (env) | built by |
|---|---|---|
| `copilot` | `COPILOT_AGENT_IMAGE` (`local/copilot-cli`) | `build_images.sh --harness copilot` |
| `claude_code` | `CLAUDE_CODE_AGENT_IMAGE` (`local/claude-code-agent`) | `build_images.sh --harness claude_code` |

Anything else gets HTTP 400. Both agents run on soma_bench's shared harness: the same
per-run stack (proxy, compression-service, dind, swe-exec-broker), the same teardown,
the same report. soma_bench still names that harness's settings `SOMA_COPILOT_*` and
the run's compose project `soma-copilot-run-<run_id>` for either agent.

Claude Code speaks the Anthropic Messages API and runs on an Anthropic model. Its
requests go through the proxy and the gateway to OpenRouter's `/v1/messages`
unchanged. The runner pins `PROXY_OPENAI_TRANSLATION=0`, because translating them to
OpenAI chat completions drops fields Anthropic relies on (`cache_control`, thinking
signatures, system blocks).


## Lifecycle of a run

1. **Accept.** Capacity counts every run the API has not finished with, including
   runs still being torn down. Full, it answers 429 and starts a short cooldown.
   A dispatch for a run already on the host is acknowledged and ignored.
2. **Launch.** Refresh the SOMA task rows if the instance is unknown, tear down
   anything a previous attempt of the run left, `docker run` the runner.
3. **Run.** The runner downloads the miner into the run's tmpfs work dir, runs
   `benchmark-solve` with the compose project `soma-copilot-run-<run_id>`,
   uploads the trajectory and compressor log, writes `result.json`.
4. **Deadline.** A runner still running after the run's timeout + grace +
   `SANDBOX_RUNNER_DEADLINE_MARGIN_SECONDS` is killed and reported as
   `RunnerDeadlineExceeded`.
5. **Finish.** When the runner exits, the API queues its report (or one built
   from the exit state: `RunnerExited ... exit_code, oom_killed`), saves the
   runner's log, verifies teardown and removes the runner and run dir. Only then
   does the run free its slot.

Reports are queued in SQLite (`callback_queue.sqlite3`) together with the
dispatch id in one transaction, so a crash can neither lose a report nor queue
it twice. Delivery retries with backoff until the platform acknowledges.

## Teardown by ownership

`app/teardown.py` removes a run's containers, volumes and networks by its compose
project label, the workspace volume by name, plus any helper container still
holding that volume, and the run's work dir. It retries until a re-listing comes
back empty. dind holds its mounts for a while after it is told to stop, which
is what used to leak volumes. A run whose teardown keeps failing keeps its slot,
because its volumes still occupy the host's tmpfs.

Every few minutes the API also tears down resources of runs it does not own
(`soma-copilot-run-*` projects and `run-<id>` dirs without a runner). That is why
the old pm2 sandbox and this one must never run on the same host.

The old age-based dangling-volume sweep (`volume_janitor.py`) runs in dry-run
mode (`SANDBOX_LEGACY_VOLUME_SWEEP=log`): a `would_remove` warning means
teardown missed something. Retire it once it stays quiet.

## Building and deploying

```bash
./build_images.sh --benchmark-ref <ref> --shared-ref <ref> [--push <registry>]
```

builds `soma-sandbox`, `soma-copilot-compression-service` and the agent images picked
with `--harness` (`copilot` by default, `claude_code`, or `all`), each tagged with the
commits it contains, and prints the lines that select them.
Hosts are set up and updated with SANDBOX-ANSIBLE (`playbook.yml`, `deploy.yml`).
By hand:

```bash
SANDBOX_IMAGE=soma-sandbox:<tag> docker compose -f docker-compose.yml up -d
```

with `/etc/soma-sandbox/sandbox.env` modelled on `.env.example`. A deploy
recreates only the API. Runs in flight finish on the images they started with,
so no drain and no `docker rmi` are needed. `POST /admin/drain
{"enabled": true}` stops accepting runs before host maintenance.


`COMPACT_BENCH_DEBUG_PRESERVE_OUTPUTS=true` keeps run work dirs after teardown.

## Tests

```bash
python -m pytest sandbox_service/tests
```

The supervisor and teardown tests run against an in-memory stand-in for the docker
CLI (`tests/fake_docker.py`).
