#!/usr/bin/env bash
set -euo pipefail

INTERVAL="${1:-60}"
if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]] || [[ "$INTERVAL" -le 0 ]]; then
    echo "Usage: $0 <poll_interval_seconds>" >&2
    echo "Interval must be a positive integer (got '$INTERVAL')." >&2
    exit 1
fi

if [[ -z "${REPO_DIR:-}" ]]; then
    CURRENT_DIR=$(pwd)
    if [[ "${CURRENT_DIR##*/}" == "validator" ]]; then
        REPO_DIR=$(cd .. && pwd)
    else
        echo "Error: run this script from MCP-subnet/validator (current: $CURRENT_DIR)." >&2
        exit 1
    fi
fi
PM2_NAME="${PM2_NAME:-mcp-validator}"
UPSTREAM_BRANCH="${UPSTREAM_BRANCH:-origin/main}"

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*"
}

if ! command -v pm2 >/dev/null 2>&1; then
    log "pm2 command not found; please install pm2 first."
    exit 1
fi

if [[ ! -d "$REPO_DIR/.git" ]]; then
    log "Repository directory '$REPO_DIR' does not contain a .git folder."
    exit 1
fi

cd "$REPO_DIR"

if ! git rev-parse "$UPSTREAM_BRANCH" >/dev/null 2>&1; then
    log "Upstream branch '$UPSTREAM_BRANCH' not found. Set UPSTREAM_BRANCH explicitly if needed."
    exit 1
fi

start_validator() {
    log "Starting validator via pm2 ('$PM2_NAME')."
    pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true
    pm2 start "uvicorn validator.validator:app --host 0.0.0.0 --port 8000 --env-file validator/.env" --name "$PM2_NAME"
}

is_validator_running() {
    local pid
    pid=$(pm2 pid "$PM2_NAME" 2>/dev/null | tr -d '\n' | tr -d '[:space:]')
    if [[ -z "$pid" || "$pid" == "0" || "$pid" == "[]" ]]; then
        return 1
    fi
    return 0
}

action_if_needed() {
    local local_hash remote_hash

    if ! git fetch --prune "${UPSTREAM_BRANCH%%/*}" >/dev/null 2>&1; then
        log "git fetch failed; retrying after sleep."
        return 1
    fi

    local_hash=$(git rev-parse HEAD)
    remote_hash=$(git rev-parse "$UPSTREAM_BRANCH")

    if [[ "$local_hash" != "$remote_hash" ]]; then
        log "New commit detected on $UPSTREAM_BRANCH ($local_hash -> $remote_hash). Synchronizing repository."
        if git reset --hard "$UPSTREAM_BRANCH"; then
            log "Repository synchronized to $(git rev-parse --short HEAD). Restarting validator."
            start_validator
        else
            log "Reset failed; leaving validator as-is. Resolve manually."
        fi
    elif ! is_validator_running; then
        log "Validator is not running; starting now."
        start_validator
    else
        log "No updates detected; validator already running."
    fi
}

log "Watching $REPO_DIR (tracking $UPSTREAM_BRANCH) every $INTERVAL seconds."

trap 'log "Monitor interrupted."; exit 0' INT TERM

while true; do
    action_if_needed || true
    sleep "$INTERVAL"
done
