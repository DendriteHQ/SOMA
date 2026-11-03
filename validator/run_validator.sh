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
# docker: run the validator with validator/docker-compose.yml, rebuilding the image on updates.
# pm2: run it from the repo's .venv as a PM2 process.
# auto (default): docker when Docker with the Compose plugin is usable, else pm2. A
# watcher started before docker support picks this up on its next self-update, so
# existing PM2 validators move to the container without any change on the host.
VALIDATOR_RUNTIME="${VALIDATOR_RUNTIME:-auto}"
PM2_NAME="${PM2_NAME:-mcp-validator}"
UPSTREAM_BRANCH="${UPSTREAM_BRANCH:-origin/main}"
ENV_FILE="${ENV_FILE:-validator/.env}"
COMPOSE_FILE="${COMPOSE_FILE:-validator/docker-compose.yml}"
SOMA_SHARED_PACKAGE_NAME="${SOMA_SHARED_PACKAGE_NAME:-soma-shared}"
SOMA_SHARED_REPO_URL="${SOMA_SHARED_REPO_URL:-https://github.com/DendriteHQ/SOMA-shared.git}"
SOMA_SHARED_BRANCH="${SOMA_SHARED_BRANCH:-main}"
SCRIPT_PATH=$(realpath "${BASH_SOURCE[0]}")

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$*"
}

if [[ "$VALIDATOR_RUNTIME" == "auto" ]]; then
    if docker compose version >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
        VALIDATOR_RUNTIME=docker
    else
        log "Docker with the Compose plugin is not usable; running the validator via pm2."
        VALIDATOR_RUNTIME=pm2
    fi
fi

case "$VALIDATOR_RUNTIME" in
    pm2)
        if ! command -v pm2 >/dev/null 2>&1; then
            log "pm2 command not found; please install pm2 first."
            exit 1
        fi
        ;;
    docker)
        if ! docker compose version >/dev/null 2>&1; then
            log "docker compose not found; please install Docker with the Compose plugin first."
            exit 1
        fi
        ;;
    *)
        log "VALIDATOR_RUNTIME must be 'pm2' or 'docker' (got '$VALIDATOR_RUNTIME')."
        exit 1
        ;;
esac

if [[ ! -d "$REPO_DIR/.git" ]]; then
    log "Repository directory '$REPO_DIR' does not contain a .git folder."
    exit 1
fi

cd "$REPO_DIR"

if ! git rev-parse "$UPSTREAM_BRANCH" >/dev/null 2>&1; then
    log "Upstream branch '$UPSTREAM_BRANCH' not found. Set UPSTREAM_BRANCH explicitly if needed."
    exit 1
fi

resolve_python_bin() {
    if [[ -x "$REPO_DIR/.venv/bin/python" ]]; then
        printf '%s\n' "$REPO_DIR/.venv/bin/python"
    elif command -v python3 >/dev/null 2>&1; then
        command -v python3
    elif command -v python >/dev/null 2>&1; then
        command -v python
    else
        return 1
    fi
}

get_file_fingerprint() {
    local file_path="$1"

    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$file_path" | awk '{ print $1 }'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$file_path" | awk '{ print $1 }'
    else
        cksum "$file_path" | awk '{ print $1":"$2 }'
    fi
}

restart_monitor_if_script_changed() {
    local current_fingerprint

    current_fingerprint=$(get_file_fingerprint "$SCRIPT_PATH")
    if [[ -z "$current_fingerprint" || "$current_fingerprint" == "$SCRIPT_FINGERPRINT" ]]; then
        return 0
    fi

    log "Detected updated monitor script; restarting self."
    exec env \
        REPO_DIR="$REPO_DIR" \
        VALIDATOR_RUNTIME="$VALIDATOR_RUNTIME" \
        PM2_NAME="$PM2_NAME" \
        UPSTREAM_BRANCH="$UPSTREAM_BRANCH" \
        ENV_FILE="$ENV_FILE" \
        COMPOSE_FILE="$COMPOSE_FILE" \
        SOMA_SHARED_PACKAGE_NAME="$SOMA_SHARED_PACKAGE_NAME" \
        SOMA_SHARED_REPO_URL="$SOMA_SHARED_REPO_URL" \
        SOMA_SHARED_BRANCH="$SOMA_SHARED_BRANCH" \
        bash "$SCRIPT_PATH" "$INTERVAL"
}

SCRIPT_FINGERPRINT=$(get_file_fingerprint "$SCRIPT_PATH")
if [[ -z "$SCRIPT_FINGERPRINT" ]]; then
    log "Unable to fingerprint monitor script '$SCRIPT_PATH'."
    exit 1
fi

get_installed_soma_shared_commit() {
    local python_bin="$1"

    "$python_bin" -m pip freeze | awk -v package_name="$SOMA_SHARED_PACKAGE_NAME" '
        BEGIN { IGNORECASE = 1 }
        $0 ~ ("^" package_name " @ git\\+") {
            sub(/^.*@/, "", $0)
            print $0
            exit
        }
    '
}

get_remote_soma_shared_commit() {
    git ls-remote "$SOMA_SHARED_REPO_URL" "refs/heads/$SOMA_SHARED_BRANCH" 2>/dev/null | awk 'NR == 1 { print $1 }'
}

upgrade_soma_shared() {
    local python_bin="$1"
    local package_spec="$SOMA_SHARED_PACKAGE_NAME @ git+$SOMA_SHARED_REPO_URL@$SOMA_SHARED_BRANCH"

    log "Refreshing $SOMA_SHARED_PACKAGE_NAME from $SOMA_SHARED_BRANCH."
    if ! "$python_bin" -m pip install --force-reinstall --no-deps "$package_spec"; then
        log "$SOMA_SHARED_PACKAGE_NAME refresh failed."
        return 1
    fi

    return 0
}

load_validator_port() {
    if [[ -f "$ENV_FILE" ]]; then
        local env_port
        env_port=$(grep -E '^[[:space:]]*VALIDATOR_PORT=' "$ENV_FILE" | tail -n 1 | cut -d '=' -f 2- | tr -d '[:space:]')
        if [[ -n "$env_port" ]]; then
            VALIDATOR_PORT="$env_port"
        fi
    fi
    VALIDATOR_PORT="${VALIDATOR_PORT:-8000}"
}

update_requirements() {
    local python_bin

    if ! python_bin=$(resolve_python_bin); then
        log "No Python interpreter found for dependency update."
        return 1
    fi

    log "Installing Python dependencies from requirements.txt."
    if ! "$python_bin" -m pip install -r requirements.txt; then
        log "Dependency update failed."
        return 1
    fi

    return 0
}

stop_pm2_validator() {
    # A validator left in PM2 (e.g. from before switching to docker) holds the port
    # the container publishes, so it has to go before the container starts.
    if command -v pm2 >/dev/null 2>&1 && pm2 describe "$PM2_NAME" >/dev/null 2>&1; then
        log "Removing PM2 validator '$PM2_NAME' so the container can bind its port."
        pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true
    fi
}

start_validator_docker() {
    # Build before touching anything that runs, so a failed build leaves the current
    # validator (container or PM2 process) serving, and the switch from PM2 is short.
    log "Building validator image ('$COMPOSE_FILE')."
    if ! docker compose -f "$COMPOSE_FILE" build; then
        log "Image build failed; leaving validator as-is. Resolve manually."
        return 1
    fi
    stop_pm2_validator
    log "Starting validator via docker compose."
    if ! docker compose -f "$COMPOSE_FILE" up -d; then
        log "docker compose up failed. Resolve manually."
        return 1
    fi
}

is_validator_running_docker() {
    [[ -n "$(docker compose -f "$COMPOSE_FILE" ps --status running --quiet validator 2>/dev/null)" ]]
}

start_validator() {
    local python_bin

    if [[ "$VALIDATOR_RUNTIME" == "docker" ]]; then
        start_validator_docker
        return
    fi

    if ! python_bin=$(resolve_python_bin); then
        log "No Python interpreter found to start validator."
        return 1
    fi

    load_validator_port
    log "Starting validator via pm2 ('$PM2_NAME')."
    pm2 delete "$PM2_NAME" >/dev/null 2>&1 || true
    pm2 start "$python_bin -m uvicorn validator.validator:app --host 0.0.0.0 --port $VALIDATOR_PORT --env-file $ENV_FILE" --name "$PM2_NAME"
}

is_validator_running() {
    local pid

    if [[ "$VALIDATOR_RUNTIME" == "docker" ]]; then
        is_validator_running_docker
        return
    fi

    pid=$(pm2 pid "$PM2_NAME" 2>/dev/null | tr -d '\n' | tr -d '[:space:]')
    if [[ -z "$pid" || "$pid" == "0" || "$pid" == "[]" ]]; then
        return 1
    fi
    return 0
}

action_if_needed() {
    local local_hash remote_hash
    local python_bin installed_soma_shared_commit remote_soma_shared_commit
    local repo_update_needed=0
    local soma_shared_update_needed=0

    if ! git fetch --prune "${UPSTREAM_BRANCH%%/*}" >/dev/null 2>&1; then
        log "git fetch failed; retrying after sleep."
        return 1
    fi

    local_hash=$(git rev-parse HEAD)
    remote_hash=$(git rev-parse "$UPSTREAM_BRANCH")

    # The image installs dependencies, soma-shared included at the commit pinned in
    # validator/requirements.txt, so in docker mode only the repository is tracked.
    if [[ "$VALIDATOR_RUNTIME" == "pm2" ]]; then
        if ! python_bin=$(resolve_python_bin); then
            log "No Python interpreter found for dependency inspection."
            return 1
        fi

        installed_soma_shared_commit=$(get_installed_soma_shared_commit "$python_bin")
        remote_soma_shared_commit=$(get_remote_soma_shared_commit)
        if [[ -z "$remote_soma_shared_commit" ]]; then
            log "Unable to resolve remote commit for $SOMA_SHARED_PACKAGE_NAME on $SOMA_SHARED_BRANCH."
        elif [[ "$installed_soma_shared_commit" != "$remote_soma_shared_commit" ]]; then
            soma_shared_update_needed=1
        fi
    fi

    if [[ "$local_hash" != "$remote_hash" ]]; then
        repo_update_needed=1
    fi

    if [[ "$repo_update_needed" -eq 1 ]]; then
        log "New commit detected on $UPSTREAM_BRANCH ($local_hash -> $remote_hash). Synchronizing repository."
        if git reset --hard "$UPSTREAM_BRANCH"; then
            log "Repository synchronized to $(git rev-parse --short HEAD)."
        else
            log "Reset failed; leaving validator as-is. Resolve manually."
            return 1
        fi
    fi

    if [[ "$soma_shared_update_needed" -eq 1 ]]; then
        log "New $SOMA_SHARED_PACKAGE_NAME commit detected on $SOMA_SHARED_BRANCH (${installed_soma_shared_commit:-none} -> $remote_soma_shared_commit)."
    fi

    if [[ "$repo_update_needed" -eq 1 && "$VALIDATOR_RUNTIME" == "pm2" ]]; then
        log "Updating dependencies from requirements.txt."
        if ! update_requirements; then
            log "Dependency update failed; leaving validator as-is. Resolve manually."
            return 1
        fi
    fi

    if [[ "$soma_shared_update_needed" -eq 1 ]]; then
        log "Refreshing $SOMA_SHARED_PACKAGE_NAME only."
        if ! upgrade_soma_shared "$python_bin"; then
            log "$SOMA_SHARED_PACKAGE_NAME refresh failed; leaving validator as-is. Resolve manually."
            return 1
        fi
    fi

    if [[ "$repo_update_needed" -eq 1 || "$soma_shared_update_needed" -eq 1 ]]; then
        log "Updates applied. Restarting validator."
        start_validator
    elif ! is_validator_running; then
        log "Validator is not running; starting now."
        start_validator
    else
        log "No updates detected; validator already running."
    fi

    restart_monitor_if_script_changed
}

log "Watching $REPO_DIR (tracking $UPSTREAM_BRANCH) every $INTERVAL seconds."

trap 'log "Monitor interrupted."; exit 0' INT TERM

while true; do
    action_if_needed || true
    sleep "$INTERVAL"
done
