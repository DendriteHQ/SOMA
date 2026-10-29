#!/usr/bin/env bash
# Drain and replace gateway instances without ending in-flight provider streams.
# Run from any directory; it uses the repository's compose.yaml.
set -euo pipefail

repository_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repository_dir"

compose=(docker compose)
if [[ -n "${SOMA_ENV_FILE:-}" ]]; then
    compose+=(--env-file "$SOMA_ENV_FILE")
fi

# A finite default exposes a stuck stream instead of silently killing a request.
# Set this to the maximum supported model stream duration when needed.
drain_timeout_seconds="${GATEWAY_DRAIN_TIMEOUT_SECONDS:-1800}"
poll_seconds="${GATEWAY_DRAIN_POLL_SECONDS:-2}"
mode="replace"

usage() {
    cat <<'EOF'
Usage:
  rolling-update.sh [gateway-1|gateway-2 ...]
  rolling-update.sh --graceful-stop gateway-1|gateway-2

The default builds and replaces each selected gateway after it has drained.
--graceful-stop drains exactly one gateway, waits for its active calls to finish,
then stops it without recreating it. Its peer must remain running.
EOF
}

case "${1:-}" in
    --graceful-stop)
        mode="stop"
        shift
        ;;
    --help|-h)
        usage
        exit 0
        ;;
esac

if [[ "$mode" == "stop" ]] && (( $# != 1 )); then
    usage >&2
    exit 2
fi

if (( $# )); then
    services=("$@")
else
    services=(gateway-1 gateway-2)
fi

for service in "${services[@]}"; do
    case "$service" in
        gateway-1|gateway-2) ;;
        *) echo "Only gateway-1 and gateway-2 may be managed by this script: $service" >&2; exit 2 ;;
    esac
done

control() {
    local service="$1"
    local method="$2"
    local path="$3"
    "${compose[@]}" exec -T "$service" python -c '
import sys
from urllib.request import Request, urlopen
method, path = sys.argv[1:]
request = Request("http://127.0.0.1:8050" + path, data=b"" if method == "POST" else None, method=method)
print(urlopen(request, timeout=5).read().decode())
' "$method" "$path"
}

active_request_count() {
    control "$1" GET /_internal/gateway/status | sed -nE 's/.*"active_requests":([0-9]+).*/\1/p'
}

wait_for_no_active_requests() {
    local service="$1"
    local deadline=$((SECONDS + drain_timeout_seconds))
    while true; do
        local active
        active="$(active_request_count "$service")"
        if [[ "$active" =~ ^[0-9]+$ ]] && (( active == 0 )); then
            return 0
        fi
        if (( SECONDS >= deadline )); then
            echo "$service is still handling ${active:-unknown} request(s) after ${drain_timeout_seconds}s; it remains drained and was not replaced." >&2
            return 1
        fi
        sleep "$poll_seconds"
    done
}

wait_until_ready() {
    local service="$1"
    local deadline=$((SECONDS + drain_timeout_seconds))
    while true; do
        if "${compose[@]}" exec -T "$service" python -c '
from urllib.request import urlopen
urlopen("http://127.0.0.1:8050/health/ready", timeout=3).read()
' >/dev/null 2>&1; then
            return 0
        fi
        if (( SECONDS >= deadline )); then
            echo "$service did not become ready within ${drain_timeout_seconds}s." >&2
            return 1
        fi
        sleep "$poll_seconds"
    done
}

is_running() {
    "${compose[@]}" ps --status running -q "$1" | grep -q .
}

if [[ "$mode" == "replace" ]]; then
    echo "Building the replacement gateway image(s)..."
    "${compose[@]}" build "${services[@]}"
fi

for service in "${services[@]}"; do
    if is_running "$service"; then
        echo "Draining $service; new requests will use its peer."
        control "$service" POST /_internal/gateway/drain >/dev/null
        wait_for_no_active_requests "$service"
    elif [[ "$mode" == "stop" ]]; then
        echo "$service is not running; nothing to stop."
        continue
    else
        echo "$service is stopped; starting its replacement."
    fi

    if [[ "$mode" == "stop" ]]; then
        echo "Stopping drained $service..."
        "${compose[@]}" stop "$service"
        echo "$service stopped after all active requests completed."
    else
        echo "Replacing $service..."
        "${compose[@]}" up -d --no-deps --force-recreate "$service"
        wait_until_ready "$service"
        echo "$service is ready."
    fi
done

if [[ "$mode" == "stop" ]]; then
    echo "Gateway graceful stop completed."
else
    echo "Gateway rolling update completed."
fi
