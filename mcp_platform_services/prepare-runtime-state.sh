#!/usr/bin/env bash
set -euo pipefail

# Create the host paths used for persistent platform state. Application
# containers run as UID/GID 10001 and never need write access to source code.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime_dir="${SOMA_RUNTIME_DIR:-$repo_dir/../SOMA/runtime}"
runtime_dir="$(realpath -m "$runtime_dir")"
service_dirs=(
  metagraph
  platform-api
  run-coordinator
  auto-run-restarter
  results
  gateways
)

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root so the service directories can be owned by UID 10001." >&2
  exit 1
fi

for service_dir in "${service_dirs[@]}"; do
  install -d -m 0750 -o 10001 -g 10001 "$runtime_dir/$service_dir"
done

# Gateway replicas share this parent but create one child per instance.
install -d -m 0750 -o 10001 -g 10001 "$runtime_dir/gateways/failed_calls"

echo "Prepared persistent runtime state under $runtime_dir"
