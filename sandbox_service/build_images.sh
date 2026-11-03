#!/usr/bin/env bash
# Build (or rebuild) the images a sandbox host runs:
#
#   soma-sandbox:<tag>                        API and runners (this directory)
#   soma-copilot-compression-service:<btag>   per-run proxy, compression-service, swe-exec-broker
#   local/copilot-cli:<btag>                  the Copilot agent        (--harness copilot)
#   local/claude-code-agent:<btag>            the Claude Code agent    (--harness claude_code)
#
# By default each image is tagged with the commits it was built from, so a new build
# never replaces an image a run in flight still uses: no `docker rmi`, no drain.
#
# Usage:
#   build_images.sh (--benchmark-ref <ref> | --benchmark-dir <path>) [--shared-ref <ref>]
#                   [--harness <name>[,<name>]] [--tag <tag>] [--push <registry/prefix>] [--up]
#
#   --benchmark-ref  SOMA-benchmark branch, tag or commit, cloned from $SOMA_BENCHMARK_REPO
#   --benchmark-dir  a local SOMA-benchmark checkout, uncommitted changes included
#   --shared-ref     SOMA-shared branch, tag or commit (default: dev)
#   --harness        agent images to build, named like the platform's agent_name: copilot,
#                    claude_code (claude-code works too), a comma list or all
#                    (default: copilot); repeatable
#   --tag            one fixed tag for all three images instead of commit tags, e.g. dev;
#                    rebuilding with it replaces the previous images of that tag
#   --push           also push the images under this registry prefix
#   --up             then recreate the API from this directory's docker-compose.yml
#                    (env file: $SANDBOX_ENV_FILE, default ./.env); runs in flight keep going
#
# Examples:
#   build_images.sh --benchmark-ref dev --shared-ref dev                  # what SANDBOX-ANSIBLE does
#   build_images.sh --benchmark-dir ~/SOMA-benchmark --tag dev --up       # local dev loop
#   build_images.sh --benchmark-dir ~/SOMA-benchmark --tag dev --harness claude_code
#
# Prints the env lines that select the new images; all but SANDBOX_IMAGE belong in the
# env file.
set -euo pipefail

SOMA_BENCHMARK_REPO="${SOMA_BENCHMARK_REPO:-https://github.com/DendriteHQ/SOMA-benchmark.git}"
SOMA_SHARED_REPO="${SOMA_SHARED_REPO:-https://github.com/DendriteHQ/SOMA-shared.git}"
benchmark_ref=""
benchmark_dir=""
shared_ref="dev"
harness_args=()
fixed_tag=""
push_prefix=""
compose_up=false

usage() { sed -n '2,34p' "$0"; }

# Per harness: agent image name, its build context inside SOMA-benchmark, and the env
# variable its compose file reads the image from.
declare -A AGENT_IMAGE=(
  [copilot]="local/copilot-cli"
  [claude_code]="local/claude-code-agent"
)
declare -A AGENT_CONTEXT=(
  [copilot]="src/soma_bench/benchmark/backends/copilot/copilot-cli-container"
  [claude_code]="src/soma_bench/benchmark/backends/claude_code/claude-code-container"
)
declare -A AGENT_IMAGE_ENV=(
  [copilot]="COPILOT_AGENT_IMAGE"
  [claude_code]="CLAUDE_CODE_AGENT_IMAGE"
)
ALL_HARNESSES=(copilot claude_code)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --benchmark-ref) benchmark_ref="$2"; shift 2 ;;
    --benchmark-dir) benchmark_dir="$2"; shift 2 ;;
    --shared-ref) shared_ref="$2"; shift 2 ;;
    --harness) harness_args+=("$2"); shift 2 ;;
    --tag) fixed_tag="$2"; shift 2 ;;
    --push) push_prefix="${2%/}"; shift 2 ;;
    --up) compose_up=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
if [[ -n "$benchmark_ref" && -n "$benchmark_dir" ]] || [[ -z "$benchmark_ref$benchmark_dir" ]]; then
  echo "pass exactly one of --benchmark-ref and --benchmark-dir" >&2
  exit 2
fi

harnesses=()
for arg in "${harness_args[@]:-copilot}"; do
  IFS=',' read -ra names <<< "$arg"
  for name in "${names[@]}"; do
    name="${name//-/_}"
    if [[ "$name" == all ]]; then
      harnesses+=("${ALL_HARNESSES[@]}")
    elif [[ -n "${AGENT_IMAGE[$name]:-}" ]]; then
      harnesses+=("$name")
    else
      echo "unknown harness: $name (known: ${ALL_HARNESSES[*]}, all)" >&2
      exit 2
    fi
  done
done
# Deduplicated, in the order given.
mapfile -t harnesses < <(printf '%s\n' "${harnesses[@]}" | awk '!seen[$0]++')

resolve_commit() {  # <repo> <ref>
  local repo="$1" ref="$2" sha
  if [[ "$ref" =~ ^[0-9a-f]{40}$ ]]; then
    echo "$ref"
    return
  fi
  sha="$(git ls-remote "$repo" "refs/heads/$ref" "refs/tags/$ref^{}" "refs/tags/$ref" | awk 'NR==1 {print $1}')"
  if [[ -z "$sha" ]]; then
    echo "cannot resolve $ref in $repo (pass a full commit sha for an unpushed commit)" >&2
    exit 1
  fi
  echo "$sha"
}

# Short commit of a checkout, marked -dirty when it has uncommitted changes under <path>.
checkout_version() {  # <checkout> <path>
  local version
  version="$(git -C "$1" rev-parse --short=10 HEAD)"
  if [[ -n "$(git -C "$1" status --porcelain -- "$2")" ]]; then
    version="${version}-dirty"
  fi
  echo "$version"
}

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir "$work/soma-benchmark"

if [[ -n "$benchmark_ref" ]]; then
  benchmark_sha="$(resolve_commit "$SOMA_BENCHMARK_REPO" "$benchmark_ref")"
  benchmark_version="${benchmark_sha:0:10}"
  git clone --quiet --filter=blob:none --no-checkout "$SOMA_BENCHMARK_REPO" "$work/repo"
  git -C "$work/repo" archive "$benchmark_sha" | tar -x -C "$work/soma-benchmark"
  benchmark_source="SOMA-benchmark ${benchmark_sha}"
else
  benchmark_dir="$(cd "$benchmark_dir" && pwd)"
  benchmark_version="$(checkout_version "$benchmark_dir" .)"
  # Tracked and untracked files as they are on disk, minus whatever .gitignore excludes
  # (caches, .env, build output). Deleted tracked files are skipped.
  git -C "$benchmark_dir" ls-files -z --cached --others --exclude-standard \
    | tar -C "$benchmark_dir" --null --ignore-failed-read -T - -cf - 2>/dev/null \
    | tar -x -C "$work/soma-benchmark"
  benchmark_source="SOMA-benchmark ${benchmark_dir} @ ${benchmark_version}"
fi

shared_sha="$(resolve_commit "$SOMA_SHARED_REPO" "$shared_ref")"
sandbox_version="$(checkout_version "$here" .)"

if [[ -n "$fixed_tag" ]]; then
  benchmark_tag="$fixed_tag"
  sandbox_tag="$fixed_tag"
else
  benchmark_tag="$benchmark_version"
  sandbox_tag="${sandbox_version}-b${benchmark_version}-s${shared_sha:0:10}"
fi

echo "==> soma-copilot-compression-service:${benchmark_tag}"
docker build -t "soma-copilot-compression-service:${benchmark_tag}" "$work/soma-benchmark/src/compression_service"

image_env_lines=(
  "COMPACT_BENCH_COMPRESSION_SERVICE_IMAGE=soma-copilot-compression-service:${benchmark_tag}"
)
for harness in "${harnesses[@]}"; do
  agent_image="${AGENT_IMAGE[$harness]}:${benchmark_tag}"
  echo "==> ${agent_image}"
  docker build -t "$agent_image" "$work/soma-benchmark/${AGENT_CONTEXT[$harness]}"
  image_env_lines+=("${AGENT_IMAGE_ENV[$harness]}=${agent_image}")
done

echo "==> soma-sandbox:${sandbox_tag}"
docker build \
  --build-context "soma_benchmark=$work/soma-benchmark" \
  --build-arg "SOMA_SHARED_REF=${shared_sha}" \
  --build-arg "SANDBOX_VERSION=${sandbox_version}-b${benchmark_version}-s${shared_sha:0:10}" \
  -t "soma-sandbox:${sandbox_tag}" "$here"

if [[ -n "$push_prefix" ]]; then
  images=("soma-sandbox:${sandbox_tag}")
  for line in "${image_env_lines[@]}"; do
    images+=("${line#*=}")
  done
  for image in "${images[@]}"; do
    target="${push_prefix}/${image#local/}"
    docker tag "$image" "$target"
    docker push "$target"
  done
fi

echo
echo "Built from ${benchmark_source}, SOMA-shared ${shared_sha}, sandbox ${sandbox_version}."
echo
echo "SANDBOX_IMAGE=soma-sandbox:${sandbox_tag}"
printf '%s\n' "${image_env_lines[@]}"

if $compose_up; then
  env_file="${SANDBOX_ENV_FILE:-$here/.env}"
  if [[ ! -f "$env_file" ]]; then
    echo "env file $env_file not found; set SANDBOX_ENV_FILE" >&2
    exit 1
  fi
  for line in "${image_env_lines[@]}"; do
    if ! grep -qx "$line" "$env_file"; then
      echo "warning: $env_file lacks '$line'; new runs keep using the images it names" >&2
    fi
  done
  echo
  echo "==> recreating sandbox-api with soma-sandbox:${sandbox_tag}"
  # The API container only; runners are separate containers and the new API adopts them.
  SANDBOX_IMAGE="soma-sandbox:${sandbox_tag}" SANDBOX_ENV_FILE="$env_file" \
    docker compose --project-directory "$here" -f "$here/docker-compose.yml" up -d --force-recreate
fi
