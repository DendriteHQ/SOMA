# syntax=docker/dockerfile:1.7
#
# One image for the sandbox API and its per-run runners (`python -m app.runner`).
# Build with scripts/build_sandbox_images.sh, which pins SOMA-benchmark and
# SOMA-shared to exact commits and tags the image with them.
#
#   docker build \
#     --build-context soma_benchmark=<SOMA-benchmark checkout or git URL#ref> \
#     --build-arg SOMA_SHARED_REF=<commit> -t soma-sandbox:<tag> sandbox_service
FROM python:3.12-slim-bookworm

# The docker CLI and compose plugin: both the API and the runners drive the host's
# daemon through its mounted socket. git: soma_bench clones task repositories.
# iptables (nf_tables backend, like current hosts) and procps (sysctl): soma_bench fences
# each run's compression service off from dind with DROP rules in the host's DOCKER-USER
# chain; the runner shares the host's network namespace to do it.
ARG DOCKER_APT_VERSION=5:29.1.4-1~debian.12~bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git gnupg iptables procps \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends "docker-ce-cli=${DOCKER_APT_VERSION}" docker-compose-plugin \
    && apt-get purge -y gnupg && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

ARG SOMA_SHARED_REF
RUN test -n "${SOMA_SHARED_REF}" \
    && pip install --no-deps "git+https://github.com/DendriteHQ/SOMA-shared.git@${SOMA_SHARED_REF}"

# Editable, because soma_bench resolves its compose file and the scripts it bind-mounts
# from its source tree, which a regular install does not ship.
COPY --from=soma_benchmark . /opt/soma-benchmark
RUN pip install --no-deps -e /opt/soma-benchmark \
    && cp /opt/soma-benchmark/src/soma_bench/benchmark/backends/copilot/copilot-cli-container/.env.example \
          /opt/soma-benchmark/src/soma_bench/benchmark/backends/copilot/copilot-cli-container/.env \
    && rm -rf /opt/soma-benchmark/.git

COPY . /opt/soma-sandbox/
WORKDIR /opt/soma-sandbox

ARG SANDBOX_VERSION=dev
LABEL org.opencontainers.image.title="soma-sandbox" \
      org.opencontainers.image.version="${SANDBOX_VERSION}"
ENV SANDBOX_VERSION=${SANDBOX_VERSION}

EXPOSE 8001
CMD ["python", "main.py"]
