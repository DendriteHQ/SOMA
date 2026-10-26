# Build from the repository root:
#   docker build -f validator/Dockerfile --build-arg VALIDATOR_VERSION=$(git describe --tags --always) -t soma-validator .
FROM python:3.12-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
COPY validator/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt


FROM python:3.12-slim

# Grading drives the host's Docker daemon through the mounted socket: the SOMA task
# evaluator shells out to the docker CLI, the SWE-bench harness uses the Python SDK.
COPY --from=docker:27.5.1-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=build /opt/venv /opt/venv

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Runtime paths in settings and the harness are relative to the repository root
# (validator/.runtime, logs/, tasks/), so the root is the working directory.
WORKDIR /app
COPY validator ./validator

ARG VALIDATOR_VERSION=unknown
ENV VALIDATOR_VERSION=$VALIDATOR_VERSION
LABEL org.opencontainers.image.version=$VALIDATOR_VERSION

# The port is the one advertised to the platform, so it is read from VALIDATOR_PORT
# rather than fixed. Configuration comes from the environment (compose env_file).
CMD ["sh", "-c", "exec uvicorn validator.validator:app --host 0.0.0.0 --port ${VALIDATOR_PORT:-8000}"]
