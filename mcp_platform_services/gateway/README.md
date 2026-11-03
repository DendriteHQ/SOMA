# gateway

This folder owns the shared build and implementation for the `gateway-1` and
`gateway-2` Compose services. The Python package is in `gateway/`. Ingress
balances port 8050 traffic across both replicas. Each gateway has an independent
failed-call volume so a restart cannot overwrite another replica's diagnostic
records.

Use `./mcp_platform_services/gateway/rolling-update.sh` to build and replace each instance in
turn. It drains an instance, waits for current requests and SSE streams to end,
then recreates it. During a drain, ingress safely retries only the gateway's
reserved pre-provider response on the other instance; it never retries provider
errors. See the [deployment guide](../../docs/ops/docker-deployment.md).

To take one member out of service without interrupting its active work, run
`./mcp_platform_services/gateway/rolling-update.sh --graceful-stop gateway-1`. The peer must
remain running.
