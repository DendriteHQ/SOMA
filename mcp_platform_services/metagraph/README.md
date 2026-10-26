# metagraph

Owns the Bittensor connection, synchronized subnet snapshot, and chain economics
reads. It exposes its API only on the internal Compose network; `platform-api`
and `results` use a cached client to consume it.

Build with the repository root as context. See the
[container responsibilities](../../docs/ops/container-responsibilities.md).
