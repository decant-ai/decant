# Known Limitations for 0.1.0-alpha.1

## Runtime and source limits

- Kafka supports static assignment with trusted plaintext, verified TLS, or TLS plus SCRAM-SHA-256/512. It does not support consumer groups, dynamic partition discovery/rebalancing, broker commits, OAuth, Kerberos, or arbitrary native client properties.
- The resident service requires an external supervisor. Unix SIGINT/SIGTERM after readiness have a bounded healthy-shutdown path, but blocking startup, SIGKILL and filesystem stalls remain abrupt/uncertain cases. No automatic restart, automatic epoch rollover, dynamic config reload or zero-downtime upgrade protocol. See [lifecycle limits](MCP_COMPATIBILITY.md).
- Kafka JSON mapping supports required top-level null/Boolean/integer/UTF-8 projections. It does not support nested paths, arrays, floats, coercion, defaults, inference, or dynamic schemas.
- OTLP support reads a finite Collector JSON trace file. It is not a live OTLP/gRPC receiver and exact upstream JSON field compatibility can change.
- gRPC/Protobuf and WebSocket inputs are not implemented.

## State and storage limits

- Hot state is bounded in memory.
- Kafka checkpoints use the documented local atomic generation store, not RocksDB.
- Checkpoint generations are bounded and pruned by the resident lifecycle, but there is no operator migration/repair command, append-only Parquet replay archive, multi-node state, or distributed recovery.
- Current fault tests do not prove power-loss behavior on every filesystem/hardware combination.
- SIGKILL recovery returns to the latest verified checkpoint; progress after that cut must be redelivered from Kafka. Exactly-once processing is not claimed.

## Context and policy limits

- Kafka can emit one grouped Boolean ratio or one exact latest retained scalar transition using fields explicitly classified `allow`; older Kafka activity context and OTLP external context remain payload-free.
- Additive evidence V1 compares observed server spans from captures or compatible mapped Kafka input. Logical duplicate folding/conflict detection is retained-scope only. Sampling, missing baseline/version information, admission and output omissions limit interpretation. It does not infer deployment time, causality, whole-production rates or SLOs.
- Live evidence publication occurs at verified cuts, not every record. Pending progress and local observation ages are separate; source recency remains unknown. A failed worker withdraws the report instead of serving apparently current evidence.
- MCP provenance explanation locates evidence already present in the bounded compiled view. It is not a raw-event lookup or complete lineage service.
- Multiple views, arbitrary queries, HMAC pseudonymization, automatic sensitive-field discovery, final output scanning, and a general policy runtime are not implemented.
- V3 `deny` means omit. It does not transform or pseudonymize protected values, which remain inside local admitted/checkpoint state when mapped and can still affect admission resource accounting or rejection outcomes; no noninterference property is claimed.
- MCP is local stdio only. Remote MCP, tenant authentication, gRPC, and HTTP serving boundaries are not implemented.
- There is no model client or model routing.
- Do not infer complete anonymization, privacy compliance, confidentiality, or authorization from payload omission.

## Operations and evidence limits

- No production Prometheus/OpenTelemetry export, dashboard, control plane, Kubernetes integration, or hosted service exists.
- The opt-in Kafka harness covers one pinned image tag and local topology, not a compatibility matrix.
- The pilot benchmark specification and runner produce local commit-bound measurements. No throughput, latency, memory, fidelity, reduction, security, or production-readiness result is published by the repository.
- The pilot benchmark covers one pinned Kafka image, one partition, synthetic scalar data, and local stdio. Its baseline comparison is serialized bytes, not equivalent baseline throughput.
- Legacy operational counters are stderr JSON lines; new evidence diagnostics are fixed text. Neither is a Prometheus/OpenTelemetry endpoint. External supervision, bounded log retention, alerting and capacity monitoring are operator responsibilities.
- Native candidate packages are unsigned; signed-binary distribution is not available. Packaging reproducibility is not native compilation reproducibility, hermeticity, static linkage or cross-platform compatibility. The Cargo build-graph SBOM is incomplete for native/host libraries.
- Official SDK negotiation is tested. These tests do not certify every desktop client or SRE-agent integration, establish comparative utility, or demonstrate repeat usage. No such measurements are published here.

## Licensing and contribution scope

- AGPL-3.0-only is installed for original exported project material, with five explicit Apache-2.0 configuration exceptions; see [licensing](../../LICENSING.md). Third-party terms remain unchanged; internal records outside the export boundary are not part of the grant.
- See [contributing](../../CONTRIBUTING.md) for the current external code-intake policy. No CLA/DCO, commercial relicensing agreement or additional license grant is implied.
