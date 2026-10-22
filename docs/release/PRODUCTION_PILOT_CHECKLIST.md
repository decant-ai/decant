# Kafka-to-MCP Production-Pilot Checklist

This checklist authorizes one bounded, supervised design-partner pilot. It does not certify Decant for general production use and does not authorize a public release.

## Candidate and environment

- [ ] Use one reviewed clean commit and record its full 40-character Git identifier.
- [ ] Run `./scripts/kafka-mcp-pilot-gate-v0.sh --engineering` on the candidate.
- [ ] Confirm the Kafka cluster ID, topic ID, exact static partition set, and starting offsets with the stream owner.
- [ ] Confirm the single-node, single-writer, verified-local-checkpoint authority and no-broker-commit behavior are acceptable.
- [ ] Provision a dedicated checkpoint directory with reviewed permissions, capacity monitoring, backup/retention policy, and a tested restore copy.
- [ ] Use verified TLS with SCRAM where the broker requires it. Plaintext is acceptable only for an explicitly reviewed trusted network. Keep credentials in environment references and out of config, logs, evidence, and issues.
- [ ] Review the strict JSON mapping, epoch, lateness, capacity, retention, allow/deny policy, entity-key egress action, item limit, and byte budget against authorized sample data.

## Failure and measurement evidence

- [ ] On the exact candidate, run `DECANT_RUN_KAFKA_MCP_FAULT_MATRIX_V0=1 ./examples/kafka-mcp/run-pilot-fault-matrix-v0.py` on a disposable Docker-capable host and retain the fixed PASS/FAIL record outside Git.
- [ ] On the exact candidate, run `DECANT_RUN_KAFKA_MCP_BENCHMARK_V0=1 ./examples/kafka-mcp/run-pilot-benchmark-v0.py --output benchmarks/results/kafka-mcp-pilot-v0.json` on the intended pilot-class host.
- [ ] Review all three raw run measurements, variance, MCP latency, sampled RSS, context bytes, four baseline byte comparisons, exact task fidelity, protected-output scan, checkpoint pruning, and restart equality. Do not convert a passing engineering target into a public performance claim.
- [ ] Test broker interruption, process restart, strict corrupt-latest refusal, checkpoint-directory contention, slow MCP reading, and rollback using authorized non-production data.

## Operations and acceptance

- [ ] Install an external process supervisor with restart throttling; Decant does not restart itself.
- [ ] Capture stderr diagnostics in an access-controlled bounded sink and alert on exit, `kind:fault`, rejected records, checkpoint failure, capacity pressure, and repeated restarts. Never log MCP stdin/stdout or source records through generic request tracing.
- [ ] Define pilot availability, resource, data-gap, rollback, evidence-retention, and incident-owner expectations with the design partner. Do not promise exactly-once processing or complete source coverage.
- [ ] Rehearse stopping intake, preserving checkpoint evidence, rolling back the binary/config together, and choosing a fresh checkpoint namespace when an incompatible semantic change is explicitly accepted.
- [ ] Confirm the design partner accepts local stdio MCP, one configured view, static partitions, manual epoch rollover, local checkpoint sensitivity, and every item in `KNOWN_LIMITATIONS.md`.

## Commit-bound approval record

Copy `release/kafka-mcp-pilot-evidence-v0.example.json` to the untracked path `release/kafka-mcp-pilot-evidence-v0.json`. Replace the source commit and benchmark SHA-256, set every approval to `true` only after its evidence is reviewed, and serialize canonical compact JSON. Do not include customer names, broker identifiers, paths, credentials, source values, or private evidence.

Then run:

```sh
./scripts/kafka-mcp-pilot-gate-v0.sh --production-pilot
```

A PASS is permission evidence for the one approved pilot boundary. It is not a license decision, public-release approval, compatibility matrix, SLA, security/privacy certification, or general production-readiness claim.
