# Evidence evaluation, not a superiority claim

The checked-in V1 specification is a **preregistration**, not a result. It
freezes 14 semantic/holdout cases and equal-input, eight-service load workloads.
The independent Python/SQLite reference reads raw capture JSON, uses SQL window
ranking for exact p95, handles the documented admission/dedup scope, and emits
bounded facts with references. It is a competent aggregation baseline, not raw
dumping. It lacks Decant's policy/checkpoint receipts and is not a production
input adapter. Output bytes are not model tokens or total investigation cost.

Run without Kafka, a Collector, an LLM, network services or added Python packages:

```sh
cargo build --locked --offline -p decant-cli --bin decant
python3 tools/evaluate_evidence_v1.py spec
python3 tools/evaluate_evidence_v1.py correctness
```

The canonical repository gate runs specification/correctness checks only. The
resource experiment is deliberately opt-in on Unix with Python's bundled SQLite:

```sh
cargo build --locked --offline --release -p decant-cli --bin decant
python3 tools/evaluate_evidence_v1.py run --output .local/evaluation-run-1 --machine-notes .local/machine.json
python3 tools/verify_evidence_evaluation_v1.py .local/evaluation-run-1/result.json
```

Create the machine record yourself with exactly `cpu` (string), `ram_bytes`
(positive integer), `storage` (string) and `filesystem` (string), using observed
host details. Do not put identifiers, credentials or confidential descriptions
in it. The runner records OS/kernel/architecture, Rust/Python/SQLite versions,
source/binary/lock/evaluator/spec hashes, UTC observation time, input hashes,
bytes, warm-ups, every measured trial, CPU, per-child peak RSS and variance.
Production sources must match the committed build; build the release binary
immediately before running. Keep machine notes and results in ignored storage.

Each size/implementation has one separate warm-up and three fresh-process
trials, with alternating order. A fresh measurement wrapper launches exactly
one child, so cumulative child resource accounting is not mislabelled as a
per-process peak. Process startup is included; compilation is excluded. Host
load, cache and CPU scaling are not controlled laboratory conditions. Storage
model details not collected must be disclosed before any published comparison.

The verifier checks actual frozen input bytes, complete trial sets/order,
unmodified raw stdout/stderr/resource observations, replay, independently
recomputed facts and summaries. It rejects missing/failed/relabelled trials or
changed evaluator/binary identities. This checks consistency, not authenticity
of edited measurement files. Independent reproduction remains necessary.

Expected refusals, unknown sampling, missing evidence and output omissions are
part of the score. Do not drop cases or silently loosen resource/scaling limits.
Changing workload semantics requires a new specification version. Retain failed
attempts and raw artifacts. No measured numbers belong in README without maintainer
approval and independent reproduction.

The correctness checks are not a published performance result, and no
throughput/superiority claim is made.
External configured Grafana/MCP and SRE-agent workflows, independent service
captures, human investigation time, total model input across all calls and
repeat-use/customer value have **not** been evaluated by this harness. They are
explicit external launch/utility gates, not automatically successful results.
