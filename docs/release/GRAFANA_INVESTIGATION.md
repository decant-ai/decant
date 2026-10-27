# Investigate your existing Grafana data

Decant compares one service's latency and selected-error measurements against
the preceding equal-length interval. It produces a readable report with observed
changes, supporting evidence and explicit gaps. Optional model reasoning assesses
a bounded set of hypotheses; it cannot execute fixes or arbitrary queries.

This is a source-alpha workflow. It requires HTTPS Grafana with a readable
Prometheus-compatible datasource, a request counter and a **classic histogram
in seconds**. Tempo and Loki are optional. Native histograms, arbitrary schemas,
traffic-mix attribution and version comparisons are not supported by this path.
Synthetic tests do not certify your deployment or a particular provider model.

## Build and connect

Follow the [investigation-only build prerequisites](INVESTIGATION_BINARY.md).
From the repository root:

```sh
cargo fetch --locked
cargo build --release --locked --offline -p decant-cli --bin decant --no-default-features
```

Provide a Grafana token through your normal secure environment mechanism. Do not
put its value in command arguments, profiles or public issues. Then:

```sh
./target/release/decant connect grafana --url https://grafana.example.com --token-env GRAFANA_TOKEN --profile production
./target/release/decant investigate configure --profile production
./target/release/decant investigate --profile production --since 15m
```

Replace the URL with your own authorized instance. Connect asks you to confirm
datasource IDs and exact service/environment labels and values. See [connection
permissions and TLS](GRAFANA_SETUP.md). `doctor --profile production` checks
query access; it does not validate the semantics of a metric you choose.

Configure asks for:

- A monotonically increasing request counter, not a rate or summary.
- Its error-selecting label and exact comma-separated values, such as `500,503`.
  These are explicit values, not an arbitrary regex.
- A classic histogram bucket metric ending in `_bucket`, whose bucket values
  you explicitly confirm are seconds.
- Whether to include configured Tempo traces and Loki counts. Loki needs its own
  indexed error label and exact values. This CLI never requests raw log bodies.

Use metric names and labels from your own instrumentation/Grafana queries. There
is no universal mapping to guess. A missing metric produces a gap, not zero.
Setup is saved create-new in `.decant/profiles/production.investigation.json`;
profiles/settings contain references and mappings, not tokens. To change mappings,
create a new named profile/settings pair; existing settings are never overwritten.

Noninteractive setup is available with `--yes` and all required mapping flags;
run `investigate configure --help`. Optional sources default off with `--yes`
unless selected explicitly. No JSON hand-editing is required.

## Read the result

The default Markdown includes before/after metrics, observed directions, all six
mandatory checks, missing checks, qualifications and evidence identities. Exact
JSON is available with `--format json` (`decant-investigation-run` V1).

`--since` accepts `1m` through `60m`, default `15m`. Relative time is frozen once;
the baseline immediately precedes the selected interval. `--until` optionally
accepts integer Unix seconds. `--service` and `--environment`, if given, must
match the confirmed profile rather than silently expand scope.

The receipt on stderr includes the saved bundle ID. Keep it: the bundle is
verified using that expected identity. Bundles may contain sensitive backend
responses. They are **not** shareable reports or anonymous artifacts.

## Revisit and verify without querying production

Replace `BUNDLE_ID` below with the complete `sha256:v1:...` receipt:

```sh
./target/release/decant investigate follow-up --bundle BUNDLE_ID --focus errors
./target/release/decant investigate report --bundle BUNDLE_ID --format json
```

Both verify and replay saved evidence without Grafana access, a profile, or
credentials. `report` renders the original `decant-investigation` V1 evidence,
including its query receipts and qualifications. Follow-up focuses the assessment
on `overview`, `latency`, `errors`, `dependencies` or `gaps`. It is a fixed-focus
workflow, not an unrestricted conversational agent. Offline replay reproduces
facts, not a model's answer. Review any exported report before sharing it.

To acquire a new cut explicitly:

```sh
./target/release/decant investigate refresh --bundle BUNDLE_ID --profile production --since 15m
```

Refresh requires a strictly later cut and the same confirmed service/environment.
It retains the old bundle and reports the parent report ID. It does not update
old evidence in place. A follow-up never silently refreshes.

## Optional model reasoning

Deterministic investigation needs no model account. To authorize reasoning,
select a model yourself and make its API key available through an environment
reference. This example intentionally has no default model:

```sh
./target/release/decant investigate --profile production --since 15m --reason --model YOUR_MODEL_ID --model-key-env ANTHROPIC_API_KEY --allow-model-egress
```

`--allow-model-egress` explicitly permits external, potentially billable calls.
Without it, an interactive terminal must confirm; scripts fail before acquisition.
Only numeric facts, closed statuses and opaque IDs are sent—not source addresses,
service/environment values, metric/query names, trace names or log text. This is
not an anonymity guarantee. See [provider limits](MODEL_ACCESS.md).

Each authorized invocation, including a model-assisted follow-up, gets at most
six calls. There is no automatic retry or model routing. Known token categories
and unknown-usage calls are reported; this is not a billing guarantee. Model
interpretations remain separate from facts. Invalid/refused/timed-out results
leave evidence available and return a non-success reasoning status.

## Storage, limits and exits

- One active investigation per CLI process; one cooperative writer per store.
- At most 32 backend requests, six model calls and five minutes for acquisition
  plus reasoning. No retries. The current enabled recipes need at most 24 reads
  (ten metrics, up to ten Tempo, four log-count requests).
- Backend responses are capped at 4 MiB; total acquisition/model response data
  is capped at 64 MiB. A transport chunk crossing the limit is counted and
  rejected, not retained. Model responses have the smaller 64 KiB cap.
- Rendered stdout is at most 64 KiB. Network and output waits are bounded;
  filesystem stalls and interactive input are not a universal hard deadline.
- Default store `.decant/investigations`: private local Unix files, 64 bundles
  and 256 MiB per store. `--store-dir` selects an explicit alternative. Full or
  corrupt stores fail closed; there is no silent deletion, overwrite or fallback.
- Ctrl-C/SIGTERM cancels pending network work. If acquisition already published
  a bundle, its receipt remains usable even when reasoning is interrupted. Do
  not delete `writer.lock` to bypass another running process.
- Exit `0`: report produced, **not** service healthy or complete evidence.
  Exit `1`: operational failure or incomplete optional reasoning (stdout may
  still contain the preserved report). Exit `2`: invalid usage. Exit `130`: signal.

See [evidence and replay](INVESTIGATION_REPLAY.md) and
[investigator semantics](INVESTIGATOR.md) for source-cut, sampling, coverage and
interpretation limits. No comparative diagnostic or cost result is claimed.
