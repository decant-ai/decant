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

## Ask a question

After configuring the profile, use the additive question workflow:

```sh
decant investigate ask --profile production --question "Why is checkout slower?" --model YOUR_MODEL_ID --model-key-env ANTHROPIC_API_KEY --allow-model-egress
```

Use your chosen model ID, not the placeholder. `ask` needs no `--reason` flag.
The confirmed profile and `--since`/`--until` determine scope and time; a question
cannot override them. Review the confirmed scope printed in the answer. Without
`--allow-model-egress`, an interactive terminal must confirm; scripts fail before
model/backend calls. Your question (at most 1024 UTF-8 bytes, no controls) is sent
to Anthropic. Do not include credentials or sensitive details, including in shell
history. The question is not copied to reports, bundles or diagnostics.
Profile service/environment names are not added to the model input. The planner
therefore cannot verify that a service named in your question matches the profile;
select the correct profile yourself. Names you write in the question are sent.

There is one planning call and at most one explanation call, never an intermediate
model call or automatic retry. The planner selects latency or selected-error
regression, or declines/requests clarification without querying the backend.
The deterministic middle obtains the configured metrics. An observed increase or
unknown comparison schedules configured Tempo/Loki recipes. Otherwise those reads
are visibly skipped; that is not proof of health or absence of a localized problem.
Matching trace/log evidence already in the selected bundle is still inspected,
even when there is no aggregate increase; skipping applies only to new reads.
No new sources, arbitrary queries, scope expansion, traffic-mix or version checks
are introduced. Tempo is sampled evidence; Loki counts do not explain log contents.

The new `decant-investigation-answer` V1 JSON (`--format json`) records the recipe,
branch decisions, qualified facts, omissions, evidence identities, token/cache
usage and separately labelled model explanation. Reference checks and direct
scalar checks do not verify the truth of model prose. Markdown escapes that prose;
no commands or links from it are executed. Full evidence remains accessible via
`investigate report --bundle ID`. No model answer is needed to replay the facts.

Metrics are saved before additional reads; each expanded cut is saved before
continuing, and the final cut before explanation. Receipts are printed to stderr.
Failure/cancellation after publication does not remove earlier cuts. Stores remain
quota-limited; up to three immutable bundles may be published, with no pruning.
Final model failure returns evidence with exit 1. Unsupported/clarification results
also exit 1, without a new bundle. Exit 0 means answer produced, not diagnosis proved.

To explicitly reuse existing evidence, supply `--bundle ID --until EPOCH_SECONDS`
and a matching `--since`. The origin, scope, metric mapping, interval and any reused
trace/log policy must match. Existing responses, including failures, are not
retried. Missing selected sections may be acquired for that same interval; those
responses are not an atomic snapshot with the old ones. The old bundle remains
unchanged. To obtain a fresh interval, omit `--bundle` and run a new investigation.

Limits remain 32 backend requests and five minutes overall. Reserve 128 KiB of
the 64-MiB response allowance for the two model responses (64 KiB each). Planning
HTTP bodies are capped at 8 KiB/512 requested output tokens; explanation bodies at
24 KiB/2048 requested output tokens. The evidence projection is at most 16 KiB,
with whole-view omissions explicit. These byte/call bounds do not prove lower
tokens, cost or equal diagnostic usefulness. Actual provider compatibility and
comparative results remain unverified by the synthetic tests.

## Read the deterministic result

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
