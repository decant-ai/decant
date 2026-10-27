# Investigation evaluation records

No head-to-head result is published. This Unix offline tool validates record consistency;
it does not run agents, authenticate measurements, certify fairness, or approve a
competitive claim. Its unit tests use synthetic records, not benchmark results.

## Register before tuning

`python3 tools/investigation_evaluation_v1.py template` prints an **incomplete**
registration template. Keep actual registrations, inputs, expected facts, settings,
raw answers and reviews outside Git in access-controlled storage. Templates are
not completed preregistrations. A reviewer must freeze and timestamp the completed
registration before tuning or collecting held-out results; this tool cannot prove
when a file was created or whether a person has already seen held-out data.

The matrix declares 60 distinct cases: 30 development and 30 held-out, three per
split for local latency, downstream latency, error increases, traffic mix,
version-only changes, healthy periods, missing baselines, partial backends,
conflicting evidence, and out-of-scope abstention. Supply actual datasets and
independently authored expected facts for each slot. Do not generate incidents
solely to favor Decant. Repeated byte-identical input/expectation pairs cannot
fill different slots. Inputs from other organizations require their authorization.

Register five baselines: Decant with reasoning, well-configured HolmesGPT, the same
model with Grafana MCP, a competent engineer's query/runbook workflow, and Decant
without a model. Freeze exact source/binary/tool versions, commands, model settings,
access permissions, budgets, machine/resources and warm-up/order rules in the
environment, fairness-protocol and per-comparator configuration artifacts. Keep
native integrations and normal context management enabled. Use both equal-budget
and normal-default regimes. Human and deterministic baselines run once per held-out
case/regime; each model agent runs five times, totaling 1,020 held-out trial records.
Development runs and tuning histories stay in a separate retained record set.

## Local objects and schemas

Every artifact reference is a lowercase SHA-256 of its exact bytes, stored at
`OBJECT_DIRECTORY/HASH` (no extension). Files must be regular, single-linked and
not symlinks. The tool reads but never modifies them. Store credentials separately;
neither capture bundles nor raw answers are automatically safe to share. The
directory must be operator controlled; this is not protection against a hostile
same-user filesystem race. Maximum object size is 64 MiB; JSON is capped at 8 MiB
and depth 24; verification reads are capped at 16 GiB and 20,000 distinct objects.

Fill every template reference with the hash of a real artifact, pin the exact
model identifier and source commit, and change status to `registered`. The
evaluator's own source hash is bound too. Changing its semantics or registration
requires renewed review, not silently reusing old trial identities.

```sh
python3 tools/investigation_evaluation_v1.py registration --registration REGISTRATION.json --objects OBJECT_DIRECTORY
python3 tools/investigation_evaluation_v1.py summarize --registration REGISTRATION.json --ledger LEDGER.json --objects OBJECT_DIRECTORY
```

The ledger is `{format: "decant-investigation-evaluation-ledger", version: 1,
registration_sha256: HASH, trials: [...]}`. Each trial has exactly:

- `case`, `arm`, `regime`, `repeat` (one-based), and `status` (`completed`, `failed`,
  or `timeout`). Preserve every failed attempt. Never rerun until success and
  substitute it for the registered trial.
- `answer`: the raw answer/failure artifact hash, even if empty.
- `measurement`: hash of JSON containing nonnegative integer `wall_ms`, `cpu_ms`,
  `peak_rss_bytes`, `disk_bytes`. Measure the entire invocation, including all
  tool/model calls, retries, compaction and waiting; record method/limitations.
- `review`: hash of JSON containing opaque `reviewer`, `rating`
  (`correct_supported`, `correct_abstention`, `incorrect`, `unscorable`), and
  nonnegative `fabricated_measurements` and `invalid_references`. Correct ratings
  cannot coexist with failure or fabricated measurements/references. Review
  blind where practical; the tool does not decide whether evidence entails a claim.
- `backend_calls`: ordered receipt hashes for every query, including failures.
- `model_calls`: ordered records with request/response artifact hashes and integer
  `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`,
  `cost_micro_usd`. Use **null** for unknown usage/cost, never zero. Account for
  every billable call, including compaction, retries and cached-token billing.
  Failed calls still get records with unknown usage where necessary. Use provider
  billing evidence, not a guessed token price. Human/deterministic arms have no
  model calls. Human labor cost is not measured by the model-cost field.

Malformed, duplicate, missing, relabelled or cross-registration trials fail closed.
The summary retains per-scenario repetitions, resource observations, failures,
ratings, usage and unknown costs. It does not pool repetitions into independent
samples or issue a superiority verdict. Registration and ledger identities are
byte identities; ordering changes the ledger identity even when grouped facts match.

## Comparative analysis and external gates

Before trials, register the proposed diagnostic non-inferiority margin (five
percentage points), time-to-correct target (30% lower median), total model cost
per correct investigation, zero fabricated measurements/invalid references,
and 90% correct abstention target. These are **hypotheses, not measured results**.
Define scenario-level uncertainty, failed-run treatment, censored times, scoring,
sample sufficiency and stopping rules with an independent reviewer in the fairness
protocol. Do not drop unsuccessful runs when calculating cost per correct result
or hide failure rates behind success-only time summaries. Unknown costs prohibit
a cost conclusion. Inconclusive statistical evidence stays inconclusive.

Actual authorized trials, competent baseline review, independent reproduction,
pilot preference/repeat use and explicit claim approval remain pending. No paid
model calls, comparator installations, customer data access, signatures or
publication are performed by this tool.
