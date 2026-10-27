# Investigation beta evidence checklist

Engineering checks are not independent user validation or a competitive result.
This offline tool checks record completeness and consistency for exact native
candidates. It **never authorizes publication**, signs artifacts, contacts users,
runs models or sends usage telemetry. Project licensing, current public-source
availability and the existing full-build release contracts are unchanged.

External evidence remains pending: registered comparative trials, independent
own-backend users/repeat use, author-independent review, native/platform review,
signing and exact release/claim approval. Synthetic tests are not those outcomes.

## Assess locally

`python3 tools/investigation_beta_v1.py template` prints an incomplete template.
Keep actual records in private ignored storage, not public source. Use the
content-addressed object layout from [evaluation records](INVESTIGATION_EVALUATION.md).
Never put credentials, customer names, hostnames or payloads into metadata.

Build/verify [investigation candidates](INVESTIGATION_BINARY.md) for macOS arm64
and Linux x86_64 on one clean source commit. Assessment rechecks bytes without
executing the binaries. One platform cannot stand in for the other:

```sh
python3 tools/investigation_beta_v1.py assess --candidate MACOS_CANDIDATE --candidate LINUX_CANDIDATE --records PRIVATE_RECORDS.json --objects OBJECT_DIRECTORY
```

The output gives the candidate-set SHA-256 (canonical sorted target-to-manifest
hash map) and source commit. Bind real records to these values. Adding/changing a
candidate invalidates old bindings. Omit unavailable candidates to see blockers;
do not invent their files.

- Exit **1**, `NO-GO`: valid but incomplete/failed evidence, with fixed blocker codes.
- Exit **2**: malformed, corrupt, substituted or stale records; fixed safe error.
- Exit **0**, `FOUNDER_REVIEW_REQUIRED`: records are consistent, **not** authenticated,
  signature-verified, release-authorized or production-certified. Output fields
  `publication_authorized` and `authenticity_verified` always remain false.

The `decant-investigation-beta-records` V1 input has `format`, `version`,
`source_commit`, `candidate_set_sha256`, `reviews`, `activation` and `comparison`.
Null references stay pending. Unknown keys fail. Bounds: 64 KiB metadata, two
candidates, 20 participant records and 16 supporting references per review/attempt,
plus shared evaluation object/read limits. No file is pruned or approval generated.

## Review records

Each `reviews` value references JSON with exactly `format:
"decant-investigation-review"`, `version: 1`, `source_commit`, `candidate_set_sha256`,
`scope` (table key), opaque `reviewer`, boolean `independent`, `status`
(`pending`, `passed`, `failed`), and `evidence` (supporting artifact hashes).
Passed reviews require nonempty evidence; hash consistency cannot establish truth.

| Scope | Required human-reviewed evidence |
| --- | --- |
| `quality_full` | Exact source, canonical gate commands/results, unresolved failures |
| `quality_investigation` | Lean graph/tests, both native builds, copied-binary/replay/lifecycle checks |
| `independent_security_correctness` | Author-independent bounds, leakage, query/egress/replay review and resolution of blocking findings |
| `native_dependency_review` | Selected graph, incomplete all-lockfile SBOM, native/system linkage, advisories, supported runtimes |
| `clean_install_linux` | Independent unassisted installation and own-backend workflow on Linux |
| `clean_install_macos` | Independent unassisted installation and own-backend workflow on macOS |
| `rollback` | Trusted previous binary, backup/restore and bundle compatibility; no destructive lock deletion |
| `comparison_analysis` | Independent frozen-workload analysis, competent baselines, all failures/usage, scenario-level uncertainty and preregistered diagnostic/time/cost/abstention targets |
| `signing_platform_review` | Approved custody/trust identity, actual detached signatures and external verification for both exact binaries, OS acceptance and withdrawal/rotation procedure |
| `publication_review` | Exact commit/hashes, destination, distribution scope and approved wording; no unmeasured superiority or universal production claim |

Security/correctness, clean-install and comparison reviews must declare independence.
An editable declaration is not proof. Inspect the real evidence: this tool does
not establish statistical validity, reviewer independence, baseline competence or
cryptographic trust. Signing review refers to detached signatures over the exact
unsigned candidate binaries; it does not change their build manifest. The tool
does not run signing software or verify signatures itself.

`comparison` contains `registration` and `ledger` object hashes. Assessment
revalidates the [complete registered trial set](INVESTIGATION_EVALUATION.md), binds
its source commit, and rejects missing trials, unknown cost or recorded fabricated
Decant measurements/invalid references as a successful gate. It does not compute
non-inferiority. Inconclusive independent analysis must not be marked passed.
Review permissions, model parity, preregistration timing and all retries/compaction.
The comparison analysis review must reference these exact registration and ledger
hashes plus an analysis artifact; a review of another run set cannot be reused.

## Independent activation, with consent

Observe installation, metric mapping, the first own-backend report, missing checks
and stopping points. Retain failures and undocumented assistance. Ask participants
to return for a real investigation and whether they prefer doing so with Decant.
Scripts, CI, the author, duplicated identities and invented users do not count.

Each `activation` reference is JSON with `format:
"decant-investigation-activation"`, `version: 1`, the source/candidate-set binding,
opaque `participant`, `target` (one advertised target triple), `evidence`, and
booleans `consent_confirmed`, `independent`, `assisted`, `installed`,
`own_data_completed`, `repeat_investigation`, `prefers_another`.

All attempts remain in totals. Only consenting, independent, unassisted attempts
count toward five own-data completions, three repeat users and three preferring
another supported investigation. At least one completion must be on each platform.
Duplicate participant IDs fail; re-identifying the same person remains dishonest
and needs human review. Keep friction notes private and avoid copying telemetry.

If users cannot map their metrics or existing tools are easier, retain that result
rather than changing scoring. Complete records go to the founder for exact
distribution/claim review, never an automatic release.
