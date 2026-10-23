# Investigation documents and private replay

The internal application library composes captured Prometheus, Tempo and Loki
evidence into `decant-investigation` version 1. This is an evidence-only report,
not an LLM diagnosis. These APIs are not a stable Rust SDK and are not exposed
as a new CLI command. Existing `evidence` commands/formats are unchanged.

One configured Grafana profile and frozen comparison window bind all recipes.
Acquisition uses a single bounded session; at most 26 requests are planned.
Metrics are required; optional signals remain explicitly unattempted when not
scheduled. No shell, backend write, model call or implicit credential lookup is
part of report generation/replay. The responses are not an atomic snapshot.

## Report and rendering

JSON contains projected facts, exact query/response identities, recipe/policy
identities, source cuts, coverage and explicit omissions. Coverage maps opaque
probe IDs to signal/request indices and preserves successful empty, partial,
failed and unattempted observations. Response coverage is **not** measurement
adequacy, service health or population completeness. Read the qualifications
and gaps within each signal's facts.

Markdown summarizes numeric measurements and includes the same complete JSON
document as escaped data. Permitted telemetry text is not agent instructions.
HTML escaping does not prove prompt-injection immunity or remove every secret.

Both representations have a 64 KiB ceiling. Smaller configured budgets must be
at least 4 KiB. Whole fact sections are packed in metrics/Tempo/Loki order only
when both representations fit. Omitted sections retain identity and coverage
with a `report_budget` reason. If mandatory metadata cannot fit, generation
fails; it never silently drops coverage or truncates a string. Changing the
budget, policy, cut or capture changes the report identity.

## Private bundle format

`ReplayBundleV1` stores a fixed magic prefix, a four-byte big-endian manifest
length, canonical JSON manifest and ordered raw response frames. Manifest cap:
64 KiB; at most 26 frames, 4 MiB each, 64 MiB total. No compression, archive paths
or extraction. The manifest records frame lengths/hashes, closed failures,
attempt flags, sanitized configuration and expected report identity. Credential
values, credential environment references and CA file paths are not persisted.

**Raw captures can contain sensitive telemetry, backend addresses and query
scope, even when the report omits them. Do not share bundles as reports.**
Hashes and default field exclusions do not establish anonymity.

Decode requires an independently retained expected bundle ID, checks framing,
canonical schema, lengths/hashes and references, then recomputes the report
with no network or model access. Report verification checks the expected report
ID and byte equality against that recomputation. A requery is a new acquisition,
not replay. These checks detect corruption/substitution relative to trusted
expected IDs; they do not prove source truth or resist replacement of all trust
anchors by an attacker.

## Managed local storage

The library's store supports cooperative local Unix filesystems. It creates a
private directory (0700), bundle files (0600), and a persistent advisory writer
lock. The quota is 256 MiB and 64 bundle entries. Symlinks, nonregular/linked
files, unsafe permissions and unrecognized entries are refused. Store reads
require an exact bundle ID; no arbitrary member paths exist.

Writes are create-new and synchronize file and directory before success.
Existing identical verified content is idempotent. Interrupted/partial files
remain counted, fail verification, and are never overwritten or silently
removed. Full stores refuse new writes; there is no automatic pruning or
corruption fallback. Retain a separate report and its expected IDs, and review
storage deliberately before moving/removing any private evidence.

Do not delete the lock file to bypass a running owner. Drop/stop the existing
owner and reopen normally. This is not a hostile-same-account or path-race
security boundary. Network filesystems, physical disk-full/power-loss behavior
and Windows storage have not been certified.

Tests cover synthetic local TLS acquisition, offline replay, tampering, policy
exclusion, quotas and process ownership. Real Grafana/Tempo/Loki deployment
interoperability and independent end-user activation are not established by
these tests.
