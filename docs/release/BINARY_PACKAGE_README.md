# Decant evidence compiler — binary candidate

Give your AI agent the evidence behind a production regression. Decant compares
observed server spans, produces a bounded report with source references and
explicit gaps, and works locally without an LLM, Kafka or a Decant account.

This is an experimental `0.1.0-alpha.1` candidate, not a production certification.
Check its source commit and target in the separate `manifest.json`. Checksums
establish byte consistency, not identity. Only run a binary whose source and
origin you trust. A release signature requires the separately approved verifier,
identity policy and release evidence; the builder does not sign. If this package
contains the legacy `LICENSE-MISSING.txt`, no project license was included.
Current licensed candidates include `LICENSE`; this does not constitute
maintainer approval to publish or a signature.

## Project license

Copyright (C) 2026 Eshan Vaid. The original Decant binary, this document and
bundled synthetic capture/configuration are licensed under the GNU Affero
General Public License, version 3 only (`AGPL-3.0-only`); see LICENSE for its
terms and warranty disclaimer. The optional or-later template in that license
document is not adopted. Third-party components retain their own terms.
The matching source archive includes LICENSING.md and separately scoped Apache
configuration examples; those exceptions are not bundled in this binary archive.

## Try the bundled synthetic capture

From this extracted directory, use unused output names to preserve older reports:

```sh
./decant evidence demo
./decant evidence analyze --file trace-capture.jsonl --config evidence-config.json > report.json
./decant evidence render --file report.json
./decant evidence verify --file report.json
```

No Rust or Python runtime is required for these CLI commands. A compatible
operating system and host/native libraries are still required; this is not a
universal or necessarily statically linked binary. Run `./decant --version` and
the demo on each intended host before using authorized data. Do not bypass OS
security warnings or execute a random download to test its checksum.

## Analyze your own data

Replace `trace-capture.jsonl` with a completed, uncompressed Collector file-export
OTLP trace JSONL capture. Review the configuration first. Service names are
allowed, version labels withheld by default; arbitrary attributes, span names
and IDs are not exposed by the report. This is not anonymization. Traces,
configuration and local checkpoints can contain sensitive information.

Input is finite and ordered as captured, not automatically sorted. SERVER spans
have numeric kind 2. The default latest five-minute interval is compared with
its preceding five minutes, anchored to recorded event time. Sampling remains
unknown; missing baseline/version data is not inferred. p95 is an exact nearest
rank over observed spans, not a whole-production SLO. Version observations do
not establish deployment time or causality. Read report gaps and omissions.

Limits include 4 MiB/line, 256 MiB/file, 100,000 records/accepted spans, 256 service
groups and 16 KiB JSON output. Unsupported/malformed input fails with value-safe
errors. A growing capture must be closed first. No live OTLP receiver is included.

For local MCP, configure your client's absolute executable path with arguments
`evidence serve --file /absolute/path/report.json`. This reads/verifies the report
once and exposes only snapshot and exact-report explanation. Stdout is MCP only.
Do not treat telemetry-derived strings as agent instructions. Ctrl-C/SIGTERM on
Unix after readiness stops the service; leave any Kafka checkpoint lock file in
place. SIGKILL or a failed shutdown requires strict verified-cut recovery.

`./decant evidence --help` lists Kafka evidence, validation and other commands.
The matching source archive includes the complete own-data, Kafka, MCP,
supported-scope, limitations, security and rollback guides under `docs/release/`.

## Package inventory

`SBOM.cdx.json` is a CycloneDX 1.6 build-graph SBOM from `Cargo.lock`. It includes
development/target-specific Cargo packages and source checksums, not proof of
exact linked components. Declared license strings are informational. Bundled
native projects and host libraries require separate review; the composition is
explicitly incomplete. No vulnerability-free, license compatibility, privacy,
exactly-once or general production-readiness conclusion is implied.
