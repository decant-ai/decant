# Contributing to Decant

Decant is a source-alpha project. Before proposing a change, read the supported scope and known limitations. Security reports belong in the private channel described in [SECURITY.md](SECURITY.md), not a public issue.

This repository is the canonical home of Decant's open-source core. Discuss use
cases, backend/agent compatibility and synthetic reproductions in issues.
Maintainer changes use focused branches and reviewed pull requests, not direct
pushes to main. Keep private telemetry and business records out of the repository.

## Development workflow

1. Use Rust 1.98.0, Python 3.11+ and the native C/C++/make/Perl toolchain; Linux CI installs the system `librdkafka` development package and its platform prerequisites.
2. Fetch the locked Cargo graph once with `cargo fetch --locked`.
3. Keep changes small, deterministic, bounded, and covered by failure tests.
4. Run `./scripts/check-v0.sh` before opening a pull request.
5. Describe public or persisted contract changes and their compatibility impact.

Do not include private data, credentials, proprietary code, or employer-confidential material in issues, fixtures, tests, or pull requests. Use unmistakably synthetic examples.

## Current contributor-policy status

The project licenses and exact file boundaries are in [LICENSING.md](LICENSING.md).
No CLA or DCO mechanism is selected. Maintainers must not solicit or merge external
code contributions until the contributor ownership strategy is separately
approved and this document is updated. Do not infer commercial relicensing
rights from a contribution. This maintainer workflow hold does not add terms
to, or override, the installed licenses.

This is an operational boundary, not legal advice.

## Useful first contributions after those gates

Start with a small synthetic regression fixture, a capture-format diagnostic,
an own-data documentation correction or a reproducible MCP interoperability
test. Describe expected facts independently of the implementation. Keep input,
service cardinality, temporary state and output bounded. Do not add new
protocols, inferred causal claims, model calls or a dashboard to a bug-fix PR.

For activation feedback, record OS/version, command, synthetic reproduction,
expected result and where the documentation stopped helping. Never attach a
real production capture or checkpoint. Feedback is opt-in; the product adds no
usage telemetry. Maintainer availability and support are best effort, not a
24×7 SLA. No maintainer response-time commitment is offered at this stage.
