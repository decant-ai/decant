# Security Policy

## Supported version

Only the latest commit of the `0.1.0-alpha.1` source alpha receives security fixes. There is no stable or production-supported release yet, and signed binary distribution is deferred.

## Report a vulnerability

Do not include event data, credentials, private checkpoint content, or exploit payloads in a public issue. Use GitHub private vulnerability reporting after it is enabled for the public repository. If that channel is unavailable, contact the repository owner privately through their GitHub profile before sharing details.

Do not test against systems or data you do not own or have explicit permission to use. There is no bug-bounty program, response-time SLA, safe-harbor promise, or production support commitment at this stage.

## Current security boundary

- Decant executes locally by default and does not require an LLM or hosted Decant service.
- Kafka network access occurs only for explicit Kafka commands. Static assignment supports trusted plaintext, verified TLS, or TLS plus SCRAM-SHA-256/512; credentials are resolved from bounded environment references.
- Kafka egress is explicit allow/deny and server-selected, but admitted state/checkpoints can contain mapped denied values. Local checkpoint storage must be protected as source-sensitive data.
- MCP is a bounded local stdio boundary without remote authentication. Each command exposes only its configured report/view and selected references; live evidence additionally exposes separate status. Agents cannot choose raw files, policies or payload fields.
- Evidence version/service labels are untrusted data. Markdown escapes metacharacters/control text, but neither escaping nor field omission is prompt-injection immunity. Captures/checkpoints remain sensitive. Keep reports within the configured egress policy and review them before sharing.
- Parsers, configs, checkpoint files, source records, and future API inputs must be treated as untrusted.
- Hashes in current outputs/checkpoints are deterministic receipts, not signatures, MACs, authentication, or tamper-proof evidence.

## Non-guarantees

This preview does not claim exactly-once delivery, complete secret or PII detection, complete anonymization, privacy compliance, authorization, encryption at rest, protection against a compromised host, a supported dependency vulnerability posture, or production readiness. Review [known limitations](docs/release/KNOWN_LIMITATIONS.md) before testing.

The repository hygiene tool checks a small set of high-confidence signatures in the current tracked tree. It is not a full-history secret scanner. A separately approved full-history scan and manual review are required before publication.

Candidate SHA-256 checks and the Cargo build-graph SBOM establish limited byte
consistency/inventory, not origin, cryptographic trust or a vulnerability-free
native stack. Use only a separately approved signing identity/verifier. The
deferred signed-binary public gate requires actual signature bytes and a reviewed verification receipt;
it does not independently prove the receipt's truth. Do not run an untrusted
binary to verify its origin, disable OS security checks, or share private review
records in public issues. Author-independent security/correctness review remains
a release gate even when all tests pass. This source-only alpha provides no
signed binary download and makes no signing or production-certification claim.
