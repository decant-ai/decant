# Kafka evidence V1

Use `decant evidence serve-kafka --config /absolute/path/config.json` as the
stdio command launched by your MCP client. Start from
`examples/evidence/kafka-config-v1.json`; it is a reviewable template, not a
ready-to-use production connection. Consult the existing Kafka/MCP guides for
authorized broker access, TLS/SCRAM environment references, cluster/topic IDs,
static partition ownership and checkpoint permissions. Never delete a lock file
to restart. One process owns one checkpoint directory; never commit credentials.

This path expects flat JSON span records, not nested OTLP envelopes. Adapt an
authorized stream explicitly at its boundary. `kafka-spans-v1.jsonl` is a tiny
synthetic example matching the supplied projection. Map lowercase nonzero
32/16-hex trace/span IDs, numeric kind/status, integer duration nanoseconds and
source event time. SERVER kind is 2; status ERROR is 2, UNSET is 0. Service name
and version are optional projections, not inferred from other attributes.
When mapped they must be present and match the configured scalar type on each
record. Omit version mapping if unavailable. Review whether service labels may
leave the local host; version labels default to denied.

The source namespace, mapping, recipe, policy and output budget bind checkpoint
identity. Change them only with an explicit new checkpoint directory/source
replay decision. Latest checkpoint corruption or incompatibility fails closed;
there is no automatic fallback, takeover, group rebalance or broker offset commit.

The live server offers three read-only tools:

- `decant_evidence_snapshot`: the latest verified checkpoint's report, or
  unavailable before the first cut/after a fault.
- `decant_explain_evidence`: a selected reference within the exact requested
  report ID. It does not fetch raw spans.
- `decant_evidence_live_status`: report ID, generation, pending records and
  operational observation ages. Compare IDs across calls; these ages do not
  establish source recency. Pending records are not yet evidence.

Compilation occurs at count/idle/shutdown cuts, not on each record. Tune cadence
against state capacity and measured cut costs, not an assumed throughput claim.
Capacity, malformed records and conflicting logical spans fail the new service
before persisting that suffix. Existing late admission exclusions remain explicit
in report gaps. Recovery replays from the previous verified frontier.

For an opt-in isolated acceptance check, build the CLI with locked dependencies,
preload the already-approved `apache/kafka:4.3.1` test image, then run
`python3 tools/test_kafka_evidence_v1.py --run`. It uses only synthetic records,
an owned temporary broker on loopback, real MCP stdio and exact container cleanup.
Docker is not a runtime dependency. This is engineering acceptance, not production
capacity, a deployed-client certification or an independent security review.
