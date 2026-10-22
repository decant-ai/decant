# Test Decant with OpenTelemetry Demo trace JSONL

This directory provides two separate checks:

1. an offline, checked-in Collector-shaped fixture that works immediately; and
2. an optional user-operated capture from the public OpenTelemetry Demo 3.0.0 release.

The Demo is test data, not a Decant runtime dependency or product-specific architecture. Decant does not download, start, or control it.

## 1. Run the checked-in fixture now

From the Decant repository root:

```sh
cargo build --locked --offline -p decant-cli --bin decant
./target/debug/decant ingest otlp-jsonl --file examples/opentelemetry-demo/traces-v0.jsonl
```

A successful run exits zero, writes no stderr, and emits one compact JSON line. For this fixture, expect nonzero values for `decode.spans`, `admission.accepted`, `context.spans.total`, duration facts, one error span, a `sha256:v1:` admission-state receipt, and `decant://` provenance. The output intentionally excludes raw service names, span names, trace IDs, span IDs, attributes, and timestamps.

This command accepts only the bounded OTLP trace JSONL subset documented by the V0 decoder. It does not accept logs, metrics, profiles, arbitrary JSON, compressed files, protobuf framing, unterminated final records, or files above the configured record/count/total-byte bounds.

## 2. Capture public Demo traces locally

Prerequisites for this optional step are Git and Docker with the Compose plugin. They are external test tools, not Decant dependencies. The instructions pin the Demo release used when this guide was reviewed on 2026-09-01.

Choose absolute paths and create a new temporary capture directory:

```sh
export DECANT_REPO="/absolute/path/to/Decant"
export DECANT_OTEL_CAPTURE_DIR="$(mktemp -d)"
export DECANT_OTEL_CAPTURE_CONFIG="$DECANT_REPO/examples/opentelemetry-demo/otelcol-config-decant.yml"
git clone --depth 1 --branch 3.0.0 https://github.com/open-telemetry/opentelemetry-demo.git
cd opentelemetry-demo
```

Inspect the fully merged Compose model before starting it:

```sh
docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  config
```

Start the minimal Demo plus the Decant capture override. The Demo load generator creates traffic automatically; keep this first capture short so it stays inside Decant's finite in-memory limits.

```sh
docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  up -d

docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  ps
```

After roughly 15–30 seconds of healthy traffic, stop the Collector to close/flush the capture, then verify the file exists without printing its contents:

```sh
docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  stop otel-collector

test -s "$DECANT_OTEL_CAPTURE_DIR/traces.jsonl"
wc -c -l "$DECANT_OTEL_CAPTURE_DIR/traces.jsonl"
```

Run Decant over the completed file:

```sh
cd "$DECANT_REPO"
cargo build --locked --offline -p decant-cli --bin decant
./target/debug/decant ingest otlp-jsonl --file "$DECANT_OTEL_CAPTURE_DIR/traces.jsonl"
```

When finished, return to the Demo checkout and stop all of its containers with the same Compose file set:

```sh
docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  down
```

## Sensitive-data warning

Real trace captures may contain credentials, user data, business identifiers, URLs, SQL, headers, or other sensitive values. Keep the capture local, do not commit it, do not paste it into issues or logs, and delete the temporary directory when you no longer need it. The aggregate Decant output in this milestone is conservative, but this is not a complete anonymization, privacy, compliance, or egress-policy guarantee.

The Demo's current Collector config performs its own configured span processing before the added exporter. Do not infer that this removes every sensitive value. The Decant V0 decoder retains a small allowlisted projection internally at maximum unclassified sensitivity and emits only the approved aggregate context.

## Troubleshooting

If the command exits with `error: OTLP JSONL processing failed`, it intentionally does not echo the path, operating-system error, record contents, or telemetry values.

Check container health and Collector logs locally:

```sh
cd /path/to/opentelemetry-demo
docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  ps
docker compose \
  -f compose.yaml \
  -f "$DECANT_REPO/examples/opentelemetry-demo/compose.decant.yaml" \
  logs --tail 100 otel-collector
```

Common causes are an unset/non-absolute mount path, an unwritable capture directory, a file still being written, a missing terminal LF, a capture above Decant's 4 MiB-per-record, 100,000-record, 256 MiB-input, 100,000-admitted-span, or 128 MiB-admitted-state bounds, or an upstream schema change. Restarting the Collector with `append: false` replaces `traces.jsonl`; move any capture you need before restarting.

The Collector file exporter explicitly warns that exact field names are not stable. If a future Collector emits an unsupported shape, Decant must fail visibly; a later versioned decoder change—not Demo-specific product logic—is required.

## Upstream references reviewed

- [OpenTelemetry Demo 3.0.0 Compose collector service](https://github.com/open-telemetry/opentelemetry-demo/blob/3.0.0/compose.yaml#L793-L817)
- [OpenTelemetry Demo 3.0.0 Collector trace pipeline](https://github.com/open-telemetry/opentelemetry-demo/blob/3.0.0/src/otel-collector/otelcol-config.yml#L231-L250)
- [OpenTelemetry Collector contrib file exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/fileexporter/README.md)

The checked-in fixture is synthetic and public-safe. No upstream source code, capture, image, or repository content is vendored here.
