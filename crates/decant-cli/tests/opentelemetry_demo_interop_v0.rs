use std::process::Command;

const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/opentelemetry-demo/traces-v0.jsonl"
);
const GOLDEN: &[u8] = include_bytes!("golden/otlp-jsonl-run-v0.json");
const FIXTURE: &[u8] = include_bytes!("../../../examples/opentelemetry-demo/traces-v0.jsonl");
const COLLECTOR_OVERLAY: &str =
    include_str!("../../../examples/opentelemetry-demo/otelcol-config-decant.yml");
const COMPOSE_OVERRIDE: &str =
    include_str!("../../../examples/opentelemetry-demo/compose.decant.yaml");
const GUIDE: &str = include_str!("../../../examples/opentelemetry-demo/README.md");

const EXPECTED_COLLECTOR_OVERLAY: &str = concat!(
    "# SPDX-FileCopyrightText: 2026 Eshan Vaid\n",
    "# SPDX-License-Identifier: Apache-2.0\n",
    "# Additive config loaded after the OpenTelemetry Demo base/extras configs.\n",
    "exporters:\n",
    "  file/decant:\n",
    "    path: /decant-capture/traces.jsonl\n",
    "    format: json\n",
    "    append: false\n",
    "    flush_interval: 1s\n",
    "\n",
    "service:\n",
    "  pipelines:\n",
    "    traces:\n",
    "      exporters: [debug, span_metrics, file/decant]\n",
);
const EXPECTED_COMPOSE_OVERRIDE: &str = concat!(
    "# SPDX-FileCopyrightText: 2026 Eshan Vaid\n",
    "# SPDX-License-Identifier: Apache-2.0\n",
    "# OpenTelemetry Demo 3.0.0 minimal-compose override for a local trace capture.\n",
    "services:\n",
    "  otel-collector:\n",
    "    command:\n",
    "      - --config=/etc/otelcol-config.yml\n",
    "      - --config=/etc/otelcol-config-extras.yml\n",
    "      - --config=/etc/otelcol-config-decant.yml\n",
    "      - --feature-gates=service.profilesSupport\n",
    "    volumes:\n",
    "      - ${DECANT_OTEL_CAPTURE_CONFIG:?set DECANT_OTEL_CAPTURE_CONFIG}:/etc/otelcol-config-decant.yml:ro\n",
    "      - ${DECANT_OTEL_CAPTURE_DIR:?set DECANT_OTEL_CAPTURE_DIR}:/decant-capture:rw\n",
);

#[test]
fn collector_shaped_fixture_matches_exact_cli_golden_without_raw_values() {
    let output = Command::new(env!("CARGO_BIN_EXE_decant"))
        .args(["ingest", "otlp-jsonl", "--file", FIXTURE_PATH])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_eq!(output.stdout, GOLDEN);
    assert!(GOLDEN.len() <= 64 * 1_024);

    let text = std::str::from_utf8(GOLDEN).unwrap();
    for expected in [
        "\"decode\":{\"spans\":3",
        "\"accepted\":3",
        "\"finalized\":true",
        "\"error_rate_ppm\":{\"value\":333333,\"remainder\":1,\"denominator\":3}",
        "\"duration_ns\":{\"minimum\":1000000,\"maximum\":4000000",
        "sha256:v1:",
        "decant://event/v0/otlp-jsonl-v0/otlp-file/1/1/0/0",
    ] {
        assert!(text.contains(expected));
    }

    for raw in [
        "fixture-checkout-private",
        "fixture-payment-private",
        "fixture-cart-read-private",
        "fixture-checkout-write-private",
        "fixture-payment-error-private",
        "fixture-status-message-private",
        "11111111111111111111111111111111",
        "3333333333333333",
    ] {
        assert!(std::str::from_utf8(FIXTURE).unwrap().contains(raw));
        assert!(!text.contains(raw), "raw fixture value leaked: {raw}");
        assert!(!std::str::from_utf8(&output.stderr).unwrap().contains(raw));
    }
}

#[test]
fn overlays_are_exactly_additive_and_guide_keeps_demo_optional() {
    assert_eq!(COLLECTOR_OVERLAY, EXPECTED_COLLECTOR_OVERLAY);
    assert_eq!(COMPOSE_OVERRIDE, EXPECTED_COMPOSE_OVERRIDE);
    assert_eq!(COLLECTOR_OVERLAY.matches("file/decant").count(), 2);
    assert!(!COLLECTOR_OVERLAY.contains("receivers:"));
    assert!(!COLLECTOR_OVERLAY.contains("processors:"));
    assert!(!COLLECTOR_OVERLAY.contains("metrics:"));
    assert!(!COLLECTOR_OVERLAY.contains("logs:"));
    assert!(!COLLECTOR_OVERLAY.contains("profiles:"));
    assert!(!COMPOSE_OVERRIDE.contains("image:"));
    assert!(!COMPOSE_OVERRIDE.contains("ports:"));
    assert!(!COMPOSE_OVERRIDE.contains("environment:"));

    for required in [
        "--branch 3.0.0",
        "mktemp -d",
        "append: false",
        "stop otel-collector",
        "test -s",
        "ingest otlp-jsonl --file",
        "may contain credentials",
        "do not commit it",
        "exact field names are not stable",
        "not a Decant runtime dependency",
        "https://github.com/open-telemetry/opentelemetry-demo/blob/3.0.0/compose.yaml",
        "https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/fileexporter/README.md",
    ] {
        assert!(
            GUIDE.contains(required),
            "missing guide contract: {required}"
        );
    }
}
