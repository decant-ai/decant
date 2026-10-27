# Connect Grafana and check query access

Decant can save an explicitly scoped Grafana profile and test query access to
Prometheus/Mimir, Tempo and Loki through Grafana. This workflow checks access and
data availability; it does not diagnose an incident or compile a trace report.
Existing file/Kafka evidence commands are separate.

After setup, use the [guided investigation workflow](GRAFANA_INVESTIGATION.md)
to configure metric recipes, compare intervals and revisit saved evidence.

Build the CLI using the repository instructions, then run:

~~~sh
./target/release/decant connect grafana --url https://grafana.example.com --token-env GRAFANA_TOKEN
./target/release/decant doctor --profile production
~~~

Set GRAFANA_TOKEN in your environment using your normal secret-management
workflow first. Use a read-only Grafana service-account token with access to the
selected datasources. Decant needs datasource discovery and datasource-query
permissions. Do not pass a token as a command argument or put it in a profile.
No Decant account, model provider or Kafka cluster is needed for these commands.
The source build still includes Kafka's native prerequisites.

Connect lists supported datasource UIDs, asks for your choices, and asks for
service/environment labels and values. Confirm the proposed scope before saving.
A Prometheus-compatible datasource is required; Tempo and Loki are optional.
Do not guess your labels: check them in your existing Grafana queries first.

- Metrics and Loki use the exact service/environment label names you supply.
- Tempo uses resource.service.name and resource.deployment.environment.name.
  Selecting Tempo confirms those mappings; custom Tempo attributes are not
  supported by this setup command.
- Grafana must expose its documented /api/datasources discovery and
  /api/datasources/proxy/uid/:uid/… read paths. These legacy APIs remain
  operational but are deprecated in Grafana 13; compatibility with every plugin,
  reverse proxy or managed deployment is not established.

For noninteractive setup, supply all required choices explicitly:

~~~sh
./target/release/decant connect grafana --url https://grafana.example.com --token-env GRAFANA_TOKEN --profile production --prometheus-uid metrics --service-label service_name --service checkout --environment-label environment --environment prod --yes
~~~

Optional flags: --tempo-uid, --loki-uid, --ca-file, --profile-dir.
The --ca-file option adds one PEM root certificate; server certificate and hostname
verification stay enabled. Only HTTPS is supported. Redirects, implicit proxies,
automatic retries and decompression are disabled. A redirect to a login page
does not authenticate the connection.

Profiles default to .decant/profiles/PROFILE.json relative to the current
directory. They contain configuration and a token reference, not the token.
Use the same directory for doctor, or pass the same --profile-dir to both.
Existing profiles are never overwritten. Symlink paths, nonregular files and
group/world-accessible profile directories/files are refused on Unix. Use a
canonical nonsymlink path and restrictive permissions. A failed new-file write
can leave a partial file; review and remove only that file before retrying.
Profiles, endpoint names and local files can still be sensitive.

## Read the doctor report

Doctor freezes one time cut, sends scoped read queries, and emits
decant-grafana-doctor version 1 JSON. It probes metrics series count, Loki
five-minute log count, and a one-match Tempo search. It does not fetch raw log
bodies or full traces, and does not print backend warning/error text.

| Status | Meaning |
| --- | --- |
| ready | The limited scoped query succeeded and returned data |
| empty | The query succeeded but no matching data was observed |
| partial | The backend reported warning/incomplete results |
| unavailable | Access, transport, budget or response validation failed |
| not_configured | You did not select this optional datasource |

Doctor exits 0 only when every configured source is ready. Other reports exit 1.
A successful empty response is not permission denial and does not prove the
service is healthy. Ready does not establish complete telemetry, investigation
coverage or an atomic cross-source snapshot. Every probe is explicitly limited.

Fixed failure codes include permission_denied, rate_limited,
backend_timeout, redirect_refused, invalid_backend_response and
backend_budget_exhausted. Check token permissions, datasource UID, mappings
and the selected time period in Grafana; do not post tokens or production
responses to public issues. No automatic telemetry is sent.

Transport limits: one in-flight read, 32 attempts, five minutes elapsed,
30 seconds per request, 4 MiB retained per response and 64 MiB session body
budget. The reader counts and rejects a chunk crossing a body limit; network,
TLS and HTTP-parser buffers are separate. These are not exact wire-byte limits.

Local acceptance uses synthetic HTTPS servers and real CLI processes. It is not
a live Grafana installation certification. See the upstream
[datasource API](https://grafana.com/docs/grafana/latest/developer-resources/api-reference/http-api/api-legacy/data_source/)
for permissions and deployment-specific API support.
