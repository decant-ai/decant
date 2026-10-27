# Investigation-only native candidate

This is an **unsigned local engineering candidate**, not an authenticated public
release. Do not disable OS security protections to run it. Only execute a binary
you trust. The byte verifier does not execute it and is not an origin signature.

This flavor has `connect grafana`, `doctor`, `investigate`, offline report/replay,
follow-up and explicit refresh. It does not contain file-ingestion, Kafka or MCP
commands; those remain in the default full build. Evidence/bundle formats are
the same in both builds. No model account is required for deterministic reports.

## From source

Use pinned Rust 1.98.0 and previously fetched locked dependencies:

```sh
cargo build --locked --offline --release -p decant-cli --bin decant --no-default-features
./target/release/decant --help
```

The selected normal/build graph has no librdkafka or MCP. Native TLS still needs
platform prerequisites: macOS system frameworks and compiler tools; on Linux,
a C toolchain, pkg-config, OpenSSL development headers/libraries and runtime CA
certificates. This is not a static or dependency-free binary. Building the full
workspace or omitting `--no-default-features` still builds Kafka.

## Build and verify a local candidate

From a reviewed checkout, choose a **new** output directory under `.local/`:

```sh
mkdir -p .local
python3 tools/investigation_candidate_v1.py graph
python3 tools/investigation_candidate_v1.py build --output .local/investigation-candidate
python3 tools/investigation_candidate_v1.py verify --output .local/investigation-candidate
```

The builder exports tracked allowlisted source and builds it offline with an
explicit native target and no default features. It remaps/rejects known private
source prefixes and runs empty-environment help/version checks on a copied
executable. Untracked files are excluded; tracked dirty source is visibly marked
in the manifest. No key, signature, upload, tag or publication is created. Existing
outputs are refused. A failed output directory is retained, not silently replaced.

The candidate directory contains the executable, these instructions, license,
lockfile, all-locked-package inventory/SBOM, selected normal/build dependency list,
source archive/checksum/manifest, and candidate `manifest.json`/`SHA256SUMS`.
The separate `decant-investigation-candidate` V1 contract does not reinterpret the
existing full binary-archive format or pass its release approval gate.

Supported builder targets are native macOS arm64 and Linux x86_64, not a promise
about every OS version. The manifest binds source/export, target, compiler, build
flavor, file hashes and modes. The verifier rejects changed, extra, missing and
symlinked files; it never launches the submitted binary. Treat candidate directories
as trusted local storage without concurrent hostile writers.

The Cargo SBOM includes all locked packages (including inactive Kafka dependencies,
dev and other targets), **not only linked components**. `build-dependencies.txt`
narrows the selected Cargo graph; system/native linkage still requires review.
Neither is a vulnerability or license-compatibility conclusion. Native builds
are not established as hermetic or byte-reproducible. Checksums establish byte
consistency, not source authenticity or permission to distribute.

## Run a trusted copied executable

Copy `decant` to an operator-chosen directory without replacing a trusted previous
binary. Work from a private directory; profiles and raw replay bundles may be
sensitive. The binary does not need the source tree, Rust or Python to run.

```sh
./decant connect grafana --url https://grafana.example.com --token-env GRAFANA_TOKEN --profile production
./decant investigate configure --profile production
./decant doctor --profile production
./decant investigate --profile production --since 15m
```

Use an explicitly authorized backend/token and confirmed mappings. The saved
bundle identity is printed on stderr before optional model calls. Use that exact
identity for `./decant investigate report --bundle ID` or
`./decant investigate follow-up --bundle ID --focus gaps`, without a live backend
or token. Refresh is a separate explicit later-cut operation; old evidence is not
replaced. Opt-in reasoning and billing require explicit model/egress authorization.

SIGINT/SIGTERM stops pending network work with exit 130. A process killed before
bundle publication has no completed new report; restart reuses the store once
the OS releases ownership. Already persisted bundles remain available. Do not
delete lock files or assume all filesystem calls can be interrupted. Corruption
fails closed; there is no automatic rollback to another evidence identity.

For rollback, keep the previous trusted executable and private data backup. Do
not overwrite or prune evidence to make a new version start. Only versions with
matching documented bundle formats can read the same store.

Scripted source-free execution, synthetic TLS and CI checks are not independent
human activation. Own-backend installation on clean Linux/macOS machines, native
dependency review, signing custody/platform verification and exact release
authorization remain separate evidence requirements. No general production or
competitive-performance claim follows from candidate verification.
