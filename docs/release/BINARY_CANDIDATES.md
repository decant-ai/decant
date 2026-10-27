# Native binary candidates and signing boundary

For the smaller Grafana CLI without Kafka prerequisites, use the separate
[investigation-only candidate](INVESTIGATION_BINARY.md). The full-build archive
format and commands below are unchanged.

The repository can build a **local unsigned candidate**, not an approved public
download. Python 3.11+, the pinned Rust/native toolchain and previously fetched
locked dependencies are build-time requirements. Packaging supports host-native
Darwin arm64 and Linux x86_64; it does not cross-compile or certify OS versions.
The existing read-only Linux CI job runs this same offline builder/verifier after
the quality gate, without signing or retaining/uploading a distributable asset.
Passing CI supplies engineering evidence, not independent human activation.

From a reviewed checkout, choose a new output directory under ignored storage:

```sh
mkdir -p .local
python3 tools/build_binary_candidate_v1.py --output .local/candidate-1
python3 tools/verify_binary_candidate_v1.py .local/candidate-1
```

The builder exports only tracked allowlisted files, builds offline in that
temporary export, packages the executable, then runs the unpacked binary on
demo/init/validate/analyze/render/verify with an empty environment. It compares
replay bytes and the frozen demonstration. It does not contact a Kafka broker,
invoke an LLM, install anything, sign, create a key, tag or publish. Pre-existing
output directories/files and symlinks are refused; failed output is not silently
overwritten. Build logs are ordinary compiler logs; use local ignored storage.

Outputs are a target-named `.tar.gz`, `SHA256SUMS`, canonical `manifest.json` and
the existing source archive/checksum/manifest. The binary archive has exactly
eight flat members beneath a version/target prefix. It includes a reviewable
default config, synthetic trace capture, instructions, lockfile, dependency
inventory and `SBOM.cdx.json`. Licensed engineering mode includes `LICENSE`;
the legacy unlicensed variant includes `LICENSE-MISSING.txt` instead.
Source commit, export inventory, target, Rust version, binary/package hashes,
license mode and clean-tree status are recorded. A dirty engineering candidate
is visibly marked and cannot pass the public gate.

The V0 source inventory/artifact value `approved-present` describes the installed
project license, not authorization to publish. V1 binary verifiers now accept
either engineering license member set while keeping exactly eight members;
older verifiers reject the licensed engineering variant. Update the verifier,
not an existing archive. Public-candidate mode still requires LICENSE and the
separate exact-commit approval record. The source export includes LICENSING.md
and the Apache-2.0 exception text. Binary-bundled capture/config files are under
the default AGPL license; none of the Apache configuration exceptions is bundled.

Packaging is byte-equal for the same inputs; native compilation has **not** been
established as byte-reproducible or hermetic. Compiler/global host configuration,
native libraries and supported OS versions require review. The small CycloneDX
1.6 SBOM covers all locked Cargo packages, including dev/target-specific ones;
it is explicitly incomplete for host/system and bundled-native detail. It is
neither a complete linked-component inventory nor a vulnerability/license scan.

The builder remaps and rejects known home, worktree, Cargo-cache and exported
source prefixes in the executable. This is not comprehensive path scrubbing:
vendored OpenSSL still embeds its temporary installation directory as native
configuration. These paths and native defaults need review before distribution;
they are not patched out of executables or used to claim hermetic builds.

The read-only verifier does not execute the candidate. It checks bounded regular
files, exact archive members/modes, checksums, metadata and regenerated SBOM.
This is consistency, **not authentication**. Its source-archive checksum is not
a substitute for the separate source export/extracted-tree gate. Run binaries
only from sources you trust. Protect candidate directories against concurrent
untrusted writers; this is not a compromised-host defense.

## Distribution status

These commands produce local engineering artifacts, not approved downloads.
Signed binary distribution is deferred. A future downloadable release needs
reviewed native/system dependencies, clean-target installation, signing identity
and platform acceptance for each advertised target. No signing key or publishing
service is configured by these tools, and a local build is not an origin signature.

The existing release validators fail closed on absent authorization/signature
evidence. Private approval records are not source artifacts and must never be
committed or included in an export. Passing a source or packaging check does not
supply those approvals. Do not bypass OS security checks or claim a local candidate
is a signed release. Preserve previous trusted builds/configuration and follow
the [rollback runbook](ROLLBACK.md) when replacing a local installation.
