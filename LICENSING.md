# Decant licensing

Copyright (C) 2026 Eshan Vaid.

## Default: AGPL-3.0-only

This grant covers the original Decant files selected for the OSS source export
by [the public-source allowlist](release/public-source-allowlist-v0.tsv), and
binaries built from that code. Internal planning/legal records, private local
data and other files outside that export boundary are not included in this
grant. Do not infer a license to the internal repository's entire history.

Except for the specific files below and separately identified third-party
material, original exported Decant source, documentation, tests, fixtures and tools
are licensed under the GNU Affero General Public License, version 3 only
(SPDX: `AGPL-3.0-only`). See [LICENSE](LICENSE) for the complete terms.
This includes every existing Rust crate, the compiler, runtime, adapters and CLI.
The optional "or any later version" wording in the license document's example
application notice is not adopted by this project.

Decant is distributed without any warranty; see LICENSE for the full disclaimer.
The Free Software Foundation copyright in LICENSE identifies the author of the
license text, not the owner of Decant. No commercial alternative license is
granted by this notice.

## Apache-2.0 file exceptions

Only the following standalone configuration examples are licensed under
Apache License, Version 2.0, instead of the default AGPL license:

- `examples/evidence/kafka-config-v1.json`
- `examples/kafka-live/grouped-ratio-config-v0.example.json`
- `examples/kafka-mcp/config-v0.json`
- `examples/opentelemetry-demo/compose.decant.yaml`
- `examples/opentelemetry-demo/otelcol-config-decant.yml`

For each listed file: Copyright (C) 2026 Eshan Vaid.
Licensed under the Apache License, Version 2.0 (the "License"); you may not use
these files except in compliance with the License. Obtain a copy from
[LICENSES/Apache-2.0.txt](LICENSES/Apache-2.0.txt) or
https://www.apache.org/licenses/LICENSE-2.0 . Unless required by applicable law
or agreed to in writing, software distributed under the License is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.

This section is the file-specific copyright/license notice for JSON files,
which cannot contain comments. Keep the applicable notice and license when
redistributing them. YAML files also carry SPDX comments. Other example files,
generated reports, test fixtures and scripts are not Apache-licensed by this
exception. There is no standalone SDK in this release. New exceptions require
an explicit reviewed license decision; this is not `AGPL-3.0-only OR Apache-2.0`
for the core or the repository as a whole.

## Third-party material and release status

Dependencies and separately attributed upstream material retain their own
copyrights, licenses and notices; this notice does not relicense them. The
[Cargo dependency inventory](release/third-party-dependencies-v0.tsv) records
declared package metadata, not a complete native/system redistribution review.

Installing these licenses does not declare a public launch, production
readiness, or completion of signing, independent validation, contributor-policy
or publication gates. Those are maintainer release procedures, not additional
restrictions on rights granted by the licenses. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the current hold on accepting external contributions. No CLA/DCO or
commercial dual-licensing agreement is introduced here.

## License text provenance

Verbatim publisher text retrieved on 2026-10-21; SPDX choices are applied by
this notice and Cargo metadata. No edits to the license templates or their
publisher copyright notices:

- `LICENSE`: https://www.gnu.org/licenses/agpl-3.0.txt — SHA-256
  `0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0`.
- `LICENSES/Apache-2.0.txt`: https://www.apache.org/licenses/LICENSE-2.0.txt — SHA-256
  `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.
