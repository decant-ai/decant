# Rollback and Withdrawal Runbook

This runbook covers the `0.1.0-alpha.1` developer preview. Stop and preserve evidence before changing checkpoints or public artifacts.

## Source rollback

Revert the specific reviewed commit through a new pull request, rerun the engineering release gate, and issue a new prerelease identifier when public behavior or persisted bytes change. Do not rewrite shared Git history. A source archive already copied by another party cannot be recalled.

## Configuration rollback

Restore the last reviewed config and mapping together. Kafka mapping identity is checkpoint-bound: a changed mapping/source/config can require a new checkpoint namespace. Never relabel an incompatible checkpoint or silently reset progress.

Evidence V1 also binds recipe, policy and budget into Kafka source identity.
Revert those together with the binary/config; do not point an older command or
incompatible recipe at an evidence checkpoint. Immutable report JSON and its
source capture remain separate: keep the old report ID when reproducing an
investigation, and do not replace provenance with references from a new cut.

## Checkpoint recovery

Stop the finite Kafka invocation or externally supervised resident service, copy the checkpoint directory for investigation, and recover only through strict-latest validation. If the latest published generation is corrupt or incompatible, Decant deliberately refuses instead of falling back to an older generation. Do not delete or edit checkpoint files merely to force startup; choose a reviewed fresh namespace when loss of prior progress is explicitly accepted. After an abrupt stop, Kafka must still retain records from the last verified frontier because later unpublished progress is intentionally not recovery authority.

## Artifact or release withdrawal

Keep the previous trusted binary, config, checksums and signing-verification
receipt before upgrading. Test `--version`, demo and authorized replay with a
separate checkpoint copy before replacing a service. On Unix stop the healthy
owner with SIGINT/SIGTERM, wait for process exit, and leave the persistent lock
file in place. If shutdown fails, treat pending progress as uncertain and use
strict recovery, not lock deletion or corruption fallback.

Disable future downloads and mark the affected version withdrawn wherever it was published. Retain checksum, source commit, incident notes, and replacement guidance. Rotate any exposed credential immediately and follow `SECURITY.md`. Withdrawal does not erase clones, caches, forks, package mirrors, transparency logs, or Git history.

## Public-history boundary

Publishing a repository, tag, release, artifact, license, or credential can be non-reversible. Never use history rewriting as the primary response. Correct forward, preserve an audit trail, and obtain maintainer and qualified legal guidance when licensing, ownership, disclosure, or user data may be affected.
