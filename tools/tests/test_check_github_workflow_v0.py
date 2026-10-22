from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_github_workflow_v0 import EXPECTED_WORKFLOW, WorkflowFailure, validate


class GitHubWorkflowTests(unittest.TestCase):
    def test_exact_workflow_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ci.yml"
            path.write_text(EXPECTED_WORKFLOW, encoding="utf-8")
            validate(path)

    def test_mutable_action_write_permission_and_unsafe_trigger_fail(self) -> None:
        mutations = [
            EXPECTED_WORKFLOW.replace(
                "actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd",
                "actions/checkout@v5",
            ),
            EXPECTED_WORKFLOW.replace("contents: read", "contents: write"),
            EXPECTED_WORKFLOW.replace("pull_request:", "pull_request_target:"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            for index, content in enumerate(mutations):
                path = Path(directory) / f"ci-{index}.yml"
                path.write_text(content, encoding="utf-8")
                with self.assertRaises(WorkflowFailure):
                    validate(path)

    def test_secret_upload_cache_docker_and_command_drift_fail(self) -> None:
        additions = [
            "      - run: printf '%s' '${{ secrets.TOKEN }}'\n",
            "      - uses: actions/upload-artifact@0000000000000000000000000000000000000000\n",
            "      - uses: actions/cache@0000000000000000000000000000000000000000\n",
            "      - run: docker compose up\n",
            "      - run: cargo test\n",
        ]
        with tempfile.TemporaryDirectory() as directory:
            for index, addition in enumerate(additions):
                path = Path(directory) / f"ci-{index}.yml"
                path.write_text(EXPECTED_WORKFLOW + addition, encoding="utf-8")
                with self.assertRaises(WorkflowFailure):
                    validate(path)

    def test_missing_symlink_oversized_and_non_utf8_workflow_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            missing = root / "missing.yml"
            with self.assertRaises(WorkflowFailure):
                validate(missing)
            target = root / "target.yml"
            target.write_text(EXPECTED_WORKFLOW, encoding="utf-8")
            link = root / "link.yml"
            link.symlink_to(target)
            with self.assertRaises(WorkflowFailure):
                validate(link)
            oversized = root / "oversized.yml"
            oversized.write_bytes(b"x" * (16 * 1024 + 1))
            with self.assertRaises(WorkflowFailure):
                validate(oversized)
            non_utf8 = root / "non-utf8.yml"
            non_utf8.write_bytes(b"\xff")
            with self.assertRaises(WorkflowFailure):
                validate(non_utf8)


if __name__ == "__main__":
    unittest.main()
