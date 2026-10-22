#!/usr/bin/env python3
"""Check candidate bytes without executing the binary or trusting its origin."""

import argparse
import sys
from pathlib import Path

sys.dont_write_bytecode = True
from binary_candidate_v1 import CandidateFailure, verify_candidate


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    try:
        args = parser.parse_args()
        verify_candidate(args.directory)
    except (CandidateFailure, OSError, ValueError, KeyError, TypeError, RecursionError):
        print("binary candidate: failed: consistency", file=sys.stderr)
        return 1
    print("PASS: candidate byte consistency only; unsigned, not authenticated or release-approved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
