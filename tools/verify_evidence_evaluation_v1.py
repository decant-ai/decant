#!/usr/bin/env python3
"""Verify local raw evaluation evidence; no publication/utility authorization."""
import argparse
from pathlib import Path
from evidence_evaluation.results import validate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("result", type=Path)
    args = parser.parse_args()
    try:
        validate(args.result)
    except (ValueError, TypeError, KeyError, OSError, OverflowError, RecursionError):
        print("evidence evaluation verification: failed")
        return 1
    print("PASS: raw evaluation consistency; external utility/reproduction/publication gates remain")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
