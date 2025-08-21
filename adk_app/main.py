#!/usr/bin/env python3
import argparse
import os
import sys

# Reuse the core logic from tools/bq_audit.py
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
TOOLS_DIR = os.path.join(REPO_ROOT, "tools")
sys.path.insert(0, TOOLS_DIR)

from bq_audit import main as audit_main  # type: ignore  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ADK BigQuery Audit entrypoint")
    parser.add_argument("--project", required=True)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--locations", default="US,EU")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--outfile", default="./bq_job_stats.csv")
    return parser.parse_args()


def main() -> int:
    # Delegate to the core tool: it already handles auth via ADC and writes CSV
    return audit_main()


if __name__ == "__main__":
    sys.exit(main())


