#!/usr/bin/env python3
import argparse
import os
import sys

from .audit import main as audit_main


def main() -> int:
    # Delegates to tools/bq_audit.py which parses args and runs
    return audit_main()


if __name__ == "__main__":
    sys.exit(main())


