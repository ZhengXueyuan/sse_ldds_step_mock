# -*- coding: utf-8 -*-
"""Entry point: run `python sse_step_rebuild.py --help`."""

import sys
import os

# Allow running from the repository root without installing the package.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sse_step.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
