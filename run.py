from __future__ import annotations

import logging
import sys

from app.presentation.cli import cli

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cli(sys.argv[1:])
