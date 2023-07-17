from __future__ import annotations

import sys

from snowcli.cli.app import app


if __name__ == "__main__":
    app()

if getattr(sys, "frozen", False):
    app(sys.argv[1:])
