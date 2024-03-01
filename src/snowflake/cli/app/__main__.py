from __future__ import annotations

import sys

from snowflake.cli.app.cli_app import app_factory


def main(*args):
    app = app_factory()
    app(*args)


if __name__ == "__main__":
    main()

if getattr(sys, "frozen", False):
    main(sys.argv[1:])
