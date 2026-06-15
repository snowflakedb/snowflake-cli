# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import os
import sys

from snowflake.cli._app.cli_app import CliAppFactory


def _apply_stdout_encoding_from_env() -> None:
    """Apply stdout encoding from env var before config is loaded.

    Config-file encoding is applied later in config_init, but that runs inside
    the CLI invocation after some output may already have been written. Reading
    the env var here ensures the very first bytes use the right codec.
    """
    enc = os.environ.get("SNOWFLAKE_CLI_ENCODING_STDOUT")
    if not enc:
        return
    try:
        sys.stdout.reconfigure(encoding=enc)  # type: ignore[attr-defined,union-attr]
    except (AttributeError, Exception):
        pass


def main(*args):
    _apply_stdout_encoding_from_env()
    app = CliAppFactory().create_or_get_app()
    app(*args)


if __name__ == "__main__":
    main()

if getattr(sys, "frozen", False):
    main(sys.argv[1:])
