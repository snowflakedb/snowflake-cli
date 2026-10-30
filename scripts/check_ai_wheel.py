"""Verify the OAuth plugin is included in the installed wheel, not an editable tree."""

import importlib.metadata
import subprocess
import sys
from pathlib import Path

distribution = importlib.metadata.distribution("snowflake-cli")
plugin = Path(
    str(distribution.locate_file("snowflake/cli/_plugins/ai/opencode_oauth.mjs"))
).resolve()
assert plugin.is_file(), "Installed wheel is missing the OAuth plugin"
assert plugin.is_relative_to(
    sys.prefix
), "Plugin was resolved outside the installed environment"
subprocess.run(["node", "--check", str(plugin)], check=True)
