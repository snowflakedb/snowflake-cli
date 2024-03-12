import importlib
import sys
import tempfile
from pathlib import Path
from textwrap import dedent

import pytest
from snowflake.cli.api.config import config_init


@pytest.fixture(autouse=True)
def enable_snowgit_fixture(snowflake_home):
    _reload_config_to_enable_snowgit(snowflake_home / "config.toml")
    yield


def reload_config_to_enable_snowgit():
    with tempfile.TemporaryDirectory() as tmpdir:
        _reload_config_to_enable_snowgit(Path(tmpdir) / "config.toml")


def _reload_config_to_enable_snowgit(config_file_to_use: Path) -> None:
    config_file_to_use.write_text(
        dedent(
            """
            [cli.features]
            enable_snowgit = true
            """
        )
    )
    # load config and recalculate global values
    config_init(config_file_to_use)
    for module in [
        sys.modules["snowflake.cli.api.feature_flags"],
        sys.modules["snowflake.cli.app.commands_registration.builtin_plugins"],
        sys.modules["snowflake.cli.app.commands_registration.command_plugins_loader"],
    ]:
        importlib.reload(module)
