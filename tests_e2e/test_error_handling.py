import os
import subprocess

import pytest


@pytest.mark.e2e
def test_error_traceback_disabled_without_debug(snowcli, test_root_path):
    traceback_msg = "Traceback (most recent call last)"
    config_path = test_root_path / "config" / "malformatted_config.toml"
    os.chmod(config_path, 0o700)

    result = subprocess.run(
        [
            snowcli,
            "--config-file",
            config_path,
            "sql",
            "-q",
            "select 'Hello there'",
        ],
        capture_output=True,
        text=True,
    )
    assert result.stderr == "" and not traceback_msg in result.stdout

    result_debug = subprocess.run(
        [
            snowcli,
            "--config-file",
            test_root_path / "config" / "malformatted_config.toml",
            "sql",
            "-q",
            "select 'Hello there'",
            "--debug",
        ],
        capture_output=True,
        text=True,
    )
    assert traceback_msg in result_debug.stderr
