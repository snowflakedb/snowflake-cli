import os
import subprocess

import pytest


@pytest.mark.e2e
def test_error_traceback_disabled_without_debug(snowcli, test_root_path):
    config_path = test_root_path / "config" / "config.toml"
    os.chmod(config_path, 0o700)

    traceback_msg = "Traceback (most recent call last)"
    result = subprocess.run(
        [
            snowcli,
            "--config-file",
            config_path,
            "sql",
            "-c",
            "integration",
            "-q",
            "select foo",
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert "SQL compilation error" in result.stdout
    assert traceback_msg not in result.stdout
    assert not result.stderr

    result_debug = subprocess.run(
        [
            snowcli,
            "--config-file",
            config_path,
            "sql",
            "-c",
            "integration",
            "-q",
            "select foo",
            "--debug",
        ],
        capture_output=True,
        text=True,
    )

    assert result_debug.returncode == 1
    assert not result_debug.stdout
    assert traceback_msg in result_debug.stderr
