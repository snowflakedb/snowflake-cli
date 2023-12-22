import subprocess

import pytest


@pytest.mark.e2e
def test_error_traceback_disabled_without_debug(snowcli, test_root_path):
    traceback_msg = "Traceback (most recent call last)"
    import os  # TODO remove thise line and three below
    import stat
    READABLE_BY_OTHERS = stat.S_IRGRP | stat.S_IROTH
    print(f"*******FILE CHECK: {os.stat(test_root_path / 'config' / 'malformatted_config.toml') & READABLE_BY_OTHERS}")
    result = subprocess.run(
        [
            snowcli,
            "--config-file",
            test_root_path / "config" / "malformatted_config.toml",
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
