import subprocess

import pytest


@pytest.mark.e2e
def test_snow_help(snowcli, snapshot):
    output = subprocess.check_output([snowcli, "--help"], encoding="utf-8")
    snapshot.assert_match(output)


@pytest.mark.e2e
def test_snow_sql(snowcli, test_root_path, snapshot):
    output = subprocess.check_output(
        [
            snowcli,
            "--config-file",
            test_root_path / "config" / "config.toml",
            "sql",
            "-q",
            "select ln(10)",
            "-c",
            "integration",
        ],
        encoding="utf-8",
    )
    snapshot.assert_match(output)


@pytest.mark.e2e
def test_snow_streamlit_init(temp_dir, snowcli, snapshot):
    output = subprocess.check_output(
        [snowcli, "streamlit", "init", "streamlit_test"], encoding="utf-8"
    )
    snapshot.assert_match(output)


@pytest.mark.e2e
def test_command_from_external_plugin(snowcli, test_root_path, snapshot):
    output = subprocess.check_output(
        [
            snowcli,
            "--config-file",
            test_root_path / "config" / "config.toml",
            "multilingual-hello",
            "hello-en",
            "John",
            "-c",
            "integration",
        ],
        encoding="utf-8",
    )
    snapshot.assert_match(output)


@pytest.mark.e2e
def test_error_traceback_disabled_without_debug(snowcli, test_root_path):
    traceback_msg = "Traceback (most recent call last)"

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
