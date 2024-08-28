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

import os
import subprocess
from pathlib import Path
from textwrap import dedent

import pytest
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.secure_utils import windows_get_not_whitelisted_users_with_access

from tests_common import IS_WINDOWS
from tests_e2e.conftest import subprocess_run


def grant_permissions_on_windows(path: Path) -> None:
    if not IS_WINDOWS:
        return
    import subprocess

    user = os.getlogin()
    cli_console.message(f">> granting permissions on {path} to user {user}")
    result = subprocess.run(
        ["icacls", str(path), "/grant", f"{user}:F"], capture_output=True, text=True
    )
    cli_console.message(result.stdout)


def _restrict_file_permissions_unix(path: Path) -> None:
    path.chmod(0o600)


def _restrict_file_permissions_windows(path: Path):
    for user in windows_get_not_whitelisted_users_with_access(path):
        subprocess.run(["icacls", str(path), "/DENY", f"{user}:F"])
    subprocess.run(["icacls", str(path), "/GRANT", f"{os.getlogin()}:F"])


def _restrict_file_permissions(file_path: Path):
    if IS_WINDOWS:
        _restrict_file_permissions_windows(file_path)
    else:
        _restrict_file_permissions_unix(file_path)


@pytest.mark.e2e
def test_error_traceback_disabled_without_debug(snowcli, test_root_path):
    config_path = test_root_path / "config" / "config.toml"
    grant_permissions_on_windows(config_path)
    _restrict_file_permissions(config_path)
    cli_console.message(f">> Config path: {config_path}")
    cli_console.message(config_path.read_text())

    traceback_msg = "Traceback (most recent call last)"
    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            config_path,
            "sql",
            "-c",
            "integration",
            "-q",
            "select foo",
        ]
    )

    assert "SQL compilation error" in result.stdout
    assert traceback_msg not in result.stdout
    assert not result.stderr

    result_debug = subprocess_run(
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
        ]
    )

    assert result_debug.stdout == "select foo\n"
    assert traceback_msg in result_debug.stderr


@pytest.mark.e2e
def test_corrupted_config_in_default_location(
    snowcli, temp_dir, isolate_default_config_location, test_root_path, snapshot
):
    default_config = Path(temp_dir) / "config.toml"
    default_config.write_text("[connections.demo]\n[connections.demo]")
    _restrict_file_permissions(default_config)
    # corrupted config should produce human-friendly error
    result_err = subprocess_run(
        [snowcli, "connection", "list"],
    )
    assert result_err.stderr == snapshot

    # corrupted config in default location should not influence one passed with --config-file flag
    healthy_config = test_root_path / "config" / "config.toml"
    result_healthy = subprocess_run(
        [snowcli, "--config-file", healthy_config, "connection", "list"],
    )
    assert "dev" in result_healthy.stdout and "integration" in result_healthy.stdout


@pytest.mark.e2e
def test_initial_log_with_loaded_external_plugins_in_custom_log_path(
    snowcli, temp_dir, isolate_default_config_location, test_root_path
):
    custom_log_path = os.path.join(temp_dir, "custom", "logs")
    default_config = Path(temp_dir) / "config.toml"
    config_logs_path = custom_log_path.replace("\\", "\\\\")
    with open(default_config, "w", newline="\n") as config:
        config.write(
            dedent(
                f"""[cli.logs]
            path = "{config_logs_path}"

            [connections.default]
            [connections.integration]

            [cli.plugins.snowpark-hello]
            enabled = true
            [cli.plugins.snowpark-hello.config]
            greeting = "Hello"

            [cli.plugins.multilingual-hello]
            enabled = true
            """
            )
        )
        config.flush()
    _restrict_file_permissions(default_config)

    result = subprocess_run(
        [snowcli, "--help"],
    )

    assert result.returncode == 0
    with open(os.path.join(custom_log_path, "snowflake-cli.log")) as log_file:
        log_content = log_file.read()
        assert "Loaded external plugin: multilingual-hello" in log_content


@pytest.mark.e2e
def test_initial_log_with_loaded_external_plugins_in_custom_log_path_with_custom_config(
    snowcli, temp_dir, isolate_default_config_location, test_root_path
):
    custom_log_path = os.path.join(temp_dir, "custom", "logs")
    custom_config = Path(temp_dir) / "custom" / "config.toml"
    custom_config.parent.mkdir()
    config_logs_path = custom_log_path.replace("\\", "\\\\")
    with open(custom_config, "w", newline="\n") as config:
        config.write(
            dedent(
                f"""[cli.logs]
            path = "{config_logs_path}"

            [connections.default]
            [connections.integration]

            [cli.plugins.snowpark-hello]
            enabled = true
            [cli.plugins.snowpark-hello.config]
            greeting = "Hello"

            [cli.plugins.multilingual-hello]
            enabled = true
            """
            )
        )
        config.flush()
    _restrict_file_permissions(custom_config)

    result = subprocess_run([snowcli, "--config-file", custom_config, "--help"])

    assert result.returncode == 0
    with open(os.path.join(custom_log_path, "snowflake-cli.log")) as log_file:
        log_content = log_file.read()
        assert "Loaded external plugin: multilingual-hello" in log_content
