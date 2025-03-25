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
from pathlib import Path
from textwrap import dedent

import pytest
from snowflake.cli.api.secure_utils import restrict_file_permissions

from tests_e2e.conftest import subprocess_run


@pytest.mark.e2e
def test_error_traceback_disabled_without_debug(snowcli, test_root_path, config_file):
    traceback_msg = "Traceback (most recent call last)"
    result = subprocess_run(
        [
            snowcli,
            "--config-file",
            config_file,
            "sql",
            "-c",
            "integration",
            "-q",
            "select foo",
        ]
    )

    assert "SQL compilation error" in result.stderr
    assert traceback_msg not in result.stderr
    assert result.stdout == "select foo"


@pytest.mark.e2e
def test_corrupted_config_in_default_location(
    snowcli,
    temporary_directory,
    isolate_default_config_location,
    test_root_path,
    snapshot,
):
    default_config = Path(temporary_directory) / "config.toml"
    default_config.write_text("[connections.demo]\n[connections.demo]")
    restrict_file_permissions(default_config)
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
    snowcli, temporary_directory, isolate_default_config_location
):
    custom_log_path = os.path.join(temporary_directory, "custom", "logs")
    default_config = Path(temporary_directory) / "config.toml"
    config_logs_path = custom_log_path.replace("\\", "\\\\")
    default_config.write_text(
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
    restrict_file_permissions(default_config)

    result = subprocess_run(
        [snowcli, "--help"],
    )

    assert result.returncode == 0
    with open(os.path.join(custom_log_path, "snowflake-cli.log")) as log_file:
        log_content = log_file.read()
        assert "Loaded external plugin: multilingual-hello" in log_content


@pytest.mark.e2e
def test_initial_log_with_loaded_external_plugins_in_custom_log_path_with_custom_config(
    snowcli, temporary_directory, isolate_default_config_location
):
    custom_log_path = os.path.join(temporary_directory, "custom", "logs")
    custom_config = Path(temporary_directory) / "custom" / "config.toml"
    custom_config.parent.mkdir()
    config_logs_path = custom_log_path.replace("\\", "\\\\")
    custom_config.write_text(
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
    restrict_file_permissions(custom_config)

    result = subprocess_run([snowcli, "--config-file", custom_config, "--help"])

    assert result.returncode == 0
    with open(os.path.join(custom_log_path, "snowflake-cli.log")) as log_file:
        log_content = log_file.read()
        assert "Loaded external plugin: multilingual-hello" in log_content
