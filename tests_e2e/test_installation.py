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
from pathlib import Path

import pytest

from tests_common import skip_snowpark_on_newest_python
from tests_e2e.conftest import subprocess_check_output, subprocess_run


@pytest.mark.e2e
@skip_snowpark_on_newest_python
def test_snow_help(snowcli, snapshot):
    output = subprocess_check_output([snowcli, "--help"])
    snapshot.assert_match(output)


@pytest.mark.e2e
@skip_snowpark_on_newest_python
def test_snow_sql(snowcli, test_root_path, snapshot):
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            test_root_path / "config" / "config.toml",
            "sql",
            "-q",
            "select round(ln(10), 2)",
            "-c",
            "integration",
        ],
    )
    snapshot.assert_match(output)


@pytest.mark.e2e
@pytest.mark.parametrize(
    "template,files_to_check",
    [
        (
            "example_streamlit",
            ["pages/my_page.py", "common/hello.py", "environment.yml"],
        ),
        ("example_snowpark", ["requirements.txt", "app/functions.py", "snowflake.yml"]),
    ],
)
def test_snow_init(temporary_directory, snowcli, template, files_to_check):
    project_path = Path(temporary_directory) / "streamlit_template"
    output = subprocess_check_output(
        [
            snowcli,
            "init",
            str(project_path),
            "--template",
            template,
            "--no-interactive",
        ]
    )
    assert "Initialized the new project in" in output
    for file in files_to_check:
        expected_generated_file = project_path / file
        assert (
            expected_generated_file.exists()
        ), f"[{expected_generated_file}] does not exist. It should be generated from templates directory."


@pytest.mark.e2e
def test_command_from_external_plugin(snowcli, test_root_path, snapshot):
    output = subprocess_check_output(
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
    )
    snapshot.assert_match(output)


@pytest.mark.e2e
def test_disabling_and_enabling_command(snowcli, config_file, snapshot):
    # assert that test starts with enabled plugin (command group visible in help)
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_file,
            "--help",
        ],
    )
    snapshot.assert_match(output)

    # plugin should be marked as enabled by list command
    output = subprocess_check_output(
        [snowcli, "--config-file", config_file, "plugin", "list"]
    )
    snapshot.assert_match(output)

    # disable plugin
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_file,
            "plugin",
            "disable",
            "multilingual-hello",
        ],
    )
    snapshot.assert_match(output)

    # plugin should be marked as disabled by list command
    output = subprocess_check_output(
        [snowcli, "--config-file", config_file, "plugin", "list"]
    )
    snapshot.assert_match(output)

    # assert that plugin is disabled (command group not visible in help)
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_file,
            "--help",
        ],
    )
    snapshot.assert_match(output)

    # enable plugin
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_file,
            "plugin",
            "enable",
            "multilingual-hello",
        ],
    )
    snapshot.assert_match(output)

    # assert that plugin is enabled (command group visible in help)
    output = subprocess_check_output(
        [
            snowcli,
            "--config-file",
            config_file,
            "--help",
        ],
    )
    snapshot.assert_match(output)

    # plugin should be marked as enabled by list command
    output = subprocess_check_output(
        [snowcli, "--config-file", config_file, "plugin", "list"]
    )
    snapshot.assert_match(output)

    # enable not existing plugin
    output = subprocess_run(
        [snowcli, "--config-file", config_file, "plugin", "enable", "asdf1234"]
    )
    assert output.returncode == 1
    assert (
        "Plugin asdf1234 is not installed. Available plugins: multilingual-hello."
        in output.stderr
    )

    # assert that config file contains configs of enabled and disabled configs
    snapshot.assert_match(config_file.read_text())
