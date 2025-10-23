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

import json

import pytest


@pytest.mark.integration
def test_broken_command_path_plugin(runner, test_root_path, _install_plugin, caplog):
    config_path = (
        test_root_path / "config" / "plugin_tests" / "broken_plugin_config.toml"
    )

    result = runner.invoke(
        ["--config-file", config_path, "connection", "list", "--format", "JSON"]
    )
    assert result.exit_code == 0, result.output

    assert "Loaded external plugin: broken_plugin" in caplog.messages
    assert (
        "Cannot register plugin [broken_plugin]: Invalid command path [snow broken run]. Command group [broken] does not exist."
        in caplog.messages
    )

    # Parse JSON output and check for test connection existence
    connections = json.loads(result.output)

    # Find the 'test' connection
    test_connection = next(
        (conn for conn in connections if conn["connection_name"] == "test"), None
    )
    assert test_connection is not None, "Expected 'test' connection not found in output"
    assert test_connection["parameters"] == {"account": "test"}
    assert test_connection["is_default"] is False


@pytest.fixture(scope="module")
def _install_plugin(test_root_path):
    import subprocess

    path = test_root_path / ".." / "test_external_plugins" / "broken_plugin"
    subprocess.check_call(["pip", "install", path])
