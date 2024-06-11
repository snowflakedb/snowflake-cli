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


@pytest.mark.e2e
def test_corrupted_config_in_default_location(
    snowcli, temp_dir, isolate_default_config_location, test_root_path, snapshot
):
    default_config = Path(temp_dir) / "config.toml"
    default_config.write_text("[connections.demo]\n[connections.demo]")
    default_config.chmod(0o600)
    # corrupted config should produce human-friendly error
    result_err = subprocess.run(
        [snowcli, "connection", "list"],
        capture_output=True,
        text=True,
    )
    assert result_err.returncode == 1
    assert result_err.stderr == snapshot

    # corrupted config in default location should not influence one passed with --config-file flag
    healthy_config = test_root_path / "config" / "config.toml"
    result_healthy = subprocess.run(
        [snowcli, "--config-file", healthy_config, "connection", "list"],
        capture_output=True,
        text=True,
    )
    assert result_healthy.returncode == 0, result_healthy.stderr
    assert "dev" in result_healthy.stdout and "integration" in result_healthy.stdout
