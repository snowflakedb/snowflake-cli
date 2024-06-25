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

from tests_e2e.conftest import subprocess_check_output


@pytest.mark.e2e
def test_snow_help(snowcli, snapshot):
    output = subprocess_check_output([snowcli, "--help"])
    snapshot.assert_match(output)


@pytest.mark.e2e
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
def test_snow_streamlit_init(temp_dir, snowcli, snapshot):
    output = subprocess_check_output([snowcli, "streamlit", "init", "streamlit_test"])
    snapshot.assert_match(output)
    expected_generated_file = f"{temp_dir}/streamlit_test/pages/my_page.py"
    assert Path(
        expected_generated_file
    ).exists(), f"[{expected_generated_file}] does not exist. It should be generated from templates directory."


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
