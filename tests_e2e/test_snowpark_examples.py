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

import time

import pytest

from tests_e2e.conftest import subprocess_check_output


@pytest.mark.e2e
def test_snowpark_examples_functions_work_locally(snowcli):
    project_name = str(time.monotonic_ns())
    subprocess_check_output(
        [snowcli, "snowpark", "init", project_name],
    )

    python = snowcli.parent / "python"

    output = subprocess_check_output(
        [python, f"{project_name}/app/functions.py", "FooBar"],
    )
    assert output.strip() == "Hello FooBar!"

    output = subprocess_check_output(
        [python, f"{project_name}/app/procedures.py", "BazBar"],
    )
    assert output.strip() == "Hello BazBar!"
