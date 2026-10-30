# Copyright (c) 2026 Snowflake Inc.
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

"""Integration tests for ``snow app setup --capabilities``.

Which capabilities are on depends on the test account, so these tests assert
the shape of the report rather than specific values. The mapping from
parameter values to capabilities is covered by unit tests in
``tests/apps/test_commands.py``.
"""

import pytest


@pytest.mark.integration
def test_setup_capabilities_reports_each_capability(
    runner, temporary_working_directory
):
    result = runner.invoke_with_connection_json(["app", "setup", "--capabilities"])

    assert result.exit_code == 0, result.output
    assert set(result.json) == {"serverless", "python", "python-serverless"}
    assert all(isinstance(value, bool) for value in result.json.values())
    assert not (temporary_working_directory / "app.yml").exists()
