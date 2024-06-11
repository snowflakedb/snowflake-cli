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

import pytest


@pytest.mark.integration
def test_temporary_connection(runner):
    result = runner.invoke(
        [
            "object",
            "list",
            "warehouse",
            "--temporary-connection",
            "--account",
            "test_acoount",
            "--user",
            "snowcli_test",
            "--password",
            "top_secret",
            "--warehouse",
            "xsmall",
            "--database",
            "test_dv",
            "--schema",
            "PUBLIC",
        ]
    )

    assert result.exit_code == 1
    assert "HTTP 403: Forbidden" in result.output
