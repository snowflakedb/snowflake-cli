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
import os
from unittest import mock


@pytest.mark.integration
@mock.patch.dict(
    os.environ,
    {
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT": os.environ.get(
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT", None
        ),
        "SNOWFLAKE_CONNECTIONS_INTEGRATION_USER": os.environ.get(
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_USER", None
        ),
        "SNOWFLAKE_PASSWORD": os.environ.get(
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_PASSWORD"
        ),
    },
    clear=True,
)
def test_temporary_connection(runner, snapshot):

    result = runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "--temporary-connection",
            "--account",
            os.environ["SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT"],
            "--user",
            os.environ["SNOWFLAKE_CONNECTIONS_INTEGRATION_USER"],
        ]
    )
    assert result.exit_code == 0
    assert result.output == snapshot
