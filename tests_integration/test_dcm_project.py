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

import uuid
from pathlib import Path

import pytest

from tests_integration.test_utils import (
    contains_row_with,
    row_from_snowflake_session,
    rows_from_snowflake_session,
)
from tests_integration.testing_utils import assert_that_result_is_successful
from snowflake.cli._plugins.streamlit.manager import StreamlitManager


@pytest.mark.integration
@pytest.mark.qa_only
def test_project_deploy(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dcm_project"):
        result = runner.invoke_with_connection_json(["project", "create-version"])
        assert result.exit_code == 0

        # Unsupported command
        # result = runner.invoke_with_connection(["project", "validate", "my_project", "--version", "last"])
        # assert result.exit_code == 0

        result = runner.invoke_with_connection(
            [
                "project",
                "execute",
                "my_project",
                "--version",
                "last",
                "-D",
                "desc='value'",
            ]
        )
        assert result.exit_code == 0
