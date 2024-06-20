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

from unittest import mock

import pytest
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor

from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import NATIVEAPP_MANAGER_EXECUTE_QUERIES
from tests.testing_utils.fixtures import MockConnectionCtx


def _get_run_processor(working_dir):
    dm = DefinitionManager(working_dir)
    return NativeAppRunProcessor(
        project_definition=dm.project_definition.native_app,
        project_root=dm.project_root,
    )


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock_connection()
def test_sql_scripts(
    mock_conn,
    mock_execute_queries,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy") as project_dir:
        processor = _get_run_processor(str(project_dir))

        processor._execute_post_deploy_hooks()  # noqa SLF001

        assert mock_execute_queries.mock_calls == [
            mock.call("-- app post-deploy script (1/2)\n"),
            mock.call("-- app post-deploy script (2/2)\n"),
        ]


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@mock_connection()
def test_missing_sql_script(
    mock_conn,
    mock_execute_queries,
    project_directory,
):
    mock_conn.return_value = MockConnectionCtx()
    with project_directory("napp_post_deploy_missing_file") as project_dir:
        processor = _get_run_processor(str(project_dir))

        with pytest.raises(FileNotFoundError) as err:
            processor._execute_post_deploy_hooks()  # noqa SLF001
