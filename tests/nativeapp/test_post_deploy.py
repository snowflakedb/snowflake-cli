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
from snowflake.cli.plugins.nativeapp.post_deploy import execute_post_deploy_hooks
from snowflake.cli.plugins.nativeapp.run_processor import NativeAppRunProcessor

from tests.nativeapp.patch_utils import mock_connection
from tests.nativeapp.utils import NATIVEAPP_MANAGER_EXECUTE_QUERIES
from tests.testing_utils.fixtures import MockConnectionCtx


def _get_na_manager(working_dir):
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
        native_app_manager = _get_na_manager(str(project_dir))

        execute_post_deploy_hooks(native_app_manager)

        assert mock_execute_queries.mock_calls == [
            mock.call("-- app post-deploy script (1/2)"),
            mock.call("-- app post-deploy script (2/2)"),
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
        native_app_manager = _get_na_manager(str(project_dir))

        with pytest.raises(FileNotFoundError) as err:
            execute_post_deploy_hooks(native_app_manager)
