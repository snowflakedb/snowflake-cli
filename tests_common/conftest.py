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
import tempfile
from pathlib import Path
from unittest import mock

import pytest

from snowflake.cli._plugins.workspace.context import WorkspaceContext, ActionContext
from snowflake.cli.api.console.abc import AbstractConsole


@pytest.fixture
def temp_dir():
    initial_dir = os.getcwd()

    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            os.chdir(tmp_dir)
            yield tmp_dir
        finally:
            # this has to happen before tmp_dir is cleaned up
            # so that we don't try to remove the cwd of the process
            os.chdir(initial_dir)


@pytest.fixture()
def mock_console():
    yield mock.MagicMock(spec=AbstractConsole)


@pytest.fixture()
def workspace_context(mock_console):
    return WorkspaceContext(
        console=mock_console,
        project_root=Path().resolve(),
        get_default_role=lambda: "mock_role",
        get_default_warehouse=lambda: "mock_warehouse",
    )


@pytest.fixture()
def action_context():
    return ActionContext(get_entity=lambda *args: None)
