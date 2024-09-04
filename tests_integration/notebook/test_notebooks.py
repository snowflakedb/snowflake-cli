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
from snowflake.connector import ProgrammingError


def _execute_notebook(runner, notebook_name):
    result = runner.invoke_with_connection_json(
        ["notebook", "execute", notebook_name, "--format", "json"]
    )
    assert result.exit_code == 0
    assert result.json == {"message": f"Notebook {notebook_name} executed."}


def _execute_notebook_failure(runner, notebook_name):
    result = runner.invoke_with_connection(["notebook", "execute", notebook_name])
    assert result.exit_code == 1
    assert "NameError: name 'fooBar' is not defined" in result.output


@pytest.mark.integration
def test_create_notebook(runner, test_database, snowflake_session, snapshot):
    stage_name = "notebook_stage"
    snowflake_session.execute_string(f"create stage {stage_name};")

    notebooks = Path(__file__).parent.parent / "test_data/notebook"
    notebooks = [
        notebooks / "my_notebook.ipynb",
        notebooks / "my_notebook_error.ipynb",
    ]
    for local_notebook_file in notebooks:
        _create_notebook(local_notebook_file, runner, snowflake_session, stage_name)

    _execute_notebook(runner, notebooks[0].stem)
    _execute_notebook_failure(runner, notebooks[1].stem)


def _create_notebook(local_notebook_file, runner, snowflake_session, stage_name):
    notebook_name = local_notebook_file.stem
    stage_path = f"@{stage_name}/{local_notebook_file.name}"
    snowflake_session.execute_string(
        f"put file://{local_notebook_file.absolute()} @{stage_name} AUTO_COMPRESS=FALSE;"
    )
    command = (
        "notebook",
        "create",
        notebook_name,
        "--notebook-file",
        stage_path,
        "--format",
        "json",
    )
    result = runner.invoke_with_connection_json(command)
    assert result.exit_code == 0
    message: str = result.json.get("message", "")
    assert message.endswith(notebook_name.upper())
