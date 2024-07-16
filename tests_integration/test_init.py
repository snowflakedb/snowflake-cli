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

from pathlib import Path
from typing import Set


@pytest.mark.integration
def test_not_existing_url(runner, temporary_working_directory):
    url = "https://github.com/snowflakedb/snowflake-cli/this-url-does-not-exist/"
    path = "a_new_project"
    result = runner.invoke(["init", path, "--template-source", url])
    assert result.exit_code == 1
    assert "Error" in result.output
    assert url in result.output
    assert "not exist" in result.output
    assert not (temporary_working_directory / path).exists()


@pytest.mark.integration
def test_not_existing_template(runner, temporary_working_directory):
    url = "https://github.com/snowflakedb/snowflake-cli/"
    template = "this/template/does/not/exist"
    path = "a_new_project"
    result = runner.invoke(
        ["init", path, "--template-source", url, "--template", template]
    )
    assert result.exit_code == 1
    assert "Error" in result.output
    assert f"Template '{template}' cannot be found under" in result.output
    assert url in result.output


@pytest.mark.integration
def test_missing_template_yml(runner, temporary_working_directory):
    url = "https://github.com/snowflakedb/snowflake-cli/"
    path = "a_new_project"
    result = runner.invoke(["init", path, "--template-source", url])
    assert result.exit_code == 1
    assert "Error" in result.output
    assert f"Template does not have template.yml file." in result.output


@pytest.mark.integration
def test_template_contents(runner, temporary_working_directory, test_root_path):
    url = "https://github.com/snowflakedb/snowflake-cli/"
    template = "tests/test_data/projects/project_templating"
    remote_path = "from_remote"

    # remote template
    result = runner.invoke(
        [
            "init",
            remote_path,
            "--template-source",
            url,
            "--template",
            template,
            "-D required_project_name=a new project",
            "--no-interactive",
        ]
    )
    assert result.exit_code == 0, result.output

    # local template
    local_template = test_root_path.parent / template
    local_path = "from_local"
    result = runner.invoke(
        [
            "init",
            local_path,
            "--template-source",
            local_template,
            "-D required_project_name=a new project",
            "--no-interactive",
        ]
    )
    assert result.exit_code == 0, result.output

    # compare results
    def _list_contents(root: Path) -> Set[Path]:
        return set(x.relative_to(root) for x in root.rglob("*"))

    assert _list_contents(Path(local_path)) == _list_contents(Path(local_path))


@pytest.mark.integration
def test_default_url(runner, temporary_working_directory):
    path = "a_new_project"
    template = "example_snowpark"
    runner = runner.invoke(["init", path, "--template", template, "--no-interactive"])
    assert runner.exit_code == 0, runner.output
    assert (Path(path) / "snowflake.yml").exists()
