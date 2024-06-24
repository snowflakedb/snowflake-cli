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


def assert_project_contents(origin: Path, created: Path):
    def all_contents(root: Path):
        return (file.relative_to(root) for file in root.glob("**/*"))

    origin_contents = set(all_contents(origin))
    origin_contents.remove("template.yml")
    assert origin_contents == set(all_contents(created))


def test_init_no_variables_project(runner, temp_dir, test_projects_path):
    project_name = "example_streamlit_no_defaults"
    result = runner.invoke(["init", "example_streamlit_no_defaults"])
    assert result.exit_code == 0
    assert_project_contents(test_projects_path / project_name, temp_dir / project_name)


def test_init_variables_prompt(runner, temp_dir, test_projects_path, snapshot):
    project_name = "project_templating"
    result = runner.invoke(["init", "example_streamlit_no_defaults"])
    assert result.exit_code == 0
    assert_project_contents(test_projects_path / project_name, temp_dir / project_name)
    assert (test_projects_path / project_name).read_text() == snapshot


def test_init_variables_flags():
    # prompt for 1 variable
    pass


def test_init_no_prompts_with_silent_flag():
    # -D required
    pass


def test_init_error_required_not_passed_silent():
    pass


def test_init_error_not_existing_variable_in_template_toml():
    # use alter_snowflake_yml
    pass


@pytest.mark.integration
def test_init_from_url():
    pass
