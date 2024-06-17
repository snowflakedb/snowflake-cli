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
    def _filename_in_created(path: Path) -> Path:
        """Remove .jinja suffix from files (but not dirs)"""
        if path.is_dir() or path.suffix != ".jinja":
            return path
        return path.parent / path.stem

    assert created.exists()

    if origin.is_file():
        # assert file
        assert created.is_file()
        if origin.suffix == ".jinja":
            assert origin.stem == created.name
        else:
            assert origin.read_text() == created.read_text()

    else:
        # assert directory
        assert created.is_dir()
        origin_contents = (
            _filename_in_created(f)
            for f in origin.iterdir()
            if f.name != "template.toml"
        )
        assert sorted(origin_contents) == sorted(created.iterdir())
        for opath, cpath in zip(sorted(origin.iterdir()), sorted(created.iterdir())):
            assert_project_contents(opath, cpath)


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
