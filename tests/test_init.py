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

import json
from contextlib import contextmanager
from pathlib import Path

import pytest


def assert_project_contents(origin: Path, created: Path):
    def all_contents(root: Path):
        return (file.relative_to(root) for file in root.glob("**/*"))

    origin_contents = set(all_contents(origin))
    origin_contents.remove(Path("template.yml"))
    assert origin_contents == set(all_contents(created))


def _get_values_from_created_project(created: Path):
    return json.loads((created / "variable_values.json").read_text())


@pytest.fixture
def project_definition_copy(test_projects_path):
    @contextmanager
    def copy_project_definition(project_name: str):
        from snowflake.cli.api.secure_path import SecurePath

        with SecurePath.temporary_directory() as tmp_dir:
            project_path = SecurePath(test_projects_path / project_name)
            template_path = (tmp_dir / project_name).path
            project_path.copy(template_path)
            yield (tmp_dir / project_name).path

    yield copy_project_definition


def test_input_errors(
    runner, test_projects_path, temp_dir, project_definition_copy, monkeypatch, snapshot
):
    # no template.yml
    project_name = "example_streamlit_no_defaults"
    with pytest.raises(FileNotFoundError, match=r".*template\.yml.*"):
        runner.invoke(
            [
                "init",
                project_name,
                "--template-source",
                test_projects_path,
                "--template",
                project_name,
            ]
        )
    assert not Path(project_name).exists()

    # override existing directory
    project_name = "project_templating"
    with pytest.raises(FileExistsError, match=r".*already exists"):
        runner.invoke(
            [
                "init",
                temp_dir,
                "--template-source",
                test_projects_path,
                "--template",
                project_name,
            ]
        )
    assert not Path(project_name).exists()

    # template does not exist
    project_name = "this_project_does_not_exist"
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
        ]
    )
    assert result.exit_code == 1
    assert f"Template '{project_name}' cannot be found under" in result.output
    assert not Path(project_name).exists()

    # Too low CLI version
    project_name = "example_streamlit_no_defaults"
    with project_definition_copy(project_name) as template_root:
        monkeypatch.setattr("snowflake.cli.__about__.VERSION", "915.6.0")
        (template_root / "template.yml").write_text('minimum_cli_version: "916.0.0"')
        result = runner.invoke(
            [
                "init",
                project_name,
                "--template-source",
                template_root.parent,
                "--template",
                project_name,
            ]
        )
        assert result.output == snapshot
        assert not Path(project_name).exists()

    # variable not mentioned in template.yml
    project_name = "project_templating"
    with project_definition_copy(project_name) as template_root:
        (template_root / "template.yml").write_text("files:\n - variable_values.json")
        from jinja2 import UndefinedError

        with pytest.raises(UndefinedError, match="'required' is undefined"):
            runner.invoke(
                [
                    "init",
                    project_name,
                    "--template-source",
                    template_root.parent,
                    "--template",
                    project_name,
                ]
            )
        assert not Path(project_name).exists()


def test_init_project_with_no_variables(runner, temp_dir, project_definition_copy):
    project_name = "streamlit_full_definition"
    with project_definition_copy(project_name) as template_root:
        (template_root / "template.yml").touch()
        result = runner.invoke(
            [
                "init",
                project_name,
                "--template-source",
                template_root.parent,
                "--template",
                project_name,
            ]
        )
        assert result.exit_code == 0, result.output
        assert f"Project have been created at {project_name}" in result.output
        assert_project_contents(template_root, Path(project_name))


def test_init_default_values(runner, temp_dir, test_projects_path):
    project_name = "project_templating"
    communication = ["required", "", "", "", ""]
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
        ],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert f"Project have been created at {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "optional_float": 1.5,
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_unchecked": "5",
        "required": "required",
    }


def test_init_prompted_values(runner, temp_dir, test_projects_path):
    project_name = "project_templating"
    communication = [
        "required",
        "17",
        "custom value for string",
        "2.7",
        "another custom value",
    ]
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
        ],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert f"Project have been created at {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "optional_float": 2.7,
        "optional_int": 17,
        "optional_str": "custom value for string",
        "optional_unchecked": "another custom value",
        "required": "required",
    }


def test_typechecking(runner, temp_dir, test_projects_path, snapshot):
    project_name = "project_templating"
    communication = [
        "required",
        "23.1",
        "23",
        "custom value for string",
        "3..14",
        "3.14",
        "another custom value",
    ]
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
        ],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert result.output == snapshot
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "required": "required",
        "optional_int": 23,
        "optional_str": "custom value for string",
        "optional_float": 3.14,
        "optional_unchecked": "another custom value",
    }


def test_variables_flags(runner, temp_dir, test_projects_path, snapshot):
    project_name = "project_templating"
    communication = [""]
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
            "-D required=required",
            "-D optional_int=4",
            "-D optional_float=-100.5",
            "-D optional_unchecked=21",
        ],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert result.output == snapshot
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "required": "required",
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_float": -100.5,
        "optional_unchecked": "21",
    }


def test_init_no_interactive(runner, temp_dir, test_projects_path):
    project_name = "project_templating"

    # error: required variables need to be passed via -D
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
            "--no-interactive",
        ],
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Cannot determine value of variable required" in result.output

    # successful run
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path,
            "--template",
            project_name,
            "--no-interactive",
            "-D required='a value of required variable'",
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"Project have been created at {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "required": "'a value of required variable'",
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_float": 1.5,
        "optional_unchecked": "5",
    }


@pytest.mark.integration
def test_init_from_url():
    pass
