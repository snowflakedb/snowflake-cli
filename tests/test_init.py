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


def _get_values_from_created_project_as_str(created: Path):
    return (created / "variable_values.json").read_text()


def _get_values_from_created_project(created: Path):
    return json.loads(_get_values_from_created_project_as_str(created))


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


def test_error_handling(
    runner, test_projects_path, temp_dir, project_definition_copy, monkeypatch, snapshot
):
    # no template.yml
    project_name = "example_streamlit_no_defaults"
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path / project_name,
        ]
    )
    assert result.exit_code == 1
    assert "Template does not have template.yml file." in result.output
    assert not Path(project_name).exists()

    # project already exists
    project_name = "project_templating"
    result = runner.invoke(
        [
            "init",
            temp_dir,
            "--template-source",
            test_projects_path / project_name,
        ]
    )
    assert result.exit_code == 1
    assert "The directory" in result.output
    assert "exists." in result.output

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

    # template source does not exist
    with pytest.raises(FileNotFoundError, match=".*No such file or directory.*"):
        project_name = "this_project_does_not_exist"
        runner.invoke(
            [
                "init",
                project_name,
                "--template-source",
                project_name,
            ]
        )
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
                template_root,
            ]
        )
        assert result.output == snapshot
        assert not Path(project_name).exists()

    # variable not defined in template.yml
    project_name = "project_templating"
    with project_definition_copy(project_name) as template_root:
        (template_root / "template.yml").write_text(
            "files_to_render:\n - variable_values.json"
        )
        result = runner.invoke(
            [
                "init",
                project_name,
                "--template-source",
                template_root,
            ]
        )
        assert result.exit_code == 1
        assert "'required_project_name' is undefined" in result.output
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
                template_root,
            ]
        )
        assert result.exit_code == 0, result.output
        assert f"Initialized the new project in {project_name}" in result.output
        assert_project_contents(template_root, Path(project_name))


def test_init_default_values(runner, temp_dir, test_projects_path):
    project_name = "project_templating"
    communication = ["required", "", "", "", ""]
    result = runner.invoke(
        ["init", project_name, "--template-source", test_projects_path / project_name],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert f"Initialized the new project in {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "optional_float": 1.5,
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_unchecked": "5",
        "project_name": "required",
        "jinja_sum_filter": 5.5,
    }


def test_rename_project(runner, temp_dir, test_projects_path):
    project_name = "project_templating"
    new_path = Path(temp_dir) / "dir" / "subdir" / "a_new_project"
    new_path.parent.mkdir(parents=True)
    result = runner.invoke(
        [
            "init",
            str(new_path),
            "--template-source",
            test_projects_path / project_name,
            f"-D required_project_name=name",
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"Initialized the new project in {new_path}" in result.output
    assert_project_contents(test_projects_path / project_name, new_path)
    assert _get_values_from_created_project(new_path) == {
        "optional_float": 1.5,
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_unchecked": "5",
        "project_name": "name",
        "jinja_sum_filter": 5.5,
    }


def test_init_prompted_values(runner, temp_dir, test_projects_path):
    project_name = "project_templating"
    communication = [
        "a_project_name",
        "17",
        "custom value for string",
        "2.7",
        "another custom value",
    ]
    result = runner.invoke(
        ["init", project_name, "--template-source", test_projects_path / project_name],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert f"Initialized the new project in {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "optional_float": 2.7,
        "optional_int": 17,
        "optional_str": "custom value for string",
        "optional_unchecked": "another custom value",
        "project_name": "a_project_name",
        "jinja_sum_filter": 19.7,
    }


def test_template_flag(runner, temp_dir, test_projects_path):
    project_name = "project_templating"
    communication = [
        "project_name",
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
    assert f"Initialized the new project in {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "optional_float": 2.7,
        "optional_int": 17,
        "optional_str": "custom value for string",
        "optional_unchecked": "another custom value",
        "project_name": "project_name",
        "jinja_sum_filter": 19.7,
    }


def test_typechecking(runner, temp_dir, test_projects_path, snapshot):
    # incorrect type passed via flag
    project_name = "project_templating"
    with pytest.raises(
        ValueError, match=r"invalid literal for int\(\) with base 10: 'not an int'"
    ):
        runner.invoke(
            [
                "init",
                project_name,
                "--template-source",
                test_projects_path / project_name,
                "-D required_project_name=a_project_name",
                "-D optional_int=not an int",
                "--no-interactive",
            ]
        )
    assert not Path(project_name).exists()

    # incorrect value passed in interactive mode
    communication = [
        "project_name",
        "23.1",
        "23",
        "custom value for string",
        "3..14",  # incorrect float
        "3.14",
        "another custom value",
    ]
    result = runner.invoke(
        ["init", project_name, "--template-source", test_projects_path / project_name],
        input="\n".join(communication),
    )
    assert result.exit_code == 0, result.output
    assert result.output == snapshot
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "project_name": "project_name",
        "optional_int": 23,
        "optional_str": "custom value for string",
        "optional_float": 3.14,
        "optional_unchecked": "another custom value",
        "jinja_sum_filter": 26.14,
    }


def test_variables_flags(runner, temp_dir, test_projects_path, snapshot):
    project_name = "project_templating"
    communication = [""]
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path / project_name,
            "-D required_project_name=name",
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
        "project_name": "name",
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_float": -100.5,
        "optional_unchecked": "21",
        "jinja_sum_filter": -96.5,
    }


def test_init_no_interactive(runner, temp_dir, test_projects_path):
    project_name = "project_templating"

    # error: required variables need to be passed via -D
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path / project_name,
            "--no-interactive",
        ],
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Cannot determine value of variable required_project_name" in result.output

    # successful run
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path / project_name,
            "--no-interactive",
            "-D required_project_name=particular_project_name",
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"Initialized the new project in {project_name}" in result.output
    assert_project_contents(test_projects_path / project_name, Path(project_name))
    assert _get_values_from_created_project(Path(temp_dir) / project_name) == {
        "project_name": "particular_project_name",
        "optional_int": 4,
        "optional_str": "default value for string",
        "optional_float": 1.5,
        "optional_unchecked": "5",
        "jinja_sum_filter": 5.5,
    }


@pytest.mark.parametrize(
    "project_identifier,expected",
    [
        ("Project Name", "Project_Name"),
        ("project-name", "project_name"),
        ("project.name", "project_name"),
        ("project-name.12", "project_name_12"),
        ('"project-name.12"', '"project-name.12"'),
    ],
)
def test_to_project_identifier_filter(
    runner, temp_dir, test_projects_path, project_identifier, expected
):
    project_name = "project_templating"
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path / project_name,
            "--no-interactive",
            f"-D required_project_name={project_identifier}",
        ],
    )
    assert result.exit_code == 0, result.output

    assert f'"project_name": "{expected}"' in _get_values_from_created_project_as_str(
        Path(temp_dir) / project_name
    )


@pytest.mark.parametrize("project_identifier", ["7days", "123name123"])
def test_validate_snowflake_identifier(
    runner, temp_dir, test_projects_path, project_identifier
):
    project_name = "project_templating"
    result = runner.invoke(
        [
            "init",
            project_name,
            "--template-source",
            test_projects_path / project_name,
            "--no-interactive",
            f"-D required_project_name={project_identifier}",
        ],
    )
    assert result.exit_code == 1
    assert "cannot be converted to valid Snowflake identifier" in result.output


def test_project_directory_name_variable(runner, temp_dir, project_definition_copy):
    project_name = "project_templating"
    with project_definition_copy(project_name) as template_root:
        (template_root / "template.yml").write_text("files_to_render:\n - file.txt")
        (template_root / "file.txt").write_text(
            "project directory name: <! project_dir_name !>"
        )
        for project_path in [
            Path("new_project"),
            Path("very") / "nested" / "directory_with_stuff",
        ]:
            result = runner.invoke(
                [
                    "init",
                    str(project_path),
                    "--template-source",
                    template_root,
                ]
            )
            assert result.exit_code == 0, result.output
            assert f"Initialized the new project in {project_path}" in result.output
            assert (
                project_path / "file.txt"
            ).read_text() == f"project directory name: {project_path.name}"
