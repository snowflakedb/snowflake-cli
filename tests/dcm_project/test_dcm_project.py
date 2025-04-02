from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli.api.identifiers import FQN

from tests_common import IS_WINDOWS

ProjectManager = "snowflake.cli._plugins.project.commands.ProjectManager"


@mock.patch(ProjectManager)
@mock.patch("snowflake.cli.api.artifacts.upload.StageManager.create")
@mock.patch("snowflake.cli.api.artifacts.upload.StageManager.put")
@mock.patch("snowflake.cli.api.artifacts.upload.StageManager.list_files")
@pytest.mark.parametrize("no_version", [True, False])
def test_create(
    mock_list_files,
    mock_put,
    mock_create,
    mock_pm,
    runner,
    project_directory,
    no_version,
):
    stage = FQN.from_stage("my_project_stage")
    expected_project_fqn = FQN.from_string("my_project")

    with project_directory("dcm_project") as root:
        command = ["project", "create"]
        if no_version:
            command.append("--no-version")
        result = runner.invoke(command)
        assert result.exit_code == 0, result.output

    mock_pm().create.assert_called_once_with(expected_project_fqn)
    if no_version:
        mock_create.assert_not_called()
        mock_pm().add_version.assert_not_called()
        mock_create.assert_not_called()
        return

    mock_create.assert_called_once_with(fqn=stage)
    mock_pm().add_version.assert_called_once_with(
        project_name=expected_project_fqn,
        from_stage=stage.name,
        alias=None,
        comment=None,
    )

    if IS_WINDOWS:
        absolute_root = Path(root).absolute()
    else:
        absolute_root = Path(root).resolve()
    mock_put.assert_has_calls(
        [
            mock.call(
                local_path=absolute_root
                / "output"
                / "bundle"
                / "definitions"
                / "b.sql",
                stage_path="@my_project_stage/definitions",
                role=None,
                overwrite=False,
            ),
            mock.call(
                local_path=absolute_root
                / "output"
                / "bundle"
                / "definitions"
                / "a.sql",
                stage_path="@my_project_stage/definitions",
                role=None,
                overwrite=False,
            ),
            mock.call(
                local_path=absolute_root / "output" / "bundle" / "manifest.yml",
                stage_path="@my_project_stage",
                role=None,
                overwrite=False,
            ),
        ],
        any_order=True,
    )


@mock.patch(ProjectManager)
def test_add_version(mock_pm, runner, project_directory):
    with project_directory("dcm_project") as root:
        result = runner.invoke(
            [
                "project",
                "add-version",
                "my_project",
                "--from",
                "@stage",
                "--alias",
                "v1",
                "--comment",
                "fancy",
            ]
        )
        assert result.exit_code == 0, result.output

    mock_pm().add_version.assert_called_once_with(
        project_name=FQN.from_string("my_project"),
        from_stage="@stage",
        alias="v1",
        comment="fancy",
    )


@mock.patch(ProjectManager)
def test_execute_project(mock_pm, runner, project_directory):
    result = runner.invoke(["project", "execute", "fooBar"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version=None,
        variables=None,
    )


@mock.patch(ProjectManager)
def test_execute_project_with_variables(mock_pm, runner, project_directory):
    result = runner.invoke(
        ["project", "execute", "fooBar", "--version", "v1", "-D", "key=value"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
        variables=["key=value"],
    )


@mock.patch(ProjectManager)
def test_validate_project(mock_pm, runner, project_directory):
    result = runner.invoke(
        ["project", "dry-run", "fooBar", "--version", "v1", "-D", "key=value"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
        dry_run=True,
        variables=["key=value"],
    )


def test_list_command_alias(mock_connect, runner):

    result = runner.invoke(
        [
            "object",
            "list",
            "project",
            "--like",
            "%PROJECT_NAME%",
            "--in",
            "database",
            "my_db",
        ]
    )

    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["project", "list", "--like", "%PROJECT_NAME%", "--in", "database", "my_db"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 2
    assert (
        queries[0]
        == queries[1]
        == "show projects like '%PROJECT_NAME%' in database my_db"
    )


@mock.patch(ProjectManager)
def test_list_versions(mock_pm, runner):
    result = runner.invoke(["project", "list-versions", "fooBar"])

    assert result.exit_code == 0, result.output

    mock_pm().list_versions.assert_called_once_with(
        project_name=FQN.from_string("fooBar")
    )
