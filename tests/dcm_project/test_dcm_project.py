from pathlib import Path
from unittest import mock

from snowflake.cli.api.identifiers import FQN

from tests_common import IS_WINDOWS

ProjectManager = "snowflake.cli._plugins.project.commands.ProjectManager"


@mock.patch(ProjectManager)
@mock.patch("snowflake.cli._plugins.project.commands.StageManager.create")
@mock.patch("snowflake.cli.api.artifacts.upload.StageManager.put")
def test_create_version(mock_put, mock_create, mock_pm, runner, project_directory):
    stage = FQN.from_stage("my_project_stage")

    with project_directory("dcm_project") as root:
        result = runner.invoke(["project", "create-version"])
        assert result.exit_code == 0, result.output

    mock_create.assert_called_once_with(fqn=stage)
    mock_pm().create_version.assert_called_once_with(
        project_name=FQN.from_string("my_project"),
        stage_name=stage,
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
                stage_path="my_project_stage/definitions",
                overwrite=True,
            ),
            mock.call(
                local_path=absolute_root
                / "output"
                / "bundle"
                / "definitions"
                / "a.sql",
                stage_path="my_project_stage/definitions",
                overwrite=True,
            ),
            mock.call(
                local_path=absolute_root / "output" / "bundle" / "manifest.yml",
                stage_path="my_project_stage/.",
                overwrite=True,
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
        project_name="my_project", from_stage="@stage", alias="v1", comment="fancy"
    )


@mock.patch(ProjectManager)
def test_execute_project(mock_pm, runner, project_directory):
    result = runner.invoke(["project", "execute", "fooBar", "--version", "v1"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
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
