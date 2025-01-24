from unittest import mock

from snowflake.cli.api.identifiers import FQN

ProjectManager = "snowflake.cli._plugins.project.commands.ProjectManager"


@mock.patch(ProjectManager)
@mock.patch("snowflake.cli._plugins.project.commands.StageManager")
def test_create_version(mock_stage, mock_pm, runner, project_directory):
    stage = FQN.from_stage("my_project_stage")

    with project_directory("dcm_project") as fh:
        result = runner.invoke(["project", "create-version"])
        assert result.exit_code == 0, result.output

    mock_stage().create.assert_called_once_with(fqn=stage)
    mock_pm().create_version.assert_called_once_with(
        project_name=FQN.from_string("my_project"),
        stage_name=stage,
    )
    mock_stage().put.assert_has_calls(
        [
            mock.call(local_path="definitions/", stage_path=stage),
            mock.call(local_path="manifest.yml", stage_path=stage),
        ]
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
    result = runner.invoke(["project", "validate", "fooBar", "--version", "v1"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"), version="v1", dry_run=True
    )
