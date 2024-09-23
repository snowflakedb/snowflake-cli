from unittest import mock

from snowflake.cli.api.identifiers import FQN

ProjectManager = "snowflake.cli._plugins.project.commands.ProjectManager"


@mock.patch(ProjectManager)
def test_create_version(mock_pm, runner, project_directory):
    with project_directory("dcm_project") as fh:
        result = runner.invoke(["project", "create-version"])
        assert result.exit_code == 0, result.output

    mock_pm().create_version.assert_called_once_with(
        project_name=FQN.from_string("my_project"),
        stage_name=FQN.from_stage("my_project_stage"),
    )
