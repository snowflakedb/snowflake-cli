from unittest import mock

from snowflake.cli._plugins.snowpark.project.manager import SnowflakeProjectManager

COMMAND_PROJECT_MANAGER = (
    "snowflake.cli._plugins.snowpark.project.commands.SnowflakeProjectManager"
)

MANAGER_PROJECT_MANAGER = (
    "snowflake.cli._plugins.snowpark.project.manager.SnowflakeProjectManager"
)


class TestSnowparkProjectCommands:
    @mock.patch(COMMAND_PROJECT_MANAGER)
    def test_create_project_with_overwrite(self, mock_manager, runner):
        """Test that the create project command creates a project with overwrite flag."""

        project_name = "test_project"
        stage = "@test_stage"

        mock_manager.return_value.create.return_value = (
            f"{project_name} successfully created."
        )

        result = runner.invoke(
            [
                "snowpark",
                "project",
                "create",
                project_name,
                "--overwrite",
                "--stage",
                stage,
            ]
        )
        assert result.exit_code == 0, result.output
        assert f"{project_name} successfully created." in result.output
        assert mock_manager.return_value.create.call_args.kwargs == {
            "name": project_name,
            "overwrite": True,
            "stage": stage,
        }

    @mock.patch(COMMAND_PROJECT_MANAGER)
    def test_create_project_without_overwrite(self, mock_manager, runner):
        """Test that the create project command creates a project without overwrite flag."""

        project_name = "test_project"
        stage = "@test_stage"

        mock_manager.return_value.create.return_value = (
            f"{project_name} successfully created."
        )

        result = runner.invoke(
            ["snowpark", "project", "create", project_name, "--stage", stage]
        )
        assert result.exit_code == 0, result.output
        assert f"{project_name} successfully created." in result.output
        assert mock_manager.return_value.create.call_args.kwargs == {
            "name": project_name,
            "overwrite": False,
            "stage": stage,
        }

    def test_create_project_missing_name(self, runner):
        """Test that the create project command fails when name is missing."""

        result = runner.invoke(
            ["snowpark", "project", "create", "--stage", "@test_stage"]
        )
        assert result.exit_code == 1, result.output
        assert "Project name is required." in result.output

    def test_create_project_missing_stage(self, runner):
        """Test that the create project command fails when stage is missing."""

        result = runner.invoke(["snowpark", "project", "create", "test_project"])
        assert result.exit_code == 1, result.output
        assert "Stage is required." in result.output


class TestSnowparkProjectManager:
    @mock.patch(f"{MANAGER_PROJECT_MANAGER}.execute_query")
    def test_set_session_config(self, mock_execute_query, mock_cursor):
        """Test that the set session config method sets the session config."""

        mock_execute_query.return_value = mock_cursor(
            rows=[("Statement executed successfully.",)], columns=["status"]
        )

        manager = SnowflakeProjectManager()
        manager._set_session_config()  # noqa: SLF001
        mock_execute_query.assert_called_once_with(
            "ALTER SESSION SET ENABLE_SNOWPARK_PROJECT = 'ENABLE'"
        )

    @mock.patch(f"{MANAGER_PROJECT_MANAGER}.execute_query")
    @mock.patch(f"{MANAGER_PROJECT_MANAGER}._set_session_config")
    def test_create_project(
        self, mock__set_session_config, mock_execute_query, mock_cursor
    ):
        """Test that the create project method creates a project."""

        project_name = "test_project"
        stage = "@test_stage"

        mock_execute_query.return_value = mock_cursor(
            rows=[(f"{project_name} successfully created.",)], columns=["status"]
        )
        mock__set_session_config.return_value = None

        manager = SnowflakeProjectManager()
        manager.create(name=project_name, stage=stage, overwrite=False)
        mock_execute_query.assert_called_once_with(
            f"CREATE SNOWPARK PROJECT {project_name} FROM {stage}"
        )

    @mock.patch(f"{MANAGER_PROJECT_MANAGER}.execute_query")
    @mock.patch(f"{MANAGER_PROJECT_MANAGER}._set_session_config")
    def test_create_project_with_overwrite(
        self, mock__set_session_config, mock_execute_query, mock_cursor
    ):
        """Test that the create project method creates a project with overwrite flag."""

        project_name = "test_project"
        stage = "@test_stage"

        mock_execute_query.return_value = mock_cursor(
            rows=[(f"{project_name} successfully created.",)], columns=["status"]
        )
        mock__set_session_config.return_value = None
        manager = SnowflakeProjectManager()
        manager.create(name=project_name, stage=stage, overwrite=True)
        mock_execute_query.assert_called_once_with(
            f"CREATE OR REPLACE SNOWPARK PROJECT {project_name} FROM {stage}"
        )
