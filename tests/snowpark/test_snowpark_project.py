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

    def test_drop_project_missing_name(self, runner):
        """Test that the drop project command fails when name is missing."""

        result = runner.invoke(["snowpark", "project", "drop"])
        assert result.exit_code == 1, result.output
        assert "Project name is required." in result.output

    @mock.patch(COMMAND_PROJECT_MANAGER)
    def test_drop_project(self, mock_manager, runner):
        """Test that the drop project command drops a project."""

        project_name = "test_project"
        mock_manager.return_value.drop.return_value = (
            f"{project_name} successfully dropped."
        )
        result = runner.invoke(["snowpark", "project", "drop", project_name])
        assert result.exit_code == 0, result.output
        assert f"{project_name} successfully dropped." in result.output

    @mock.patch(COMMAND_PROJECT_MANAGER)
    def test_list_projects(self, mock_manager, mock_cursor, runner):
        """Test that the list projects command lists all projects."""

        mock_manager.return_value.list_projects.return_value = mock_cursor(
            rows=[("test_project",), ("test_project_2",)], columns=["name"]
        )
        result = runner.invoke(["snowpark", "project", "list"])
        assert result.exit_code == 0, result.output
        assert "test_project" in result.output
        assert "test_project_2" in result.output

    @mock.patch(COMMAND_PROJECT_MANAGER)
    def test_execute_project(self, mock_manager, mock_cursor, runner):
        """Test that the execute project command executes a project."""

        mock_manager.return_value.execute.return_value = mock_cursor(
            rows=[("done",)], columns=["result"]
        )
        result = runner.invoke(
            ["snowpark", "project", "execute", "test_project", "--entrypoint", "app.py"]
        )
        assert result.exit_code == 0, result.output
        assert "done" in result.output
        assert mock_manager.return_value.execute.call_args.kwargs == {
            "name": "test_project",
            "entrypoint": "app.py",
        }

    def test_execute_project_missing_name(self, runner):
        """Test that the execute project command fails when name is missing."""

        result = runner.invoke(
            ["snowpark", "project", "execute", "--entrypoint", "app.py"]
        )
        assert result.exit_code == 1, result.output
        assert "Project name is required." in result.output

    def test_execute_project_missing_entrypoint(self, runner):
        """Test that the execute project command fails when entrypoint is missing."""

        result = runner.invoke(["snowpark", "project", "execute", "test_project"])
        assert result.exit_code == 1, result.output
        assert "Entrypoint is required." in result.output


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

    @mock.patch(f"{MANAGER_PROJECT_MANAGER}.execute_query")
    @mock.patch(f"{MANAGER_PROJECT_MANAGER}._set_session_config")
    def test_drop_project(
        self, mock__set_session_config, mock_execute_query, mock_cursor
    ):
        """Test that the drop project method drops a project."""

        project_name = "test_project"
        mock_execute_query.return_value = mock_cursor(
            rows=[(f"{project_name} successfully dropped.",)], columns=["status"]
        )
        mock__set_session_config.return_value = None
        manager = SnowflakeProjectManager()
        manager.drop(name=project_name)
        mock_execute_query.assert_called_once_with(
            f"DROP SNOWPARK PROJECT {project_name}"
        )

    @mock.patch(f"{MANAGER_PROJECT_MANAGER}.execute_query")
    @mock.patch(f"{MANAGER_PROJECT_MANAGER}._set_session_config")
    def test_list_projects(
        self, mock__set_session_config, mock_execute_query, mock_cursor
    ):
        """Test that the list projects method lists all projects."""

        mock_execute_query.return_value = mock_cursor(
            rows=[("test_project",), ("test_project_2",)], columns=["name"]
        )
        mock__set_session_config.return_value = None
        manager = SnowflakeProjectManager()
        manager.list_projects()
        mock_execute_query.assert_called_once_with("SHOW SNOWPARK PROJECTS")

    @mock.patch(f"{MANAGER_PROJECT_MANAGER}.execute_query")
    @mock.patch(f"{MANAGER_PROJECT_MANAGER}._set_session_config")
    def test_execute_project(
        self, mock__set_session_config, mock_execute_query, mock_cursor
    ):
        """Test that the execute project method executes a project."""

        mock_execute_query.return_value = mock_cursor(
            rows=[("done",)], columns=["result"]
        )
        mock__set_session_config.return_value = None
        manager = SnowflakeProjectManager()
        manager.execute(name="test_project", entrypoint="app.py")
        mock_execute_query.assert_called_once_with(
            "EXECUTE SNOWPARK PROJECT test_project ENTRYPOINT='app.py'"
        )
