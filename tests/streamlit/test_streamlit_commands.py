import shutil
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest

from tests.streamlit.streamlit_test_class import (
    STREAMLIT_NAME,
    TYPER,
    StreamlitTestClass,
)


class TestStreamlitCommands(StreamlitTestClass):
    def test_list_streamlit(self, runner, mock_streamlit_ctx):
        self.mock_connector.return_value = mock_streamlit_ctx

        result = runner.invoke(["streamlit", "list"])

        assert result.exit_code == 0, result.output
        assert mock_streamlit_ctx.get_query() == "show streamlits like '%%'"

    @mock.patch(TYPER)
    def test_deploy_only_streamlit_file(self, mock_typer, project_directory, runner):

        with project_directory("example_streamlit") as tmp_dir:
            (tmp_dir / "environment.yml").unlink()
            shutil.rmtree(tmp_dir / "pages")
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = test_warehouse
                TITLE = 'My Fancy Streamlit';
                """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once
        self.mock_put.assert_called_once_with(
            local_file_name=Path("streamlit_app.py"),
            stage_location="/test_streamlit/",
            overwrite=True,
            auto_compress=False,
        )

        mock_typer.launch.assert_not_called()

    @mock.patch(TYPER)
    def test_deploy_only_streamlit_file_no_stage(
        self, mock_typer, project_directory, runner
    ):
        with project_directory("example_streamlit_no_stage") as tmp_dir:
            (tmp_dir / "environment.yml").unlink()
            shutil.rmtree(tmp_dir / "pages")
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = test_warehouse;
                """
        ).strip()
        assert result.exit_code == 0, result.output

        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self.mock_put.assert_called_once_with(
            local_file_name=Path("streamlit_app.py"),
            stage_location="/test_streamlit/",
            overwrite=True,
            auto_compress=False,
        )
        mock_typer.launch.assert_not_called()

    def test_deploy_with_empty_pages(self, project_directory, runner):
        with project_directory("streamlit_empty_pages") as tmp_dir:
            (tmp_dir / "pages").mkdir(parents=True, exist_ok=True)
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = test_warehouse;
                """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()

        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", "environment.yml"]
        )

    @mock.patch(TYPER)
    def test_deploy_only_streamlit_file_replace(
        self, mock_typer, project_directory, runner
    ):
        with project_directory("example_streamlit") as tmp_dir:
            (tmp_dir / "environment.yml").unlink()
            shutil.rmtree(tmp_dir / "pages")
            result = runner.invoke(["streamlit", "deploy", "--replace"])

        expected_query = dedent(
            f"""
                    CREATE OR REPLACE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                    ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                    MAIN_FILE = 'streamlit_app.py'
                    QUERY_WAREHOUSE = test_warehouse
                    TITLE = 'My Fancy Streamlit';
                    """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self.mock_put.assert_called_once_with(
            local_file_name=Path("streamlit_app.py"),
            stage_location="/test_streamlit/",
            overwrite=True,
            auto_compress=False,
        )
        mock_typer.launch.assert_not_called()

    @pytest.mark.parametrize(
        "project_name", ["example_streamlit_v2", "example_streamlit"]
    )
    @mock.patch(TYPER)
    def test_deploy_launch_browser(
        self, mock_typer, project_name, project_directory, runner
    ):

        with project_directory(project_name):
            result = runner.invoke(["streamlit", "deploy", "--open"])

        assert result.exit_code == 0, result.output

        mock_typer.launch.assert_called_once_with("https://foo.bar")

    @pytest.mark.parametrize(
        "project_name", ["example_streamlit_v2", "example_streamlit"]
    )
    def test_deploy_streamlit_and_environment_files(
        self, project_name, project_directory, runner, alter_snowflake_yml
    ):
        with project_directory(project_name) as tmp_dir:
            shutil.rmtree(tmp_dir / "pages")
            if project_name == "example_streamlit_v2":
                alter_snowflake_yml(
                    tmp_dir / "snowflake.yml",
                    parameter_path="entities.test_streamlit.artifacts",
                    value=["streamlit_app.py", "environment.yml"],
                )

            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = test_warehouse
                TITLE = 'My Fancy Streamlit';
                """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self._assert_that_exactly_those_files_were_put_to_stage(
            ["environment.yml", "streamlit_app.py"]
        )

    @pytest.mark.parametrize(
        "project_name", ["example_streamlit_v2", "example_streamlit"]
    )
    def test_deploy_streamlit_and_pages_files(
        self, project_name, project_directory, runner, alter_snowflake_yml
    ):
        with project_directory(project_name) as tmp_dir:
            (tmp_dir / "environment.yml").unlink()
            if project_name == "example_streamlit_v2":
                alter_snowflake_yml(
                    tmp_dir / "snowflake.yml",
                    parameter_path="entities.test_streamlit.artifacts",
                    value=["streamlit_app.py", "pages/"],
                )
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                    CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                    ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                    MAIN_FILE = 'streamlit_app.py'
                    QUERY_WAREHOUSE = test_warehouse
                    TITLE = 'My Fancy Streamlit';
                    """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self._assert_that_exactly_those_files_were_put_to_stage(
            ["pages/my_page.py", "streamlit_app.py"]
        )

    @pytest.mark.parametrize(
        "project_name", ["streamlit_full_definition_v2", "streamlit_full_definition"]
    )
    def test_deploy_all_streamlit_files(self, project_name, project_directory, runner):
        with project_directory(project_name) as tmp_dir:
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                ROOT_LOCATION = '@streamlit/{STREAMLIT_NAME}'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = test_warehouse;
                """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self._assert_that_exactly_those_files_were_put_to_stage(
            [
                "streamlit_app.py",
                "environment.yml",
                "pages/my_page.py",
                "utils/utils.py",
                "extra_file.py",
            ]
        )

    @pytest.mark.parametrize(
        "project_name, merge_definition",
        [
            (
                "example_streamlit_v2",
                {
                    "entities": {
                        "test_streamlit": {
                            "stage": "streamlit_stage",
                            "artifacts": [
                                "streamlit_app.py",
                                "environment.yml",
                                "pages/my_page.py",
                            ],
                        }
                    }
                },
            ),
            ("example_streamlit", {"streamlit": {"stage": "streamlit_stage"}}),
        ],
    )
    def test_deploy_put_files_on_stage(
        self, project_name, merge_definition, project_directory, runner
    ):
        with project_directory(
            project_name,
            merge_project_definition=merge_definition,
        ) as tmp_dir:
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                        CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                        ROOT_LOCATION = '@streamlit_stage/{STREAMLIT_NAME}'
                        MAIN_FILE = 'streamlit_app.py'
                        QUERY_WAREHOUSE = test_warehouse
                        TITLE = 'My Fancy Streamlit';
                        """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
            streamlit_name=STREAMLIT_NAME,
        )

    @pytest.mark.parametrize(
        "project_name",
        ["example_streamlit_no_defaults", "example_streamlit_no_defaults_v2"],
    )
    def test_deploy_all_streamlit_files_not_defaults(
        self, project_name, project_directory, runner
    ):
        with project_directory(project_name) as tmp_dir:
            result = runner.invoke(["streamlit", "deploy"])

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                ROOT_LOCATION = '@streamlit_stage/{STREAMLIT_NAME}'
                MAIN_FILE = 'main.py'
                QUERY_WAREHOUSE = streamlit_warehouse;
                """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_called_with(expected_query)
        self.mock_create_stage.assert_called_once()
        self._assert_that_exactly_those_files_were_put_to_stage(
            [
                "main.py",
                "streamlit_environment.yml",
                "streamlit_pages/first_page.py",
            ],
            STREAMLIT_NAME,
        )

    @pytest.mark.parametrize(
        "project_name", ["example_streamlit", "example_streamlit_v2"]
    )
    @pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
    @pytest.mark.parametrize("enable_streamlit_no_checkouts", [True, False])
    def test_deploy_streamlit_main_and_pages_files_experimental(
        self,
        os_agnostic_snapshot,
        enable_streamlit_versioned_stage,
        enable_streamlit_no_checkouts,
        project_name,
        project_directory,
        runner,
        alter_snowflake_yml,
    ):
        with (
            mock.patch(
                "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
                return_value=enable_streamlit_versioned_stage,
            ),
            mock.patch(
                "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled",
                return_value=enable_streamlit_no_checkouts,
            ),
        ):
            with project_directory(project_name) as tmp_dir:
                if project_name == "example_streamlit_v2":
                    alter_snowflake_yml(
                        tmp_dir / "snowflake.yml",
                        parameter_path="entities.test_streamlit.artifacts",
                        value=[
                            "streamlit_app.py",
                            "environment.yml",
                            "pages/my_page.py",
                        ],
                    )
                result = runner.invoke(["streamlit", "deploy", "--experimental"])

        if enable_streamlit_versioned_stage:
            post_create_command = f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') ADD LIVE VERSION FROM LAST;"
        else:
            if enable_streamlit_no_checkouts:
                post_create_command = None
            else:
                post_create_command = (
                    f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') CHECKOUT;"
                )

        expected_query = dedent(
            f"""
                   CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('{STREAMLIT_NAME}')
                   MAIN_FILE = 'streamlit_app.py'
                   QUERY_WAREHOUSE = test_warehouse
                   TITLE = 'My Fancy Streamlit';
                   """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_any_call(expected_query)
        if post_create_command:
            self.mock_execute.assert_any_call(post_create_command)

        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
        )

    @pytest.mark.parametrize(
        "project_name", ["example_streamlit_no_stage", "example_streamlit_no_stage_v2"]
    )
    @pytest.mark.parametrize("enable_streamlit_versioned_stage", [True, False])
    def test_deploy_streamlit_main_and_pages_files_experimental_no_stage(
        self, enable_streamlit_versioned_stage, project_name, project_directory, runner
    ):
        with mock.patch(
            "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled",
            return_value=enable_streamlit_versioned_stage,
        ):
            with project_directory(project_name) as tmp_dir:
                result = runner.invoke(["streamlit", "deploy", "--experimental"])

        if enable_streamlit_versioned_stage:
            post_create_command = f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') ADD LIVE VERSION FROM LAST;"
        else:
            post_create_command = (
                f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') CHECKOUT;"
            )

        expected_query = dedent(
            f"""
                CREATE STREAMLIT IF NOT EXISTS IDENTIFIER('{STREAMLIT_NAME}')
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = test_warehouse;
                """
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_any_call(expected_query)
        if post_create_command:
            self.mock_execute.assert_any_call(post_create_command)
        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
        )

    @pytest.mark.parametrize(
        "project_name", ["example_streamlit", "example_streamlit_v2"]
    )
    def test_deploy_streamlit_main_and_pages_files_experimental_replace(
        self, project_name, project_directory, runner, alter_snowflake_yml
    ):

        with project_directory(project_name) as tmp_dir:
            if project_name == "example_streamlit_v2":
                alter_snowflake_yml(
                    tmp_dir / "snowflake.yml",
                    parameter_path="entities.test_streamlit.artifacts",
                    value=["streamlit_app.py", "environment.yml", "pages/"],
                )
            result = runner.invoke(
                ["streamlit", "deploy", "--experimental", "--replace"]
            )

        expected_query = dedent(
            f"""
                    CREATE OR REPLACE STREAMLIT IDENTIFIER('{STREAMLIT_NAME}')
                    MAIN_FILE = 'streamlit_app.py'
                    QUERY_WAREHOUSE = test_warehouse
                    TITLE = 'My Fancy Streamlit';
                    """
        ).strip()
        assert result.exit_code == 0, result.output
        self.mock_execute.assert_any_call(expected_query)
        self.mock_execute.assert_any_call(
            f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') CHECKOUT;"
        )
        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
        )

    def test_share_streamlit(self, runner, mock_streamlit_ctx):
        self.mock_connector.return_value = mock_streamlit_ctx
        role = "other_role"

        result = runner.invoke(["streamlit", "share", STREAMLIT_NAME, role])

        assert result.exit_code == 0, result.output
        assert (
            mock_streamlit_ctx.get_query()
            == f"grant usage on streamlit IDENTIFIER('{STREAMLIT_NAME}') to role {role}"
        )

    def test_drop_streamlit(self, runner, mock_streamlit_ctx):
        self.mock_connector.return_value = mock_streamlit_ctx

        result = runner.invoke(["object", "drop", "streamlit", STREAMLIT_NAME])

        assert result.exit_code == 0, result.output
        assert (
            mock_streamlit_ctx.get_query()
            == f"drop streamlit IDENTIFIER('{STREAMLIT_NAME}')"
        )

    @mock.patch(
        "snowflake.cli._plugins.streamlit.manager.make_snowsight_url",
        return_value="https://foo.bar",
    )
    def test_get_streamlit_url(self, mock_url, runner, mock_streamlit_ctx):
        self.mock_connector.return_value = mock_streamlit_ctx
        result = runner.invoke(["streamlit", "get-url", STREAMLIT_NAME])

        assert result.exit_code == 0, result.output
        assert result.output == "https://foo.bar\n"

    @pytest.mark.parametrize(
        "command, parameters",
        [
            ("list", []),
            ("list", ["--like", "PATTERN"]),
            ("describe", ["NAME"]),
            ("drop", ["NAME"]),
        ],
    )
    @mock.patch("snowflake.cli._plugins.object.manager.ObjectManager.execute_query")
    def test_command_aliases(self, mock_execute_query, command, parameters, runner):
        result = runner.invoke(["object", command, "streamlit", *parameters])
        assert result.exit_code == 0, result.output

        result = runner.invoke(
            ["streamlit", command, *parameters], catch_exceptions=False
        )
        assert result.exit_code == 0, result.output

        queries = mock_execute_query.call_args_list
        assert len(queries) == 2
        assert queries[0] == queries[1]

    @pytest.mark.parametrize("entity_id", ["app_1", "app_2"])
    def test_selecting_streamlit_from_pdf(self, entity_id, project_directory, runner):

        with project_directory("example_streamlit_multiple_v2"):
            result = runner.invoke(["streamlit", "deploy", entity_id, "--replace"])

        expected_query = dedent(
            f"""
                CREATE OR REPLACE STREAMLIT IDENTIFIER('{entity_id}')
                ROOT_LOCATION = '@streamlit/None'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = 'streamlit';"""
        ).strip()

        assert result.exit_code == 0, result.output
        self.mock_execute.assert_any_call(expected_query)

        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", f"{entity_id}.py"], streamlit_name=entity_id
        )

    def test_multiple_streamlit_raise_error_if_multiple_entities(
        self, os_agnostic_snapshot, project_directory, runner
    ):
        with project_directory("example_streamlit_multiple_v2"):
            result = runner.invoke(["streamlit", "deploy"])

        assert result.exit_code == 2, result.output
        assert result.output.strip() == os_agnostic_snapshot

    def test_deploy_streamlit_with_comment_v2(self, project_directory, runner):
        with project_directory("example_streamlit_with_comment_v2") as tmp_dir:
            result = runner.invoke(["streamlit", "deploy", "--replace"])

        expected_query = dedent(
            f"""
                CREATE OR REPLACE STREAMLIT IDENTIFIER('test_streamlit_deploy_snowcli')
                ROOT_LOCATION = '@streamlit/test_streamlit_deploy_snowcli'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = xsmall
                TITLE = 'My Streamlit App with Comment'
                COMMENT = 'This is a test comment';
                """
        ).strip()
        assert result.exit_code == 0, result.output
        self.mock_execute.assert_any_call(expected_query)
        self.mock_create_stage.assert_called_once()
        self._assert_that_exactly_those_files_were_put_to_stage(
            ["streamlit_app.py", "environment.yml", "pages/my_page.py"],
            streamlit_name="test_streamlit_deploy_snowcli",
        )

    def test_execute_streamlit(self, runner, mock_streamlit_ctx):
        self.mock_connector.return_value = mock_streamlit_ctx
        result = runner.invoke(["streamlit", "execute", STREAMLIT_NAME])

        assert result.exit_code == 0, result.output
        assert result.output == f"Streamlit {STREAMLIT_NAME} executed.\n"
        assert mock_streamlit_ctx.get_queries() == [
            "EXECUTE STREAMLIT IDENTIFIER('test_streamlit')()"
        ]
