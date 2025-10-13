import json
from unittest import mock

import pytest
from snowflake.cli.api.identifiers import FQN

DCMProjectManager = "snowflake.cli._plugins.dcm.commands.DCMProjectManager"
ObjectManager = "snowflake.cli._plugins.dcm.commands.ObjectManager"


@pytest.fixture
def mock_project_exists():
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands.ObjectManager.object_exists",
        return_value=True,
    ) as _fixture:
        yield _fixture


@pytest.fixture
def mock_from_resource():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.FQN.from_resource",
        return_value=FQN(
            database="MockDatabase",
            schema="MockSchema",
            name="DCM_TEST_PIPELINE_1757333281_OUTPUT_TMP_STAGE",
        ),
    ) as _fixture:
        yield _fixture


class TestDCMCreate:
    @mock.patch(DCMProjectManager)
    @mock.patch(ObjectManager)
    def test_create(self, mock_om, mock_pm, runner, project_directory):
        mock_om().object_exists.return_value = False
        with project_directory("dcm_project"):
            command = ["dcm", "create", "my_project"]
            result = runner.invoke(command)
            assert result.exit_code == 0, result.output

            mock_pm().create.assert_called_once_with(
                project_identifier=FQN.from_string("my_project")
            )

    @mock.patch(DCMProjectManager)
    @mock.patch(ObjectManager)
    @pytest.mark.parametrize("if_not_exists", [False, True])
    def test_create_object_exists(
        self, mock_om, mock_pm, runner, project_directory, if_not_exists
    ):
        mock_om().object_exists.return_value = True
        with project_directory("dcm_project"):
            command = ["dcm", "create", "my_project"]
            if if_not_exists:
                command.append("--if-not-exists")
            result = runner.invoke(command)
            if if_not_exists:
                assert result.exit_code == 0, result.output
                assert "DCM Project 'my_project' already exists." in result.output
            else:
                assert result.exit_code == 1, result.output

            mock_pm().create.assert_not_called()


class TestDCMDeploy:
    @mock.patch(DCMProjectManager)
    def test_deploy_project(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=None,
            alias=None,
            output_path=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_from_stage(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(["dcm", "deploy", "fooBar", "--from", "@my_stage"])
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="@my_stage",
            variables=None,
            alias=None,
            output_path=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_variables(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(
            ["dcm", "deploy", "fooBar", "--from", "@my_stage", "-D", "key=value"]
        )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="@my_stage",
            variables=["key=value"],
            alias=None,
            output_path=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_configuration(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(
            [
                "dcm",
                "deploy",
                "fooBar",
                "--from",
                "@my_stage",
                "--configuration",
                "some_configuration",
            ]
        )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration="some_configuration",
            from_stage="@my_stage",
            variables=None,
            alias=None,
            output_path=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_alias(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(
            ["dcm", "deploy", "fooBar", "--from", "@my_stage", "--alias", "my_alias"]
        )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="@my_stage",
            variables=None,
            alias="my_alias",
            output_path=None,
            skip_plan=False,
        )

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_sync(
        self,
        mock_pm,
        _mock_create,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        """Test that files are synced to project stage when from_stage is not provided."""
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "my_project"])
            assert result.exit_code == 0, result.output

        call_args = mock_pm().execute.call_args
        assert "DCM_FOOBAR" in call_args.kwargs["from_stage"]
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_from_local_directory(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        tmp_path,
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        source_dir = tmp_path / "source_project"
        source_dir.mkdir()

        manifest_file = source_dir / "manifest.yml"
        manifest_file.write_text("type: dcm_project\n")

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "deploy", "my_project", "--from", str(source_dir)]
            )
            assert result.exit_code == 0, result.output

        mock_pm().sync_local_files.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            source_directory=str(source_dir),
        )

        call_args = mock_pm().execute.call_args
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")


class TestDCMPlan:
    @mock.patch(DCMProjectManager)
    def test_plan_project(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "plan",
                    "fooBar",
                    "-D",
                    "key=value",
                    "--configuration",
                    "some_configuration",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration="some_configuration",
            from_stage=mock_from_resource(),
            dry_run=True,
            variables=["key=value"],
            output_path=None,
        )

    @mock.patch(DCMProjectManager)
    def test_plan_project_with_from_stage(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(
            [
                "dcm",
                "plan",
                "fooBar",
                "--from",
                "@my_stage",
                "-D",
                "key=value",
                "--configuration",
                "some_configuration",
            ]
        )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration="some_configuration",
            from_stage="@my_stage",
            dry_run=True,
            variables=["key=value"],
            output_path=None,
        )

    @mock.patch(DCMProjectManager)
    def test_plan_project_with_output_path(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(
            [
                "dcm",
                "plan",
                "fooBar",
                "--from",
                "@my_stage",
                "--output-path",
                "@output_stage/results",
            ]
        )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="@my_stage",
            dry_run=True,
            variables=None,
            output_path="@output_stage/results",
        )

    @mock.patch(DCMProjectManager)
    def test_plan_project_with_output_path_and_configuration(
        self, mock_pm, runner, project_directory, mock_cursor
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )

        result = runner.invoke(
            [
                "dcm",
                "plan",
                "fooBar",
                "--from",
                "@my_stage",
                "--configuration",
                "some_config",
                "--output-path",
                "@output_stage",
            ]
        )
        assert result.exit_code == 0, result.output

        mock_pm().execute.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration="some_config",
            from_stage="@my_stage",
            dry_run=True,
            variables=None,
            output_path="@output_stage",
        )

    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    @mock.patch(DCMProjectManager)
    def test_plan_project_with_sync(
        self,
        mock_pm,
        _mock_create,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        """Test that files are synced to project stage when from_stage is not provided."""
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "my_project"])
            assert result.exit_code == 0, result.output

            call_args = mock_pm().execute.call_args
            assert "DCM_FOOBAR_" in call_args.kwargs["from_stage"]
            assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    @mock.patch(DCMProjectManager)
    def test_plan_project_with_from_local_directory(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        tmp_path,
    ):
        mock_pm().execute.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        source_dir = tmp_path / "source_project"
        source_dir.mkdir()
        manifest_file = source_dir / "manifest.yml"
        manifest_file.write_text("type: dcm_project\n")

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "plan", "my_project", "--from", str(source_dir)]
            )
            assert result.exit_code == 0, result.output

        mock_pm().sync_local_files.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            source_directory=str(source_dir),
        )

        call_args = mock_pm().execute.call_args
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")


class TestDCMList:
    def test_list_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "list",
                "dcm",
                "--like",
                "%PROJECT_NAME%",
                "--in",
                "database",
                "my_db",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dcm", "list", "--like", "%PROJECT_NAME%", "--in", "database", "my_db"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            queries[0]
            == queries[1]
            == "show DCM Projects like '%PROJECT_NAME%' in database my_db"
        )

    @pytest.mark.parametrize(
        "terse, limit, expected_query_suffix",
        [
            (True, None, "show terse DCM Projects like '%%'"),
            (False, 10, "show DCM Projects like '%%' limit 10"),
            (False, 5, "show DCM Projects like '%%' limit 5"),
            (True, 10, "show terse DCM Projects like '%%' limit 10"),
        ],
    )
    def test_dcm_list_with_terse_and_limit_options(
        self, mock_connect, terse, limit, expected_query_suffix, runner
    ):
        """Test DCM list command with TERSE and LIMIT options."""
        cmd = ["dcm", "list"]

        if terse:
            cmd.extend(["--terse"])
        if limit is not None:
            cmd.extend(["--limit", str(limit)])

        result = runner.invoke(cmd, catch_exceptions=False)
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 1
        assert queries[0] == expected_query_suffix

    def test_dcm_list_with_all_options_combined(self, mock_connect, runner):
        """Test DCM list command with all options (like, scope, terse, limit) combined."""
        result = runner.invoke(
            [
                "dcm",
                "list",
                "--like",
                "test%",
                "--in",
                "database",
                "my_db",
                "--terse",
                "--limit",
                "20",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 1
        expected_query = (
            "show terse DCM Projects like 'test%' in database my_db limit 20"
        )
        assert queries[0] == expected_query


class TestDCMListDeployments:
    @mock.patch(DCMProjectManager)
    def test_list_deployments(self, mock_pm, runner):
        result = runner.invoke(["dcm", "list-deployments", "fooBar"])

        assert result.exit_code == 0, result.output

        mock_pm().list_deployments.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar")
        )


class TestDCMDropDeployment:
    @mock.patch(DCMProjectManager)
    @pytest.mark.parametrize("if_exists", [True, False])
    def test_drop_deployment(self, mock_pm, runner, if_exists):
        command = ["dcm", "drop-deployment", "fooBar", "v1"]
        if if_exists:
            command.append("--if-exists")

        result = runner.invoke(command)

        assert result.exit_code == 0, result.output
        assert "Deployment 'v1' dropped from DCM Project 'fooBar'" in result.output

        mock_pm().drop_deployment.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            deployment_name="v1",
            if_exists=if_exists,
        )

    @mock.patch(DCMProjectManager)
    @pytest.mark.parametrize(
        "deployment_name,should_warn",
        [
            ("deployment", True),
            ("DEPLOYMENT", True),
            ("Deployment", True),
            ("DEPLOYMENT$1", False),
            ("v1", False),
            ("my_deployment", False),
            ("deployment1", False),
            ("actual_deployment", False),
        ],
    )
    def test_drop_deployment_shell_expansion_warning(
        self, mock_pm, runner, deployment_name, should_warn
    ):
        """Test that warning is displayed for deployment names that look like shell expansion results."""
        result = runner.invoke(["dcm", "drop-deployment", "fooBar", deployment_name])

        assert result.exit_code == 0, result.output

        if should_warn:
            assert "might be truncated due to shell expansion" in result.output
            assert "try using single quotes" in result.output
        else:
            assert "might be truncated due to shell expansion" not in result.output

        mock_pm().drop_deployment.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            deployment_name=deployment_name,
            if_exists=False,
        )


class TestDCMDrop:
    def test_drop_project(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "drop",
                "dcm",
                "my_project",
            ]
        )

        assert result.exit_code == 0, result.output

        result = runner.invoke(
            ["dcm", "drop", "my_project"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert queries[0] == queries[1] == "drop DCM Project IDENTIFIER('my_project')"


class TestDCMDescribe:
    def test_describe_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "describe",
                "dcm",
                "PROJECT_NAME",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dcm", "describe", "PROJECT_NAME"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            queries[0]
            == queries[1]
            == "describe DCM Project IDENTIFIER('PROJECT_NAME')"
        )


class TestDCMTest:
    @mock.patch(DCMProjectManager)
    def test_test_success(self, mock_pm, runner, mock_cursor):
        """Test the test command when all expectations pass."""
        success_result = {
            "status": "SUCCESS",
            "expectations": [
                {
                    "table_name": "JW_DCM_TESTALL.ANALYTICS.EMPLOYEES",
                    "metric_database": "JW_DCM_TESTALL",
                    "metric_schema": "ANALYTICS",
                    "metric_name": "COUNT_BELOW_2",
                    "expectation_name": "LEVELS_MUST_BE_HIGHER_THAN_ZERO",
                    "expectation_expression": "value = 0",
                    "value": 0,
                    "expectation_violated": False,
                },
            ],
        }
        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(success_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 0, result.output
        assert "All 1 expectation(s) passed successfully." in result.output
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_test_with_violated_expectations(self, mock_pm, runner, mock_cursor):
        """Test the test command when expectations are violated."""
        violated_result = {
            "status": "EXPECTATION_VIOLATED",
            "expectations": [
                {
                    "table_name": "JW_DCM_TESTALL.ANALYTICS.EMPLOYEES",
                    "metric_database": "JW_DCM_TESTALL",
                    "metric_schema": "ANALYTICS",
                    "metric_name": "COUNT_BELOW_2",
                    "expectation_name": "LEVELS_MUST_BE_HIGHER_THAN_ZERO",
                    "expectation_expression": "value = 0",
                    "value": 0,
                    "expectation_violated": False,
                },
                {
                    "table_name": "JW_DCM_TESTALL.ANALYTICS.EMPLOYEES",
                    "metric_database": "JW_DCM_TESTALL",
                    "metric_schema": "ANALYTICS",
                    "metric_name": "COUNT_BELOW_1",
                    "expectation_name": "LEVELS_MUST_BE_HIGHER_THAN_ZERO",
                    "expectation_expression": "value = 0",
                    "value": 0,
                    "expectation_violated": False,
                },
                {
                    "table_name": "JW_DCM_TESTALL.ANALYTICS.EMPLOYEES",
                    "metric_database": "JW_DCM_TESTALL",
                    "metric_schema": "ANALYTICS",
                    "metric_name": "COUNT_BELOW_5",
                    "expectation_name": "LEVELS_MUST_BE_HIGHER_THAN_ZERO",
                    "expectation_expression": "value = 0",
                    "value": 4,
                    "expectation_violated": True,
                },
            ],
        }

        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(violated_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 1, result.output
        assert "Tests completed: 2 passed, 1 failed out of 3 total." in result.output
        assert "Failed expectations:" in result.output
        assert "Table: JW_DCM_TESTALL.ANALYTICS.EMPLOYEES" in result.output
        assert "Expectation: LEVELS_MUST_BE_HIGHER_THAN_ZERO" in result.output
        assert "Metric: COUNT_BELOW_5" in result.output
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_test_with_no_expectations(self, mock_pm, runner, mock_cursor):
        """Test the test command when there are no expectations defined."""
        no_expectations_result = {"status": "SUCCESS", "expectations": []}

        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(no_expectations_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 0, result.output
        assert "No expectations defined in the project." in result.output
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_test_with_multiple_failed_expectations(self, mock_pm, runner, mock_cursor):
        """Test the test command with multiple failed expectations from different tables."""
        violated_result = {
            "status": "EXPECTATION_VIOLATED",
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.TABLE_A",
                    "metric_database": "DB",
                    "metric_schema": "SCHEMA",
                    "metric_name": "ROW_COUNT",
                    "expectation_name": "MIN_ROWS",
                    "expectation_expression": "value >= 100",
                    "value": 50,
                    "expectation_violated": True,
                },
                {
                    "table_name": "DB.SCHEMA.TABLE_B",
                    "metric_database": "DB",
                    "metric_schema": "SCHEMA",
                    "metric_name": "NULL_COUNT",
                    "expectation_name": "NO_NULLS",
                    "expectation_expression": "value = 0",
                    "value": 5,
                    "expectation_violated": True,
                },
            ],
        }

        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(violated_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 1, result.output
        assert "Tests completed: 0 passed, 2 failed out of 2 total." in result.output
        assert "Failed expectations:" in result.output
        assert "Table: DB.SCHEMA.TABLE_A" in result.output
        assert "Table: DB.SCHEMA.TABLE_B" in result.output
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )


class TestDCMRefresh:
    @mock.patch(DCMProjectManager)
    def test_refresh_with_outdated_tables(self, mock_pm, runner, mock_cursor, snapshot):
        refresh_result = {
            "refreshed_tables": [
                {
                    "dt_name": "JW_DCM_TESTALL.ANALYTICS.DYNAMIC_EMPLOYEES",
                    "refreshed_dt_count": 1,
                    "data_timestamp": "1760357032.175",
                    "statistics": '{"insertedRows":5,"copiedRows":0,"deletedRows":5}',
                }
            ]
        }
        mock_pm().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "refresh", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_pm().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_refresh_with_fresh_tables(self, mock_pm, runner, mock_cursor, snapshot):
        refresh_result = {
            "refreshed_tables": [
                {
                    "dt_name": "JW_DCM_TESTALL.ANALYTICS.DYNAMIC_EMPLOYEES",
                    "refreshed_dt_count": 0,
                    "data_timestamp": "1760356974.543",
                    "statistics": "No new data",
                }
            ]
        }
        mock_pm().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "refresh", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_pm().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_refresh_with_no_dynamic_tables(
        self, mock_pm, runner, mock_cursor, snapshot
    ):
        refresh_result = {"refreshed_tables": []}
        mock_pm().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "refresh", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_pm().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )
