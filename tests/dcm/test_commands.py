import json
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.manager import DCMManifest
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


def _manifest_without_config():
    """Helper to create a manifest with target that has no templating_config."""
    return DCMManifest.from_dict(
        {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "dev",
            "targets": {"dev": {"project_name": "ignored"}},
        }
    )


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
        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output

        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=None,
            alias=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_from_stage_fails(
        self, mock_pm, runner, project_directory
    ):
        result = runner.invoke(["dcm", "deploy", "fooBar", "--from", "@my_stage"])
        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_variables(
        self, mock_pm, runner, project_directory, mock_cursor, mock_connect
    ):
        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = "TMP_STAGE"
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar", "-D", "key=value"])
        assert result.exit_code == 0, result.output

        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=["key=value"],
            alias=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_project_with_alias(
        self, mock_pm, runner, project_directory, mock_cursor, mock_connect
    ):
        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = "TMP_STAGE"
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar", "--alias", "my_alias"])
        assert result.exit_code == 0, result.output

        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias="my_alias",
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
        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "my_project"])
            assert result.exit_code == 0, result.output

        call_args = mock_pm().deploy.call_args
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
        mock_pm().deploy.return_value = mock_cursor(
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

        call_args = mock_pm().deploy.call_args
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
        mock_pm().plan.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "plan",
                    "fooBar",
                    "-D",
                    "key=value",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_pm().plan.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=["key=value"],
            save_output=False,
        )

    @mock.patch(DCMProjectManager)
    def test_plan_project_with_save_output(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        mock_pm().plan.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "plan",
                    "fooBar",
                    "--save-output",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_pm().plan.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=None,
            save_output=True,
        )

    @mock.patch(DCMProjectManager)
    def test_plan_project_with_from_stage_fails(
        self, mock_pm, runner, project_directory
    ):
        result = runner.invoke(["dcm", "plan", "fooBar", "--from", "@my_stage"])
        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

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
        mock_pm().plan.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations")
        )
        mock_pm().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "my_project"])
            assert result.exit_code == 0, result.output

            call_args = mock_pm().plan.call_args
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
        mock_pm().plan.return_value = mock_cursor(
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

        call_args = mock_pm().plan.call_args
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
        command = ["dcm", "drop-deployment", "fooBar", "--deployment", "v1"]
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
        result = runner.invoke(
            ["dcm", "drop-deployment", "fooBar", "--deployment", deployment_name]
        )

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

    @mock.patch(DCMProjectManager)
    @mock.patch(ObjectManager)
    def test_drop_with_target_flag(
        self, mock_om, mock_pm, runner, mock_cursor, project_directory
    ):
        mock_om().drop.return_value = mock_cursor(rows=[], columns=("status",))
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "drop", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_om().drop.assert_called_once_with(
            object_type="dcm",
            fqn=FQN.from_string("my_project"),
            if_exists=False,
        )


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

    @mock.patch(DCMProjectManager)
    @mock.patch(ObjectManager)
    def test_describe_with_target_flag(
        self, mock_om, mock_pm, runner, mock_cursor, project_directory
    ):
        mock_om().describe.return_value = mock_cursor(rows=[], columns=("name",))
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "describe", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_om().describe.assert_called_once_with(
            object_type="dcm",
            fqn=FQN.from_string("my_project"),
        )


class TestDCMPreview:
    @mock.patch(DCMProjectManager)
    def test_preview_basic(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        mock_pm().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")],
            columns=("id", "name", "email"),
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "preview", "my_project", "--object", "my_table"]
            )

        assert result.exit_code == 0, result.output

        mock_pm().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=None,
            limit=None,
        )

    @mock.patch(DCMProjectManager)
    def test_preview_with_from_stage_fails(self, mock_pm, runner, project_directory):
        result = runner.invoke(
            [
                "dcm",
                "preview",
                "my_project",
                "--object",
                "my_table",
                "--from",
                "@my_stage",
            ]
        )
        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

    @mock.patch(DCMProjectManager)
    @pytest.mark.parametrize(
        "extra_args,expected_vars,expected_limit",
        [
            (
                ["-D", "key=value", "--limit", "10"],
                ["key=value"],
                10,
            ),
            (
                ["-D", "var1=val1", "-D", "var2=val2", "--limit", "5"],
                ["var1=val1", "var2=val2"],
                5,
            ),
            (
                ["--limit", "100"],
                None,
                100,
            ),
        ],
    )
    def test_preview_with_various_options(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        extra_args,
        expected_vars,
        expected_limit,
    ):
        mock_pm().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com")],
            columns=("id", "name", "email"),
        )
        mock_pm().sync_local_files.return_value = "TMP_STAGE"
        mock_pm.load_manifest.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "preview",
                    "my_project",
                    "--object",
                    "my_table",
                ]
                + extra_args
            )
        assert result.exit_code == 0, result.output

        mock_pm().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=expected_vars,
            limit=expected_limit,
        )

    def test_preview_without_object_fails(self, runner, project_directory):
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "preview", "my_project"])

        assert result.exit_code == 2
        assert "Missing option '--object'" in result.output


class TestDCMRefresh:
    @mock.patch(DCMProjectManager)
    def test_refresh_with_outdated_tables(self, mock_pm, runner, mock_cursor, snapshot):
        refresh_result = {
            "refreshed_tables": [
                {
                    "dt_name": "JW_DCM_TESTALL.ANALYTICS.DYNAMIC_EMPLOYEES",
                    "refreshed_dt_count": 1,
                    "data_timestamp": "1760357032.175",
                    "statistics": '{"insertedRows":12345,"copiedRows":0,"deletedRows":999999999995}',
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


class TestDCMTest:
    @mock.patch(DCMProjectManager)
    def test_test_all_passing(self, mock_pm, runner, mock_cursor, snapshot):
        test_result = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.EMPLOYEES",
                    "expectation_name": "ROW_COUNT_CHECK",
                    "expectation_violated": False,
                },
                {
                    "table_name": "DB.SCHEMA.ORDERS",
                    "expectation_name": "NULL_CHECK",
                    "expectation_violated": False,
                },
            ]
        }
        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_test_with_failures(self, mock_pm, runner, mock_cursor, snapshot):
        test_result = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.EMPLOYEES",
                    "expectation_name": "ROW_COUNT_CHECK",
                    "expectation_violated": False,
                },
                {
                    "table_name": "DB.SCHEMA.ORDERS",
                    "expectation_name": "NULL_CHECK",
                    "expectation_violated": True,
                    "expectation_expression": "= 0",
                    "metric_name": "null_count",
                    "value": 15,
                },
            ]
        }
        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 1, result.output
        assert result.output == snapshot
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_test_no_expectations(self, mock_pm, runner, mock_cursor, snapshot):
        test_result = {"expectations": []}
        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )


class TestDCMTargetFlag:
    """Tests for --target flag functionality.

    These tests verify that the --target flag correctly resolves project
    identifiers from the manifest.yml targets section.
    """

    @mock.patch(DCMProjectManager)
    def test_deploy_with_target_flag(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations",)
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=None,
            alias=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_with_default_target(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations",)
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy"])

        assert result.exit_code == 0, result.output
        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration=None,
            from_stage=mock_from_resource(),
            variables=None,
            alias=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_explicit_identifier_still_uses_target_config(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        """When explicit identifier is provided, it overrides target's project_name
        but configuration from target should still be applied."""
        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations",)
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {
                        "project_name": "target_project",
                        "templating_config": "dev_config",
                    }
                },
                "templating": {"configurations": {"dev_config": {}}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "deploy", "explicit_project", "--target", "dev"]
            )

        assert result.exit_code == 0, result.output
        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("explicit_project"),
            configuration="dev_config",
            from_stage=mock_from_resource(),
            variables=None,
            alias=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_deploy_with_target_uses_configuration(
        self,
        mock_pm,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_from_resource,
    ):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        mock_pm().deploy.return_value = mock_cursor(
            rows=[("[]",)], columns=("operations",)
        )
        mock_pm().sync_local_files.return_value = mock_from_resource()
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {
                        "project_name": "my_project",
                        "templating_config": "dev_config",
                    }
                },
                "templating": {"configurations": {"dev_config": {}}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_pm().deploy.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration="dev_config",
            from_stage=mock_from_resource(),
            variables=None,
            alias=None,
            skip_plan=False,
        )

    @mock.patch(DCMProjectManager)
    def test_refresh_with_target_flag(
        self, mock_pm, runner, mock_cursor, project_directory
    ):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        refresh_result = {"refreshed_tables": []}
        mock_pm().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "refresh", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_pm().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_test_with_target_flag(
        self, mock_pm, runner, mock_cursor, project_directory
    ):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        test_result = {"expectations": []}
        mock_pm().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "test", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_pm().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    def test_list_deployments_with_target_flag(
        self, mock_pm, runner, mock_cursor, project_directory
    ):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        mock_pm().list_deployments.return_value = mock_cursor(
            rows=[], columns=("name",)
        )
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "list-deployments", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_pm().list_deployments.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    @mock.patch(DCMProjectManager)
    @mock.patch(ObjectManager)
    def test_create_with_target_flag(self, mock_om, mock_pm, runner, project_directory):
        from snowflake.cli._plugins.dcm.manager import DCMManifest

        mock_om().object_exists.return_value = False
        mock_pm.load_manifest.return_value = DCMManifest.from_dict(
            {
                "manifest_version": "2.0",
                "type": "dcm_project",
                "targets": {"dev": {"project_name": "my_project"}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "create", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_pm().create.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )
