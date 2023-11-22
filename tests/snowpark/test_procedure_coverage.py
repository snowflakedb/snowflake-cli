from unittest import mock

IDENTIFIER = "test_procedure(a int, b string)"


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.coverage")
@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_report_error_when_no_report_on_stage(
    mock_connector, mock_stage_manager, mock_coverage, runner, mock_ctx, mock_cursor
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().get.return_value = mock_cursor(rows=[], columns=[])
    mock_combined_coverage = mock.Mock()
    mock_coverage.Coverage.return_value = mock_combined_coverage

    result = runner.invoke(
        [
            "snowpark",
            "coverage",
            "report",
            IDENTIFIER,
        ]
    )

    assert result.exit_code == 1
    assert not mock_combined_coverage.combine.called
    assert ctx.get_query() == ""


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.coverage")
@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_report_create_default_report(
    mock_connector,
    mock_stage_manager,
    mock_coverage,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().get.return_value = mock_cursor(
        rows=[["1.coverage"]], columns=[]
    )
    mock_combined_coverage = mock.Mock()
    mock_coverage.Coverage.return_value = mock_combined_coverage

    with project_directory("snowpark_procedures_coverage"):
        result = runner.invoke(
            [
                "snowpark",
                "coverage",
                "report",
                IDENTIFIER,
            ]
        )

    assert result.exit_code == 0, result.output
    tmp_dir_path = mock_stage_manager().get.call_args.kwargs["dest_path"]
    mock_combined_coverage.combine.assert_called_once_with(
        data_paths=[str(tmp_dir_path / "1.coverage")]
    )
    assert mock_combined_coverage.html_report.called
    assert ctx.get_query() == ""


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.coverage")
@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_report_create_html_report(
    mock_connector,
    mock_stage_manager,
    mock_coverage,
    runner,
    mock_ctx,
    mock_cursor,
    snapshot,
    project_directory,
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().get.return_value = mock_cursor(
        rows=[["1.coverage"]], columns=[]
    )
    mock_combined_coverage = mock.Mock()
    mock_coverage.Coverage.return_value = mock_combined_coverage

    with project_directory("snowpark_procedures_coverage"):
        result = runner.invoke(
            [
                "snowpark",
                "coverage",
                "report",
                IDENTIFIER,
                "--output-format",
                "html",
            ]
        )

    assert result.exit_code == 0, result.output
    assert result.output.strip() == snapshot
    tmp_dir_path = mock_stage_manager().get.call_args.kwargs["dest_path"]
    mock_combined_coverage.combine.assert_called_once_with(
        data_paths=[str(tmp_dir_path / "1.coverage")]
    )
    assert mock_combined_coverage.html_report.called
    assert ctx.get_query() == ""


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.coverage")
@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_report_create_json_report(
    mock_connector,
    mock_stage_manager,
    mock_coverage,
    runner,
    mock_ctx,
    mock_cursor,
    snapshot,
    project_directory,
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().get.return_value = mock_cursor(
        rows=[["1.coverage"]], columns=[]
    )
    mock_combined_coverage = mock.Mock()
    mock_coverage.Coverage.return_value = mock_combined_coverage

    with project_directory("snowpark_procedures_coverage"):
        result = runner.invoke(
            [
                "snowpark",
                "coverage",
                "report",
                IDENTIFIER,
                "--output-format",
                "json",
            ]
        )

    assert result.exit_code == 0
    assert result.output.strip() == snapshot
    tmp_dir_path = mock_stage_manager().get.call_args.kwargs["dest_path"]
    mock_combined_coverage.combine.assert_called_once_with(
        data_paths=[str(tmp_dir_path / "1.coverage")]
    )
    assert mock_combined_coverage.json_report.called
    assert ctx.get_query() == ""


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.coverage")
@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_report_create_lcov_report(
    mock_connector,
    mock_stage_manager,
    mock_coverage,
    runner,
    mock_ctx,
    mock_cursor,
    snapshot,
    project_directory,
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().get.return_value = mock_cursor(
        rows=[["1.coverage"]], columns=[]
    )
    mock_combined_coverage = mock.Mock()
    mock_coverage.Coverage.return_value = mock_combined_coverage

    with project_directory("snowpark_procedures_coverage"):
        result = runner.invoke(
            [
                "snowpark",
                "coverage",
                "report",
                IDENTIFIER,
                "--output-format",
                "lcov",
            ]
        )

    assert result.exit_code == 0
    assert result.output.strip() == snapshot
    tmp_dir_path = mock_stage_manager().get.call_args.kwargs["dest_path"]
    mock_combined_coverage.combine.assert_called_once_with(
        data_paths=[str(tmp_dir_path / "1.coverage")]
    )
    assert mock_combined_coverage.lcov_report.called
    assert ctx.get_query() == ""


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.coverage")
@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_report_store_as_comment(
    mock_connector,
    mock_stage_manager,
    mock_coverage,
    runner,
    mock_ctx,
    mock_cursor,
    project_directory,
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().get.return_value = mock_cursor(
        rows=[["1.coverage"]], columns=[]
    )
    mock_combined_coverage = mock.Mock()
    mock_coverage.Coverage.return_value = mock_combined_coverage
    percentage = 91
    mock_combined_coverage.html_report.return_value = percentage

    with project_directory("snowpark_procedures_coverage"):
        result = runner.invoke(
            [
                "snowpark",
                "coverage",
                "report",
                IDENTIFIER,
                "--store-as-comment",
            ]
        )

    assert result.exit_code == 0
    tmp_dir_path = mock_stage_manager().get.call_args.kwargs["dest_path"]
    mock_combined_coverage.combine.assert_called_once_with(
        data_paths=[str(tmp_dir_path / "1.coverage")]
    )
    assert mock_combined_coverage.html_report.called
    assert (
        ctx.get_query()
        == "ALTER PROCEDURE test_procedure(a int, b string) SET COMMENT = $$91$$"
    )


@mock.patch("snowcli.cli.snowpark.procedure_coverage.manager.StageManager")
@mock.patch("snowflake.connector.connect")
def test_procedure_coverage_clear(
    mock_connector, mock_stage_manager, runner, mock_ctx, mock_cursor, project_directory
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_stage_manager().remove.return_value = mock_cursor(rows=[], columns=[])

    with project_directory("snowpark_procedures_coverage"):
        result = runner.invoke(
            [
                "snowpark",
                "coverage",
                "clear",
            ]
        )

    assert result.exit_code == 0
    mock_stage_manager().remove.assert_called_once_with(
        stage_name="@dev_deployment/my_snowpark_project/coverage",
        path="",
    )
