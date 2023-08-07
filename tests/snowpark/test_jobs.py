from tempfile import NamedTemporaryFile
from unittest import mock


@mock.patch("snowflake.connector.connect")
def test_create_job(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with NamedTemporaryFile(suffix="yaml") as fh:
        result = runner.invoke(
            [
                "snowpark",
                "jobs",
                "create",
                "--compute-pool",
                "jobName",
                "--spec-path",
                fh.name,
                "--stage",
                "stageValue",
            ]
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_desc_job(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "jobs", "desc", "jobName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_job_status(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "jobs", "status", "jobName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_job_logs(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["snowpark", "jobs", "logs", "--container-name", "containerName", "jobName"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_drop_job(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "jobs", "drop", "cpNameToDrop"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot
