from tempfile import NamedTemporaryFile
from unittest import mock


@mock.patch("snowflake.connector.connect")
def test_create_service(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with NamedTemporaryFile() as fh:
        result = runner.invoke(
            [
                "snowpark",
                "services",
                "create",
                "--name",
                "serviceName",
                "--compute_pool",
                "computePoolValue",
                "--spec_path",
                fh.name,
                "--num_instances",
                42,
                "--stage",
                "stagName",
            ]
        )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_list_service(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "list"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_drop_service(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "drop", "serviceName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_service_status(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "status", "serviceName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot


@mock.patch("snowflake.connector.connect")
def test_service_logs(mock_connector, runner, mock_ctx, snapshot):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        [
            "snowpark",
            "services",
            "logs",
            "--container_name",
            "containerName",
            "serviceName",
        ]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == snapshot
