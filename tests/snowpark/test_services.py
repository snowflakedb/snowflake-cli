from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
def test_create_service(mock_connector, runner, mock_ctx):
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
    assert ctx.get_query() == (
        (
            "create stage if not exists stagName\n"
            f"put file://{fh.name} "
            "@stagName auto_compress=false parallel=4 overwrite=True\n"
            "CREATE SERVICE IF NOT EXISTS serviceName\n"
            "MIN_INSTANCES = 42\n"
            "MAX_INSTANCES = 42\n"
            "COMPUTE_POOL =  computePoolValue\n"
            f"spec=@stagName/jobs/d41d8cd98f00b204e9800998ecf8427e/{Path(fh.name).stem};\n"
        )
    )


@mock.patch("snowflake.connector.connect")
def test_desc_service(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    service_name = "test_service"

    result = runner.invoke(["snowpark", "services", "desc", service_name])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == f"desc service {service_name}"


@mock.patch("snowflake.connector.connect")
def test_list_service(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "list"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show services"


@mock.patch("snowflake.connector.connect")
def test_drop_service(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "drop", "serviceName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "drop service serviceName"


@mock.patch("snowflake.connector.connect")
def test_service_status(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "services", "status", "serviceName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "CALL SYSTEM$GET_SERVICE_STATUS(('serviceName')"


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
    assert (
        ctx.get_query()
        == "call SYSTEM$GET_SERVICE_LOGS('serviceName', '0', 'containerName');"
    )
