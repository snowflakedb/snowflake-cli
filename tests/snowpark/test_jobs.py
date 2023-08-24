from pathlib import Path
from tempfile import NamedTemporaryFile
from unittest import mock

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
def test_create_job(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    with NamedTemporaryFile(
        prefix="spec", suffix="yaml", dir=Path(__file__).parent
    ) as fh:
        name = fh.name
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
    assert ctx.get_query() == (
        "create stage if not exists stageValue\n"
        f"put file://{name} "
        "@stageValue auto_compress=false parallel=4 overwrite=True\n"
        "EXECUTE SERVICE\n"
        "COMPUTE_POOL =  jobName\n"
        f"spec=@stageValue/jobs/d41d8cd98f00b204e9800998ecf8427e/{Path(name).stem};\n"
    )


@mock.patch("snowflake.connector.connect")
def test_desc_job(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "jobs", "desc", "jobName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "desc service jobName"


@mock.patch("snowflake.connector.connect")
def test_job_status(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "jobs", "status", "jobName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "CALL SYSTEM$GET_JOB_STATUS('jobName')"


@mock.patch("snowflake.connector.connect")
def test_job_logs(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["snowpark", "jobs", "logs", "--container-name", "containerName", "jobName"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "call SYSTEM$GET_JOB_LOGS('jobName', 'containerName')"


@mock.patch("snowflake.connector.connect")
def test_drop_job(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["snowpark", "jobs", "drop", "cpNameToDrop"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "CALL SYSTEM$CANCEL_JOB('cpNameToDrop')"
