from tempfile import TemporaryDirectory

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.connector.connect")
def test_create_job(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    test_spec = """
spec:
  containers:
  - name: main
    image: public.ecr.aws/myrepo:latest
    """
    with TemporaryDirectory() as temp_dir:
        filepath = os.path.join(temp_dir, "test")
        with open(filepath, "w") as fh:
            fh.write(test_spec)
        runner.invoke(
            [
                "spcs",
                "job",
                "create",
                "--compute-pool",
                "testPool",
                "--spec-path",
                filepath,
            ]
        )
    assert ctx.get_query() == (
        "USE DATABASE MockDatabase\n"
        "USE MockDatabase.MockSchema\n"
        "EXECUTE SERVICE\n"
        "IN COMPUTE POOL testPool\n"
        "FROM SPECIFICATION $$\n"
        '{"spec": {"containers": [{"name": "main", "image": "public.ecr.aws/myrepo:latest"}]}}\n'
        "$$\n"
    )


@mock.patch("snowflake.connector.connect")
def test_job_status(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["spcs", "job", "status", "jobName"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "CALL SYSTEM$GET_JOB_STATUS('jobName')"


@mock.patch("snowflake.connector.connect")
def test_job_logs(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(
        ["spcs", "job", "logs", "--container-name", "containerName", "jobName"]
    )

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "call SYSTEM$GET_JOB_LOGS('jobName', 'containerName')"
