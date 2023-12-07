from tests.testing_utils.fixtures import *


def test_global(runner):
    result = runner.invoke(["-h"])

    assert result.exit_code == 0

    assert "SnowCLI - A CLI for Snowflake " in result.output


@pytest.mark.parametrize(
    "namespace, expected",
    [
        ("object", "Manages Snowflake objects"),
        ("snowpark", "Manage procedures and functions."),
        ("streamlit", " Manages Streamlit in Snowflake."),
    ],
)
def test_namespace(namespace, expected, runner):
    result = runner.invoke([namespace, "-h"])

    assert result.exit_code == 0

    assert expected in result.output


def test_a(runner): #TODO remove this
    os.chdir("/Users/jsikorski/PycharmProjects/test/example_snowpark")
    result = runner.invoke(["snowpark", "build", "--pypi-download", "yes"])
    assert result.exit_code == 0
