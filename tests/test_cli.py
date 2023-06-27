import pytest


def test_global(runner, snapshot):
    result = runner.invoke(["-h"])

    assert result.exit_code == 0

    assert result.output == snapshot


@pytest.mark.parametrize(
    "namespace", [("warehouse"), ("streamlit"), ("stage"), ("snowpark")]
)
def test_namespace(namespace, runner, snapshot):
    result = runner.invoke([namespace, "-h"])

    assert result.exit_code == 0

    assert result.output == snapshot
