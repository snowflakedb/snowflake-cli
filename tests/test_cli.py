from typer.testing import CliRunner

runner = CliRunner()


def test_global(snapshot):
    from snowcli.cli import app

    result = runner.invoke(app, ["-h"])

    assert result.exit_code == 0

    assert result.output == snapshot


def test_streamlit(snapshot):
    from snowcli.cli.streamlit import app

    result = runner.invoke(app, ["-h"])

    assert result.exit_code == 0

    assert result.output == snapshot


def test_stage(snapshot):
    from snowcli.cli.stage import app

    result = runner.invoke(app, ["-h"])

    assert result.exit_code == 0

    assert result.output == snapshot


def test_snowpark(snapshot):
    from snowcli.cli.snowpark import app

    result = runner.invoke(app, ["-h"])

    assert result.exit_code == 0

    assert result.output == snapshot


def test_warehouse(snapshot):
    from snowcli.cli.warehouse import app

    result = runner.invoke(app, ["-h"])

    assert result.exit_code == 0

    assert result.output == snapshot
