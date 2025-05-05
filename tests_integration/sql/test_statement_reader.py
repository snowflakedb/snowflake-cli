import pytest


@pytest.mark.integration
def test_source_with_variables_and_templating_substitution(
    runner, tmp_path_factory, snapshot
):
    source_foo = tmp_path_factory.mktemp("data") / "source_foo.sql"
    source_foo.write_text("select '<% ctx.env.Test %>';")

    result = runner.invoke_with_connection(
        (
            "sql",
            "-q",
            f"select 1; !source {source_foo.parent / 'source_&value.sql'} ",
            "-D value=foo",
            "--env",
            "Test=73",
        )
    )
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@pytest.mark.integration
def test_sql_source_command_from_user_input(runner, tmp_path_factory, snapshot):
    include_file = tmp_path_factory.mktemp("data") / "include.sql"
    include_file.write_text("select 42;")

    result = runner.invoke_with_connection(
        (
            "sql",
            "-q",
            f"select 1; !source {include_file.as_posix()}; select 3;",
        )
    )

    assert result.output == snapshot
    assert result.exit_code == 0, result.output
