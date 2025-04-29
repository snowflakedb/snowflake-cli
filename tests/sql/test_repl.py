from unittest import mock

import pytest
from snowflake.cli._plugins.sql.manager import SqlManager
from snowflake.cli._plugins.sql.repl import Repl


@pytest.fixture(name="repl")
def make_repl(mock_cursor):
    mocked_cursor = [
        mock_cursor(
            rows=[("1",)],
            columns=["1"],
        ),
    ]

    with mock.patch.object(SqlManager, "_execute_string", return_value=mocked_cursor):
        manager = SqlManager()
        repl = Repl(manager)
        yield repl


def test_repl_input_handling(repl, capsys, os_agnostic_snapshot):
    user_inputs = iter(("select 1;", "exit", "y"))

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ):
        repl.run()

    output = capsys.readouterr().out
    os_agnostic_snapshot.assert_match(output)


@pytest.mark.parametrize(
    "user_inputs",
    (
        pytest.param(("exit", "y"), id="exit"),
        pytest.param(("quit", "y"), id="quit"),
        pytest.param(("exit", "n", "exit", "y"), id="hesistate on exit"),
        pytest.param(("quit", "n", "quit", "y"), id="hesistate on quit"),
    ),
)
def test_exit_sequence(user_inputs, repl, os_agnostic_snapshot, capsys):
    user_inputs = iter(user_inputs)

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ):
        repl.run()

    output = capsys.readouterr().out
    os_agnostic_snapshot.assert_match(output)


def test_repl_full_app(runner, os_agnostic_snapshot, mock_cursor):
    user_inputs = iter(("exit", "y"))
    mocked_cursor = [
        mock_cursor(
            rows=[("1",)],
            columns=("1",),
        ),
    ]

    repl_prompt = "snowflake.cli._plugins.sql.repl.PromptSession"
    repl_execute = "snowflake.cli._plugins.sql.repl.Repl._execute"

    with mock.patch(repl_prompt) as mock_prompt:
        mock_instance = mock.MagicMock()
        mock_instance.prompt.side_effect = user_inputs
        mock_prompt.return_value = mock_instance

        with mock.patch(repl_execute, return_value=mocked_cursor):
            result = runner.invoke(("sql",))
            assert result.exit_code == 0
            os_agnostic_snapshot.assert_match(result.output)
