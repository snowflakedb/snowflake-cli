import sys
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


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows snpashot require different approach due to frame corners.",
)
def test_repl_input_handling(repl, capsys, snapshot):
    user_inputs = iter(("select 1;", "exit", "y"))

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ):
        repl.run()

    output = capsys.readouterr().out
    snapshot.assert_match(output)


@pytest.mark.parametrize(
    "user_inputs",
    (
        pytest.param(("exit", "y"), id="exit"),
        pytest.param(("quit", "y"), id="quit"),
        pytest.param(("exit", "n", "exit", "y"), id="hesistate on exit"),
        pytest.param(("quit", "n", "quit", "y"), id="hesistate on quit"),
    ),
)
def test_exit_sequence(user_inputs, repl, snapshot, capsys):
    user_inputs = iter(user_inputs)

    with mock.patch.object(
        repl.session,
        "prompt",
        side_effect=user_inputs,
    ):
        repl.run()

    output = capsys.readouterr().out
    snapshot.assert_match(output)
