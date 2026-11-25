# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import csv
import json
import logging
import sys
from io import StringIO
from textwrap import dedent

import pytest
from click.exceptions import BadOptionUsage, BadParameter, NoSuchOption, UsageError
from snowflake.cli._app.printing import (
    exception_payload,
    install_structured_error_rendering,
    print_result,
    print_structured_error,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError, MissingConfigurationError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import MessageResult


def test_exception_payload_includes_type_and_exit_code():
    payload = exception_payload(CliError("connection failed"))

    assert payload["error"] == "connection failed"
    assert payload["type"] == "CliError"
    assert payload["exit_code"] == 1
    assert "traceback" not in payload


def test_exception_payload_preserves_zero_exit_code():
    err = CliError("dry run failed")
    err.exit_code = 0

    assert exception_payload(err)["exit_code"] == 0


def test_exception_payload_non_numeric_exit_code_defaults_to_one():
    err = CliError("boom")
    err.exit_code = "nope"

    assert exception_payload(err)["exit_code"] == 1


def test_exception_payload_strips_ansi_from_error():
    payload = exception_payload(CliError("\x1b[31mboom\x1b[0m"))

    assert payload["error"] == "boom"
    assert "\x1b" not in payload["error"]


def test_exception_payload_includes_traceback_when_debug(monkeypatch):
    get_cli_context_manager().enable_tracebacks = True
    try:
        raise CliError("boom")
    except CliError as err:
        payload = exception_payload(err)

    assert payload["error"] == "boom"
    assert "CliError" in payload["traceback"]
    assert "boom" in payload["traceback"]


def test_print_structured_error_is_a_noop_for_table(capsys):
    get_cli_context_manager().output_format = OutputFormat.TABLE

    handled = print_structured_error(CliError("boom"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


@pytest.mark.parametrize("fmt", (OutputFormat.JSON, OutputFormat.JSON_EXT))
def test_print_structured_error_json(capsys, fmt):
    get_cli_context_manager().output_format = fmt

    handled = print_structured_error(
        MissingConfigurationError("Connection x is not configured")
    )

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "error": "Connection x is not configured",
        "type": "MissingConfigurationError",
        "exit_code": 1,
    }


def test_print_structured_error_csv(capsys):
    get_cli_context_manager().output_format = OutputFormat.CSV

    handled = print_structured_error(CliError("boom"))

    assert handled is True
    assert capsys.readouterr().err == dedent(
        """\
        error,type,exit_code
        boom,CliError,1
        """
    )


def test_print_structured_error_csv_strips_ansi(capsys):
    get_cli_context_manager().output_format = OutputFormat.CSV

    handled = print_structured_error(CliError("\x1b[31mboom\x1b[0m"))

    assert handled is True
    err = capsys.readouterr().err
    assert "\x1b" not in err
    rows = list(csv.reader(StringIO(err)))
    assert rows[1][0] == "boom"


def test_print_structured_error_csv_escapes_formula_prefix(capsys):
    get_cli_context_manager().output_format = OutputFormat.CSV

    handled = print_structured_error(CliError('=cmd|"/c calc"!A1'))

    assert handled is True
    rows = list(csv.reader(StringIO(capsys.readouterr().err)))
    assert rows[1][0].startswith("'=")


def test_print_structured_error_skips_no_args_is_help(capsys):
    class NoArgsIsHelpError(CliError):
        pass

    get_cli_context_manager().output_format = OutputFormat.JSON
    handled = print_structured_error(NoArgsIsHelpError("unused"))

    assert handled is False
    assert capsys.readouterr().err == ""


def test_missing_connection_json_error_is_parseable(runner):
    result = runner.invoke(
        ["connection", "set-default", "no-such-conn", "--format", "JSON"]
    )

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "MissingConfigurationError"
    assert "no-such-conn" in payload["error"]
    assert payload["exit_code"] == 1
    assert "Error ---" not in result.output


def test_missing_connection_table_error_keeps_rich_panel(runner):
    result = runner.invoke(["connection", "set-default", "no-such-conn"])

    assert result.exit_code != 0
    assert "Connection no-such-conn is not configured" in result.output
    assert "Error" in result.output
    assert '"type"' not in result.output


def test_missing_connection_csv_error_is_parseable(runner):
    result = runner.invoke(
        ["connection", "set-default", "no-such-conn", "--format", "CSV"]
    )

    assert result.exit_code != 0
    lines = [line for line in result.output.splitlines() if line]
    assert lines[0] == "error,type,exit_code"
    assert "no-such-conn" in lines[1]
    assert "MissingConfigurationError" in lines[1]
    assert "Error ---" not in result.output


def test_unexpected_exception_json_error(capsys):
    from snowflake.cli._app.main_typer import _handle_exception

    get_cli_context_manager().output_format = OutputFormat.JSON
    with pytest.raises(SystemExit) as exit_info:
        _handle_exception(RuntimeError("internal failure"))

    assert exit_info.value.code == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"] == "internal failure"
    assert payload["type"] == "RuntimeError"
    assert payload["exit_code"] == 1


def test_unexpected_exception_table_keeps_warning(capsys):
    from snowflake.cli._app.main_typer import _handle_exception

    get_cli_context_manager().output_format = OutputFormat.TABLE
    with pytest.raises(SystemExit) as exit_info:
        _handle_exception(RuntimeError("internal failure"))

    assert exit_info.value.code == 1
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "unexpected exception" in combined
    assert "internal failure" in combined


def test_print_structured_error_reads_format_from_argv_for_no_such_option(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "sql", "--not-a-real-flag", "--format", "JSON", "-q", "select 1"],
    )

    handled = print_structured_error(NoSuchOption("--not-a-real-flag"))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "NoSuchOption"
    assert "--not-a-real-flag" in payload["error"]


def test_print_structured_error_honors_env_format_for_no_such_option(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--foobar"])

    handled = print_structured_error(NoSuchOption("--foobar"))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "NoSuchOption"
    assert "--foobar" in payload["error"]


def test_print_structured_error_argv_format_overrides_env_for_no_such_option(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--format", "table", "--foobar"])

    handled = print_structured_error(NoSuchOption("--foobar"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_print_structured_error_honors_config_format_for_no_such_option(
    capsys, monkeypatch, with_custom_config
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.delenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", raising=False)
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--foobar"])

    with with_custom_config({"cli": {"output_format": "JSON"}}):
        handled = print_structured_error(NoSuchOption("--foobar"))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "NoSuchOption"


def test_print_structured_error_ignores_invalid_env_format_for_no_such_option(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "xml")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--foobar"])

    handled = print_structured_error(NoSuchOption("--foobar"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_print_structured_error_uses_first_format_when_passthrough_follows_typo(
    capsys, monkeypatch
):
    # snow dbt execute --format table --dbt-versoin … compile --format=json
    # must stay TABLE: the trailing --format is dbt's, not Snowflake CLI's.
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "snow",
            "dbt",
            "execute",
            "--format",
            "table",
            "--dbt-versoin",
            "1.7",
            "pipeline_name",
            "compile",
            "--format=json",
        ],
    )

    handled = print_structured_error(NoSuchOption("--dbt-versoin"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_print_structured_error_honors_cli_json_before_passthrough_on_typo(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "snow",
            "dbt",
            "execute",
            "--format",
            "JSON",
            "--dbt-versoin",
            "1.7",
            "pipeline_name",
            "compile",
            "--format=csv",
        ],
    )

    handled = print_structured_error(NoSuchOption("--dbt-versoin"))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "NoSuchOption"
    assert "--dbt-versoin" in payload["error"]


def test_print_structured_error_ignores_lone_passthrough_format_after_typo(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "snow",
            "dbt",
            "execute",
            "--dbt-versoin",
            "1.7",
            "pipeline_name",
            "compile",
            "--format=json",
        ],
    )

    handled = print_structured_error(NoSuchOption("--dbt-versoin"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_print_structured_error_sees_format_after_typo_before_positionals(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "snow",
            "dbt",
            "execute",
            "--dbt-versoin",
            "1.7",
            "--format",
            "JSON",
            "pipeline_name",
            "compile",
            "--format=csv",
        ],
    )

    handled = print_structured_error(NoSuchOption("--dbt-versoin"))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "NoSuchOption"


def test_print_structured_error_last_cli_format_wins_before_unknown_option(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "sql", "--format", "JSON", "--format", "CSV", "--not-a-real-flag"],
    )

    handled = print_structured_error(NoSuchOption("--not-a-real-flag"))

    assert handled is True
    captured = capsys.readouterr()
    assert captured.err.startswith("error,type,exit_code")


def test_print_structured_error_ignores_format_after_end_of_options(
    capsys, monkeypatch
):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "sql", "--not-a-real-flag", "--", "--format=json"],
    )

    handled = print_structured_error(NoSuchOption("--not-a-real-flag"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_print_structured_error_ignores_passthrough_format_in_argv(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "snow",
            "dbt",
            "execute",
            "--format",
            "table",
            "my_pipeline",
            "compile",
            "--format=json",
        ],
    )

    handled = print_structured_error(CliError("boom"))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_print_result_ignores_unparsed_format_in_argv(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "dbt", "execute", "proj", "run", "--format=json"],
    )

    print_result(MessageResult("ok"))

    assert capsys.readouterr().out == "ok\n"


def test_invalid_port_before_format_still_emits_json(runner):
    result = runner.invoke(
        ["sql", "--port", "notanint", "--format", "JSON", "-q", "select 1"]
    )

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert "notanint" in payload["error"]
    assert "Error ---" not in result.output


def test_unknown_option_with_format_json_emits_json(runner, monkeypatch):
    # CliRunner does not put command args in sys.argv; production does, and
    # NoSuchOption is raised while tokenizing — before any --format callback.
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "sql", "--not-a-real-flag", "--format", "JSON", "-q", "select 1"],
    )
    result = runner.invoke(
        ["sql", "--not-a-real-flag", "--format", "JSON", "-q", "select 1"]
    )

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "NoSuchOption"
    assert "Error ---" not in result.output


def test_unknown_option_with_env_format_json_emits_json(runner, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--foobar"])
    result = runner.invoke(["sql", "--foobar"])

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "NoSuchOption"
    assert "--foobar" in payload["error"]
    assert "Error ---" not in result.output


def test_unknown_option_format_flag_overrides_env(runner, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--format", "table", "--foobar"])
    result = runner.invoke(["sql", "--format", "table", "--foobar"])

    assert result.exit_code != 0
    assert "Error" in result.output
    assert '"type"' not in result.output


def test_missing_option_value_honors_explicit_format(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--format", "JSON", "--warehouse"])

    handled = print_structured_error(
        BadOptionUsage("--warehouse", "Option '--warehouse' requires an argument.")
    )

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "BadOptionUsage"
    assert "traceback" not in payload


def test_missing_format_value_honors_env(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--format"])

    handled = print_structured_error(
        BadOptionUsage("--format", "Option '--format' requires an argument.")
    )

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "BadOptionUsage"
    assert "requires an argument" in payload["error"]


def test_unknown_command_honors_format_after_command_name(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.delenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", raising=False)
    monkeypatch.setattr(sys, "argv", ["snow", "nosuchcommand", "--format", "JSON"])

    handled = print_structured_error(UsageError("No such command 'nosuchcommand'."))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "UsageError"
    assert "nosuchcommand" in payload["error"]


def test_unknown_subcommand_honors_format_flag(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.delenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", raising=False)
    monkeypatch.setattr(
        sys, "argv", ["snow", "connection", "nosuch", "--format", "JSON"]
    )

    handled = print_structured_error(UsageError("No such command 'nosuch'."))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert payload["type"] == "UsageError"
    assert "nosuch" in payload["error"]


def test_unknown_command_ignores_format_after_later_positional(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.delenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "nosuchcommand", "extra", "--format", "JSON"],
    )

    handled = print_structured_error(UsageError("No such command 'nosuchcommand'."))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.err == ""


def test_unknown_command_honors_env_not_passthrough_format(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "csv")
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "dbt", "execute", "pipeline", "compile", "--format=json"],
    )

    handled = print_structured_error(UsageError("No such command 'nosuch'."))

    assert handled is True
    captured = capsys.readouterr().err
    assert captured.startswith("error,type,exit_code")
    assert "UsageError" in captured


def test_unknown_command_ignores_passthrough_format_without_env(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.delenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        ["snow", "dbt", "execute", "pipeline", "compile", "--format=json"],
    )

    handled = print_structured_error(UsageError("No such command 'nosuch'."))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_bad_parameter_keeps_parsed_format(capsys, monkeypatch):
    get_cli_context_manager().output_format = OutputFormat.TABLE
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(
        sys, "argv", ["snow", "sql", "--port", "nope", "--format", "JSON"]
    )

    handled = print_structured_error(BadParameter("'nope' is not a valid integer."))

    assert handled is False
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_install_clears_default_tracebacks(capsys):
    get_cli_context_manager().enable_tracebacks = True
    get_cli_context_manager().output_format = OutputFormat.JSON
    install_structured_error_rendering()

    assert get_cli_context_manager().enable_tracebacks is False
    handled = print_structured_error(NoSuchOption("--foobar"))

    assert handled is True
    payload = json.loads(capsys.readouterr().err)
    assert "traceback" not in payload


def test_missing_warehouse_value_with_format_json_emits_json(runner, monkeypatch):
    # CliRunner does not put command args in sys.argv; production does.
    # BadOptionUsage is raised while tokenizing, before --format's callback.
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--format", "JSON", "--warehouse"])
    result = runner.invoke(["sql", "--format", "JSON", "--warehouse"])

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "BadOptionUsage"
    assert "traceback" not in payload
    assert "Error ---" not in result.output


def test_missing_format_value_with_env_emits_json(runner, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--format"])
    result = runner.invoke(["sql", "--format"])

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "BadOptionUsage"
    assert "Error ---" not in result.output


def test_unknown_command_with_env_emits_json(runner, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "nosuchcommand"])
    result = runner.invoke(["nosuchcommand"])

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "UsageError"
    assert "nosuchcommand" in payload["error"]
    assert "traceback" not in payload
    assert "Error ---" not in result.output


def test_unknown_command_with_format_flag_emits_json(runner, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["snow", "nosuchcommand", "--format", "JSON"])
    result = runner.invoke(["nosuchcommand", "--format", "JSON"])

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["type"] == "UsageError"
    assert "nosuchcommand" in payload["error"]
    assert "Error ---" not in result.output


def test_unknown_subcommand_with_format_flag_emits_json(runner, monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["snow", "connection", "nosuch", "--format", "JSON"]
    )
    result = runner.invoke(["connection", "nosuch", "--format", "JSON"])

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["type"] == "UsageError"
    assert "nosuch" in payload["error"]
    assert "Error ---" not in result.output


def test_debug_structured_error_is_json_without_log_prefix(runner, monkeypatch):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "snow",
            "connection",
            "set-default",
            "nosuch",
            "--format",
            "JSON",
            "--debug",
        ],
    )
    result = runner.invoke(
        ["connection", "set-default", "nosuch", "--format", "JSON", "--debug"]
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["type"] == "MissingConfigurationError"
    assert "traceback" in payload
    assert "DEBUG" not in result.output
    assert "Writing to file" not in result.output


def test_successful_json_command_keeps_debug_logs(runner, monkeypatch):
    """``--debug`` on a successful JSON command still reaches the console.

    integration-nativeapp asserts the upgrade SQL from ``app run --debug``,
    which ``invoke_with_connection_json`` captures in the mixed output.
    Buffering must not swallow those lines when the command succeeds.
    """
    from snowflake.cli._app import loggers

    original = loggers.create_loggers

    def _create_loggers_and_emit(verbose: bool, debug: bool) -> None:
        original(verbose, debug)
        logging.getLogger("snowflake.cli").debug(
            "alter application demo upgrade using @stage"
        )

    monkeypatch.setattr(loggers, "create_loggers", _create_loggers_and_emit)
    result = runner.invoke(["connection", "list", "--format", "JSON", "--debug"])

    assert result.exit_code == 0, result.output
    assert "alter application demo upgrade using @stage" in result.output


def test_library_entry_omits_traceback_without_debug(runner, monkeypatch):
    # CliRunner uses typer.get_command(app).main(), which never runs the
    # production argv pre-sniff. The default enable_tracebacks=True must
    # not leak a traceback into the structured error.
    get_cli_context_manager().enable_tracebacks = True
    install_structured_error_rendering()
    monkeypatch.setenv("SNOWFLAKE_CLI_OUTPUT_FORMAT", "json")
    monkeypatch.setattr(sys, "argv", ["snow", "sql", "--foobar"])
    result = runner.invoke(["sql", "--foobar"])

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["type"] == "NoSuchOption"
    assert "traceback" not in payload
