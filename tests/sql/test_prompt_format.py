from types import SimpleNamespace
from unittest import mock

import pytest
from prompt_toolkit.formatted_text import FormattedText
from snowflake.cli._plugins.sql.prompt_format import (
    DEFAULT_REPL_PROMPT,
    format_repl_prompt,
    format_repl_prompt_for_terminal,
    require_string_prompt_format,
    session_prompt_values,
    unknown_token_warning,
    unknown_tokens_in_prompt_format,
)
from snowflake.cli.api.exceptions import CliError


def test_default_prompt_when_template_missing():
    assert format_repl_prompt(None) == DEFAULT_REPL_PROMPT
    assert format_repl_prompt("") == DEFAULT_REPL_PROMPT
    assert DEFAULT_REPL_PROMPT == " > "


def test_fully_sanitised_template_falls_back_to_default():
    assert format_repl_prompt("\r\x00\x07") == DEFAULT_REPL_PROMPT


def test_expands_placeholders():
    values = {
        "user": "alice",
        "host": "xy12345.snowflakecomputing.com",
        "account": "xy12345",
        "role": "SYSADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "connection": "prod",
    }
    result = format_repl_prompt(
        "[connection] [user]#[warehouse]@[database].[schema]>",
        values,
    )
    assert result == "prod alice#COMPUTE_WH@ANALYTICS.PUBLIC>"


def test_placeholders_are_case_insensitive():
    result = format_repl_prompt(
        "[CONNECTION] [USER]#[Warehouse]@[Database].[Schema]>",
        {
            "connection": "prod",
            "user": "alice",
            "warehouse": "COMPUTE_WH",
            "database": "ANALYTICS",
            "schema": "PUBLIC",
        },
    )
    assert result == "prod alice#COMPUTE_WH@ANALYTICS.PUBLIC>"


def test_rejects_non_string_config_value():
    with pytest.raises(CliError, match="Expected a string for cli.prompt_format"):
        require_string_prompt_format(True)


def test_missing_values_use_placeholder_text():
    result = format_repl_prompt(
        "[user]#[warehouse]@[database].[schema]>",
        {},
    )
    assert result == "(no user)#(no warehouse)@(no database).(no schema)>"


def test_literal_bracket_and_newline():
    result = format_repl_prompt("\\[[user]\\n>", {"user": "alice"})
    assert result == "[alice\n>"


def test_escaped_closing_bracket_and_backslash():
    assert format_repl_prompt("\\[[user]\\]>", {"user": "alice"}) == "[alice]>"
    assert format_repl_prompt("foo\\\\bar>", {}) == "foo\\bar>"
    assert format_repl_prompt("\\\\[user]>", {"user": "alice"}) == "\\alice>"
    assert require_string_prompt_format("\\[[user]\\]>") == "\\[[user]\\]>"
    assert require_string_prompt_format("foo\\\\bar>") == "foo\\\\bar>"


def test_uppercase_backslash_n_stays_literal():
    """Only lowercase ``\\n`` is the newline escape. ``\\N`` is ordinary text,
    so it is neither swallowed nor reported as an unknown placeholder."""
    assert format_repl_prompt("foo\\New>", {}) == "foo\\New>"
    assert unknown_tokens_in_prompt_format("foo\\New>") == ()
    assert format_repl_prompt("[USER]\\n>", {"user": "alice"}) == "alice\n>"


@pytest.mark.parametrize("colour_token", ["[#ff00ff]", "[bg:#00ff00]"])
def test_snowsql_hex_colour_tokens_do_not_change_visible_text(colour_token):
    assert format_repl_prompt(f"{colour_token}[user]>", {"user": "alice"}) == "alice>"
    assert unknown_tokens_in_prompt_format(f"{colour_token}[user]>") == ()


def test_formats_foreground_and_background_colours_for_terminal():
    result = format_repl_prompt_for_terminal(
        "[#BCA81F][user]@[bg:#001122][database][#FFff00]>",
        {"user": "alice", "database": "DB"},
    )

    assert result == FormattedText(
        [
            ("fg:#bca81f", "alice@"),
            ("fg:#bca81f bg:#001122", "DB"),
            ("fg:#ffff00 bg:#001122", ">"),
        ]
    )


def test_formats_jira_multiline_prompt_and_ignores_trailing_colour():
    template = (
        "┌──[#bca81f][user]@[account].[role].[warehouse].[database].[schema]\n"
        "└─$[#ffff00]"
    )
    values = {
        "user": "alice",
        "account": "acct",
        "role": "SYSADMIN",
        "warehouse": "WH",
        "database": "DB",
        "schema": "PUBLIC",
    }

    assert format_repl_prompt_for_terminal(template, values) == FormattedText(
        [
            ("", "┌──"),
            ("fg:#bca81f", "alice@acct.SYSADMIN.WH.DB.PUBLIC\n└─$"),
        ]
    )


def test_escaped_colour_is_literal_unstyled_text():
    result = format_repl_prompt_for_terminal(
        "\\[#ff00ff][#00ff00][user]>",
        {"user": "alice"},
    )

    assert result == FormattedText(
        [
            ("", "[#ff00ff]"),
            ("fg:#00ff00", "alice>"),
        ]
    )


@pytest.mark.parametrize(
    "token",
    ["[#fff]", "[#gggggg]", "[bg:#12345g]", "[red]"],
)
def test_malformed_and_named_colours_remain_unknown(token):
    assert unknown_tokens_in_prompt_format(f"{token}[user]>") == (token,)


def test_colour_only_template_falls_back_to_default():
    assert format_repl_prompt_for_terminal("[#ff00ff]", {}) == DEFAULT_REPL_PROMPT


def test_sanitizes_values_without_interpreting_them_as_styles():
    result = format_repl_prompt_for_terminal(
        "[#ff00ff][user]>",
        {"user": "\033[31m[bg:#000000]alice"},
    )

    assert result == FormattedText([("fg:#ff00ff", "[bg:#000000]alice>")])


def test_unknown_placeholder_is_dropped_and_reported():
    assert format_repl_prompt("[future-token][user]>", {"user": "alice"}) == "alice>"
    assert unknown_tokens_in_prompt_format("[future-token][user]>") == (
        "[future-token]",
    )


def test_named_style_tokens_are_unknown_not_snowsql_colour():
    """Colour directives in this version are hex only (``[#rrggbb]`` /
    ``[bg:#rrggbb]``). Named tokens such as ``[red]`` are ordinary
    unknown placeholders, not colour syntax."""
    assert format_repl_prompt("[red][user]>", {"user": "alice"}) == "alice>"
    assert unknown_tokens_in_prompt_format("[red][user]>") == ("[red]",)


def test_unknown_tokens_are_listed_once_in_order():
    tokens = unknown_tokens_in_prompt_format("[#ff00ff][user][future-token][#ff00ff]>")
    assert tokens == ("[future-token]",)


def test_known_tokens_and_escapes_are_not_reported():
    assert unknown_tokens_in_prompt_format("[USER]@[database]\\[x\\]\\n\\\\") == ()


def test_unknown_token_warning_names_tokens_and_support():
    message = unknown_token_warning(("[future-token]",))
    assert "'[future-token]'" in message
    assert "[user]" in message
    assert "not supported" not in message


def test_unknown_token_warning_sanitizes_tokens():
    # ESC M is a non-CSI escape, so the token still matches the [...] shape
    # and the reported name is the one with the escape stripped.
    assert unknown_tokens_in_prompt_format("[ev\033Mil]>") == ("[evil]",)
    assert unknown_tokens_in_prompt_format("[ev\ril]>") == ("[evil]",)


def test_escaped_unknown_token_is_literal():
    result = format_repl_prompt("\\[#ff00ff][user]>", {"user": "alice"})
    assert result == "[#ff00ff]alice>"


def test_value_containing_placeholder_is_not_reexpanded():
    result = format_repl_prompt(
        "[user]",
        {"user": "[database]", "database": "DB"},
    )
    assert result == "[database]"


def test_sanitizes_ansi_in_session_values():
    result = format_repl_prompt("[user]>", {"user": "alice\033[31m"})
    assert "\033" not in result
    assert result == "alice>"


def test_strips_carriage_return_and_nul_from_values():
    assert format_repl_prompt("[user]>", {"user": "alice\rEVIL"}) == "aliceEVIL>"
    assert format_repl_prompt("[user]", {"user": "a\x00b"}) == "ab"
    assert "\r" not in format_repl_prompt("foo\rbar>", {})


def test_coerces_non_string_values():
    assert format_repl_prompt("[account]>", {"account": 12345}) == "12345>"


def test_session_prompt_values_from_connection_object():
    connection = SimpleNamespace(
        user="alice",
        host="host.example",
        account="acct",
        role="SYSADMIN",
        warehouse="WH",
        database="DB",
        schema="PUBLIC",
    )
    values = session_prompt_values(connection, connection_name="prod")
    assert values["user"] == "alice"
    assert values["database"] == "DB"
    assert values["connection"] == "prod"


def test_session_prompt_values_without_connection_uses_context_only():
    context = SimpleNamespace(
        user="from-ctx",
        host=None,
        account=None,
        role=None,
        warehouse=None,
        database="CTX_DB",
        schema=None,
        connection_name="dev",
    )
    mock_ctx = mock.Mock()
    mock_ctx.connection_context = context
    type(mock_ctx).connection = mock.PropertyMock(
        side_effect=AssertionError("prompt must not look up the connection cache")
    )

    with mock.patch(
        "snowflake.cli._plugins.sql.prompt_format.get_cli_context",
        return_value=mock_ctx,
    ):
        values = session_prompt_values()

    assert values["user"] == "from-ctx"
    assert values["database"] == "CTX_DB"
    assert values["connection"] == "dev"
