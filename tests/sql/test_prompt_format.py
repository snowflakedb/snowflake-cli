from types import SimpleNamespace
from unittest import mock

import pytest
from snowflake.cli._plugins.sql.prompt_format import (
    DEFAULT_REPL_PROMPT,
    format_repl_prompt,
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
def test_snowsql_hex_colour_tokens_are_dropped_and_reported(colour_token):
    """Hex colour directives are unknown in this version. Dropped, never
    passed through as text, so a later colour release can honour them
    without changing the visible text of a format that works today."""
    assert format_repl_prompt(f"{colour_token}[user]>", {"user": "alice"}) == "alice>"
    assert unknown_tokens_in_prompt_format(f"{colour_token}[user]>") == (colour_token,)


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
    assert tokens == ("[#ff00ff]", "[future-token]")


def test_known_tokens_and_escapes_are_not_reported():
    assert unknown_tokens_in_prompt_format("[USER]@[database]\\[x\\]\\n\\\\") == ()


def test_unknown_token_warning_names_tokens_and_support():
    message = unknown_token_warning(("[#ff00ff]", "[future-token]"))
    assert "'[#ff00ff]'" in message
    assert "'[future-token]'" in message
    assert "[user]" in message
    assert "[#rrggbb]" in message
    assert "[bg:#rrggbb]" in message


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
