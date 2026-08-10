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

from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.env import (
    collect_env_vars,
    parse_env_file,
    resolve_declared_env_vars,
)
from snowflake.cli._plugins.dcm.models import DCMTemplating
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS


def test_collect_env_vars_collects_present_names(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
    monkeypatch.setenv("WH_SIZE", "XLARGE")

    result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {"DB_HOST": "prod.analytics.internal", "WH_SIZE": "XLARGE"}


def test_collect_env_vars_omits_absent_names(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.setenv("WH_SIZE", "XLARGE")

    result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {"WH_SIZE": "XLARGE"}


def test_collect_env_vars_ignores_undeclared_names(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
    monkeypatch.setenv("SOME_UNRELATED_VAR", "should-not-leak")

    result = collect_env_vars({"DB_HOST"})

    assert result == {"DB_HOST": "prod.analytics.internal"}


def test_collect_env_vars_empty_declared_names_returns_empty_dict(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")

    result = collect_env_vars(set())

    assert result == {}


def test_collect_env_vars_none_present_returns_empty_dict(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("WH_SIZE", raising=False)

    result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {}


def test_collect_env_vars_warns_about_missing_names(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.setenv("WH_SIZE", "XLARGE")

    with mock.patch("snowflake.cli._plugins.dcm.env.cli_console") as mock_console:
        result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {"WH_SIZE": "XLARGE"}
    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "the shell environment" in warning_message
    assert "DB_HOST" in warning_message


def test_collect_env_vars_does_not_warn_when_all_present(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")

    with mock.patch("snowflake.cli._plugins.dcm.env.cli_console") as mock_console:
        collect_env_vars({"DB_HOST"})

    mock_console.warning.assert_not_called()


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="os.environ folds env-var name case on Windows; DB_HOST/db_host collapse to one slot",
)
def test_collect_env_vars_treats_different_case_names_as_distinct(monkeypatch):
    """DB_HOST and db_host are two different declared names (exact string
    equality, matching how GS matches them server-side) -- on this
    case-sensitive platform they resolve independently, to two different
    values, not collapsed into one."""
    monkeypatch.setenv("DB_HOST", "upper-value")
    monkeypatch.setenv("db_host", "lower-value")

    result = collect_env_vars({"DB_HOST", "db_host"})

    assert result == {"DB_HOST": "upper-value", "db_host": "lower-value"}


@pytest.mark.skipif(
    not IS_WINDOWS,
    reason="Windows-only: os.environ folds env-var name case there, so a "
    "manifest-declared name resolves against a differently-cased host "
    "process variable. The mirror-image assumption "
    "(different-case names are DISTINCT) is pinned for Linux/Mac by "
    "test_collect_env_vars_treats_different_case_names_as_distinct above; "
    "this is the same mechanism from the other side, on the platform "
    "where it actually applies.",
)
def test_collect_env_vars_resolves_lowercase_manifest_names_against_uppercase_host_variables(
    monkeypatch,
):
    """A manifest declaring 'db_host' (env_vars) and 'api_key'
    (env_secrets), both lowercase, must resolve against real Windows
    process variables actually set as DB_HOST/API_KEY (uppercase) -- the
    CLI doesn't distinguish env_vars from env_secrets once they're
    collected into one declared-name set (DCMTemplating.declared_variable_names),
    so this exercises the same collect_env_vars() resolution for both."""
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
    monkeypatch.setenv("API_KEY", "shhh-secret-value")
    templating = DCMTemplating.from_dict(
        {"env_vars": [{"db_host": None}], "env_secrets": [{"api_key": None}]}
    )

    result = collect_env_vars(templating.declared_variable_names)

    assert result == {
        "db_host": "prod.analytics.internal",
        "api_key": "shhh-secret-value",
    }


def test_parse_env_file_parses_key_value_pairs(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=prod.analytics.internal\nWH_SIZE=XLARGE\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"DB_HOST": "prod.analytics.internal", "WH_SIZE": "XLARGE"}


def test_parse_env_file_skips_comments_and_blank_lines(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# a comment\n"
        "\n"
        "DB_HOST=prod.analytics.internal\n"
        "   \n"
        "# another comment\n"
        "WH_SIZE=XLARGE\n"
    )

    result = parse_env_file(SecurePath(env_file))

    assert result == {"DB_HOST": "prod.analytics.internal", "WH_SIZE": "XLARGE"}


def test_parse_env_file_strips_matching_quotes(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        'AWS_SECRET_KEY="wJalrXUtnFEMI/K7MDENG"\n' "API_TOKEN='bearer-token-here'\n"
    )

    result = parse_env_file(SecurePath(env_file))

    assert result == {
        "AWS_SECRET_KEY": "wJalrXUtnFEMI/K7MDENG",
        "API_TOKEN": "bearer-token-here",
    }


def test_parse_env_file_strips_inline_comment_after_value(tmp_path):
    """python-dotenv strips a trailing `# comment` that's preceded by
    whitespace, even on a line that also declares a value."""
    env_file = tmp_path / ".env"
    env_file.write_text("KEY=value  # this is my env value\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"KEY": "value"}


def test_parse_env_file_unquoted_value_containing_hash_is_truncated(tmp_path):
    """An unquoted value with a whitespace-preceded '#' is silently cut at
    the '#' -- the same inline-comment stripping as the test above, just
    pinned for a value where truncation is unwanted (e.g. a secret that
    happens to contain '#'). Quoting preserves the full value."""
    env_file = tmp_path / ".env"
    env_file.write_text("AWS_SECRET_KEY=abc #123def\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"AWS_SECRET_KEY": "abc"}


def test_parse_env_file_quoted_value_containing_hash_is_preserved(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text('AWS_SECRET_KEY="abc #123def"\n')

    result = parse_env_file(SecurePath(env_file))

    assert result == {"AWS_SECRET_KEY": "abc #123def"}


def test_parse_env_file_supports_export_prefix(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("export DB_HOST=prod.analytics.internal\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"DB_HOST": "prod.analytics.internal"}


def test_parse_env_file_interprets_escaped_double_quotes(tmp_path):
    """Unlike a hand-rolled minimal parser, python-dotenv processes
    backslash escapes inside a double-quoted value -- \\" unescapes to a
    literal quote, matching the common .env-authoring convention."""
    env_file = tmp_path / ".env"
    env_file.write_text('KEY="some \\"quoted\\" value"\n')

    result = parse_env_file(SecurePath(env_file))

    assert result == {"KEY": 'some "quoted" value'}


def test_parse_env_file_interprets_escaped_single_quotes(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("KEY='it\\'s here'\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"KEY": "it's here"}


def test_parse_env_file_value_containing_equals_sign_is_preserved(tmp_path):
    """Only the FIRST '=' splits key from value -- a connection string or
    query string containing its own '=' must not be truncated."""
    env_file = tmp_path / ".env"
    env_file.write_text("DATABASE_URL=postgres://user:pass@host/db?sslmode=require\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"DATABASE_URL": "postgres://user:pass@host/db?sslmode=require"}


def test_parse_env_file_last_duplicate_key_wins(tmp_path):
    """Same last-line-wins convention as shell `export`."""
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=first\nDB_HOST=second\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"DB_HOST": "second"}


def test_parse_env_file_skips_line_without_equals(tmp_path):
    """A line with a key but no '=' parses to a None value (python-dotenv
    reports nothing for it) and is dropped by our own value-is-not-None
    filter, rather than raising -- the rest of the file still parses
    normally."""
    env_file = tmp_path / ".env"
    env_file.write_text("GOOD_ONE=ok\nTHIS_LINE_IS_BROKEN\nGOOD_TWO=also_ok\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"GOOD_ONE": "ok", "GOOD_TWO": "also_ok"}
    assert "THIS_LINE_IS_BROKEN" not in result


def test_parse_env_file_does_not_interpolate_variables(tmp_path):
    """No variable interpolation: a value containing $VAR/${VAR} is kept
    literal -- a secret containing a literal '$' must never be rewritten."""
    env_file = tmp_path / ".env"
    env_file.write_text("BASE=hello\nDERIVED=${BASE}_world\n")

    result = parse_env_file(SecurePath(env_file))

    assert result == {"BASE": "hello", "DERIVED": "${BASE}_world"}


def test_parse_env_file_raises_when_file_missing(tmp_path):
    missing_path = tmp_path / "does_not_exist.env"

    with pytest.raises(CliError, match="was not found"):
        parse_env_file(SecurePath(missing_path))


def test_parse_env_file_raises_when_path_is_a_directory(tmp_path):
    """A directory `.exists()` too -- must check `.is_file()` separately."""
    with pytest.raises(CliError, match="is not a file"):
        parse_env_file(SecurePath(tmp_path))


# No test for a file exceeding DEFAULT_SIZE_LIMIT_MB (128MB): that limit is
# enforced by SecurePath.open() itself (FileTooLargeError, already a
# CliError subclass), not custom logic in parse_env_file -- writing a real
# 128MB+ fixture file isn't a reasonable trade for exercising someone
# else's already-covered code path.


def test_resolve_declared_env_vars_shell_wins_over_file_on_conflict(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DB_HOST", "shell-value")
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=file-value\n")

    result = resolve_declared_env_vars({"DB_HOST"}, SecurePath(env_file))

    assert result == {"DB_HOST": "shell-value"}


def test_resolve_declared_env_vars_file_fills_in_names_absent_from_shell(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.setenv("WH_SIZE", "from-shell")
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=from-file\n")

    result = resolve_declared_env_vars({"DB_HOST", "WH_SIZE"}, SecurePath(env_file))

    assert result == {"DB_HOST": "from-file", "WH_SIZE": "from-shell"}


def test_resolve_declared_env_vars_falls_back_to_shell_when_no_file(monkeypatch):
    monkeypatch.setenv("DB_HOST", "from-shell")

    result = resolve_declared_env_vars({"DB_HOST"}, None)

    assert result == {"DB_HOST": "from-shell"}


def test_resolve_declared_env_vars_warns_about_names_missing_from_both(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("WH_SIZE", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=from-file\n")

    with mock.patch("snowflake.cli._plugins.dcm.env.cli_console") as mock_console:
        result = resolve_declared_env_vars({"DB_HOST", "WH_SIZE"}, SecurePath(env_file))

    assert result == {"DB_HOST": "from-file"}
    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "WH_SIZE" in warning_message
    assert "env file" in warning_message


def test_resolve_declared_env_vars_does_not_warn_when_file_alone_satisfies_all(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("DB_HOST", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("DB_HOST=from-file\n")

    with mock.patch("snowflake.cli._plugins.dcm.env.cli_console") as mock_console:
        result = resolve_declared_env_vars({"DB_HOST"}, SecurePath(env_file))

    assert result == {"DB_HOST": "from-file"}
    mock_console.warning.assert_not_called()


def test_resolve_declared_env_vars_missing_file_raises_even_with_no_declared_names(
    tmp_path,
):
    """A user who typed --env-file with a wrong path wants to know
    immediately, even if the manifest happens to declare zero env vars."""
    missing_path = tmp_path / "does_not_exist.env"

    with pytest.raises(CliError, match="was not found"):
        resolve_declared_env_vars(set(), SecurePath(missing_path))


def test_resolve_declared_env_vars_file_is_case_sensitive(tmp_path, monkeypatch):
    """Unlike os.environ on Windows, a hand-parsed .env file is a plain
    dict -- always case-sensitive, on every platform."""
    monkeypatch.delenv("db_host", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("db_host=prod.analytics.internal\n")

    result = resolve_declared_env_vars({"DB_HOST"}, SecurePath(env_file))

    assert result == {}
