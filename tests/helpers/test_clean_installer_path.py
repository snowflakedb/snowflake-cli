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


import json
import os
import stat
from unittest.mock import Mock

import pytest
from snowflake.cli.api.constants import IS_WINDOWS

COMMAND = "clean-installer-path"
COMMANDS_OS = "snowflake.cli._plugins.helpers.commands.os"
RC_FILENAMES = (".zprofile", ".zshrc", ".profile", ".bash_profile", ".bashrc")
HISTORICAL_COMMENT = "# added by Snowflake SnowflakeCLI installer v1.0"
HISTORICAL_PATH = "export PATH=/Applications/SnowflakeCLI.app/Contents/MacOS/:$PATH"
BEGIN_MARK = "# snowflake-cli PATH begin"
END_MARK = "# snowflake-cli PATH end"


def _patch_geteuid(monkeypatch, euid: int) -> None:
    # Windows has no os.geteuid. Production uses getattr(os, "geteuid", None);
    # raising=False lets these tests create the attribute when it is missing.
    monkeypatch.setattr(f"{COMMANDS_OS}.geteuid", lambda: euid, raising=False)


def _assert_skipped(output: str, path) -> None:
    # CliError renders the message in a wrapped panel, so the path may be
    # split across lines with box-drawing characters in between.
    compact = output.replace("|", "").replace("\n", "").replace(" ", "")
    assert "Skipped" in output
    assert str(path) in compact


@pytest.fixture
def macos_user(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.platform.system", lambda: "Darwin"
    )
    _patch_geteuid(monkeypatch, 501)
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.Path.home", lambda: tmp_path
    )
    return tmp_path


def test_non_macos_explains_that_the_command_is_macos_only(
    runner, monkeypatch, tmp_path
):
    rc_file = tmp_path / ".zprofile"
    original = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n"
    rc_file.write_text(original)
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.platform.system", lambda: "Linux"
    )
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.Path.home", lambda: tmp_path
    )

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "intended only for macOS" in result.output
    assert rc_file.read_text() == original


def test_refuses_effective_root(runner, monkeypatch):
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.platform.system", lambda: "Darwin"
    )
    _patch_geteuid(monkeypatch, 0)

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code != 0
    assert "must not be run as root" in result.output


def test_root_is_not_refused_on_non_macos_platforms(runner, monkeypatch, tmp_path):
    # The command is a no-op on non-Darwin platforms, so root should never be
    # refused there: the platform check must run before the root check.
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.platform.system", lambda: "Linux"
    )
    _patch_geteuid(monkeypatch, 0)
    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.commands.Path.home", lambda: tmp_path
    )

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert "intended only for macOS" in result.output


def test_dry_run_reports_pairs_without_writing(runner, macos_user):
    rc_file = macos_user / ".zprofile"
    original = f"keep\n{HISTORICAL_COMMENT}\n\n{HISTORICAL_PATH}\nafter\n"
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND])

    assert result.exit_code == 0, result.output
    assert (
        f"Would remove 1 historical installer PATH pair from {rc_file}" in result.output
    )
    assert rc_file.read_text() == original


def test_apply_removes_pairs_only_from_the_five_historical_files(runner, macos_user):
    pair = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n"
    original_contents = {}
    for filename in RC_FILENAMES:
        content = f"before\n{pair}after\n"
        (macos_user / filename).write_text(content)
        original_contents[filename] = content
    # A pre-existing, unrelated backup file (matching the macOS installer's own
    # backup naming convention, not ours) must be left untouched.
    installer_backup = macos_user / ".zprofile-snowflake.bak"
    installer_backup.write_text(pair)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    for filename in RC_FILENAMES:
        rc_file = macos_user / filename
        assert rc_file.read_text() == "before\nafter\n"
        assert (
            f"Removed 1 historical installer PATH pair from {rc_file}" in result.output
        )
        cleanup_backup = macos_user / f"{filename}-snowflake-cleanup.bak"
        assert cleanup_backup.read_text() == original_contents[filename]
    assert installer_backup.read_text() == pair


def test_apply_creates_backup_with_original_contents_before_overwriting(
    runner, macos_user
):
    rc_file = macos_user / ".zprofile"
    original = f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n"
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    backup = macos_user / ".zprofile-snowflake-cleanup.bak"
    assert backup.read_text() == original
    assert rc_file.read_text() == "keep\nafter\n"


def test_apply_overwrites_stale_backup_from_a_previous_run(runner, macos_user):
    rc_file = macos_user / ".zprofile"
    original = f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n"
    rc_file.write_text(original)
    backup = macos_user / ".zprofile-snowflake-cleanup.bak"
    backup.write_text("stale backup from an earlier, unrelated run\n")

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert backup.read_text() == original
    assert rc_file.read_text() == "keep\nafter\n"


def test_apply_does_not_create_backup_when_there_is_nothing_to_remove(
    runner, macos_user
):
    rc_file = macos_user / ".zshrc"
    original = "export PATH=/usr/local/bin:$PATH\n"
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    backup = macos_user / ".zshrc-snowflake-cleanup.bak"
    assert not backup.exists()
    assert rc_file.read_text() == original


def test_apply_preserves_marked_blocks(runner, macos_user):
    rc_file = macos_user / ".zprofile"
    paired_marked_block = (
        f"{BEGIN_MARK}\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n{END_MARK}\n"
    )
    unpaired_marked_block = (
        f"{BEGIN_MARK}\n{HISTORICAL_COMMENT}\nexport PATH=/usr/bin:$PATH\n{END_MARK}\n"
    )
    removable_pair = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n"
    preserved = paired_marked_block + unpaired_marked_block
    original = preserved + removable_pair
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert "Unpaired historical installer comment" in result.output
    assert "Removed 1 historical installer PATH pair" in result.output
    assert rc_file.read_text() == preserved


def test_apply_preserves_brew_shellenv_line_following_historical_comment(
    runner, macos_user
):
    """The brew-shellenv exclusion in ``_is_historical_path_line`` must be
    load-bearing here: the line immediately following the historical comment
    starts with ``export PATH=`` and its destination contains
    ``SnowflakeCLI.app/Contents/MacOS`` -- so without the ``"brew shellenv" in
    line`` check it would satisfy every other condition in
    ``_is_historical_path_line`` and get paired up and removed.
    """
    rc_file = macos_user / ".zprofile"
    brew_pair = (
        f"{HISTORICAL_COMMENT}\n"
        "export PATH=/usr/local/Homebrew/SnowflakeCLI.app/Contents/MacOS:$PATH "
        '# eval "$(brew shellenv)"\n'
    )
    removable_pair = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n"
    original = brew_pair + removable_pair
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert "Unpaired historical installer comment" in result.output
    assert "Removed 1 historical installer PATH pair" in result.output
    assert rc_file.read_text() == brew_pair


def test_apply_preserves_unrelated_path_line_following_historical_comment(
    runner, macos_user
):
    """The MACOS_APP_PATH check in ``_is_historical_path_line`` must be
    load-bearing here: the line immediately following the historical comment
    starts with ``export PATH=`` and ends in ``:$PATH``, but its destination
    does not reference ``SnowflakeCLI.app/Contents/MacOS`` -- so without the
    ``MACOS_APP_PATH in destination`` check it would be misidentified as a
    historical installer PATH line and removed.
    """
    rc_file = macos_user / ".zprofile"
    unrelated_pair = (
        f"{HISTORICAL_COMMENT}\n"
        "export PATH=/Applications/Other.app/Contents/MacOS:$PATH\n"
    )
    removable_pair = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n"
    original = unrelated_pair + removable_pair
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert "Unpaired historical installer comment" in result.output
    assert "Removed 1 historical installer PATH pair" in result.output
    assert rc_file.read_text() == unrelated_pair


def test_apply_removes_multiple_pairs_and_warns_for_unpaired_comment(
    runner, macos_user
):
    rc_file = macos_user / ".bash_profile"
    pair = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n"
    pair_with_separator = f"{HISTORICAL_COMMENT}\n\n{HISTORICAL_PATH}\n"
    original = pair + "keep\n" + pair_with_separator + HISTORICAL_COMMENT + "\n"
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert f"Removed 2 historical installer PATH pairs from {rc_file}" in result.output
    assert "Unpaired historical installer comment" in result.output
    assert rc_file.read_text() == f"keep\n{HISTORICAL_COMMENT}\n"


def test_apply_removes_blank_lines_adjacent_to_removed_pairs(runner, macos_user):
    rc_file = macos_user / ".bashrc"
    pair = f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n\n"
    original = (
        '# sfid\neval "$(sf aliases)"\n\n'
        + pair * 3
        + "export FOO=bar\n\n"
        + "# added by Snowflake SnowflakeCLI installer v2.0\n"
        + f"{HISTORICAL_PATH}\n"
    )
    rc_file.write_text(original)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert rc_file.read_text() == (
        '# sfid\neval "$(sf aliases)"\n\n'
        "export FOO=bar\n\n"
        "# added by Snowflake SnowflakeCLI installer v2.0\n"
        f"{HISTORICAL_PATH}\n"
    )


def test_json_format_returns_summary_as_message_result(runner, macos_user):
    rc_file = macos_user / ".zprofile"
    rc_file.write_text(f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n")
    missing = ", ".join(str(macos_user / name) for name in RC_FILENAMES[1:])

    result = runner.invoke(["helpers", COMMAND, "--format", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "message": (
            f"Would remove 1 historical installer PATH pair from {rc_file}.\n"
            f"Scanned {rc_file}.\n"
            f"Not present: {missing}."
        )
    }


def test_reports_what_was_analyzed_when_no_file_exists(runner, macos_user):
    missing = ", ".join(str(macos_user / name) for name in RC_FILENAMES)

    result = runner.invoke(["helpers", COMMAND, "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "message": (
            f"No historical installer PATH entries found.\nNot present: {missing}."
        )
    }


def test_reports_what_was_analyzed_when_every_file_is_clean(runner, macos_user):
    clean_file = macos_user / ".zshrc"
    clean_file.write_text("export PATH=/usr/local/bin:$PATH\n")
    missing = ", ".join(
        str(macos_user / name) for name in RC_FILENAMES if name != ".zshrc"
    )

    result = runner.invoke(["helpers", COMMAND, "--format", "json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "message": (
            "No historical installer PATH entries found.\n"
            f"Scanned {clean_file}.\n"
            f"Not present: {missing}."
        )
    }


def test_no_entries_found_message_is_qualified_when_a_file_is_skipped(
    runner, macos_user
):
    skipped = macos_user / ".zprofile"
    skipped.mkdir()

    result = runner.invoke(["helpers", COMMAND, "--format", "json"])

    assert result.exit_code == 0, result.output
    assert (
        "No historical installer PATH entries found in the files that could be scanned."
        in json.loads(result.output)["message"]
    )


def test_apply_exits_one_when_a_file_is_skipped(runner, macos_user):
    skipped = macos_user / ".zprofile"
    skipped.mkdir()
    editable = macos_user / ".zshrc"
    editable.write_text(f"{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\n")

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 1
    _assert_skipped(result.output, skipped)
    assert editable.read_text() == ""


@pytest.mark.skipif(IS_WINDOWS, reason="Unix-based permission system test")
def test_apply_preserves_original_file_permissions(runner, macos_user):
    rc_file = macos_user / ".zprofile"
    original = f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n"
    rc_file.write_text(original)
    os.chmod(rc_file, 0o600)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 0, result.output
    assert stat.S_IMODE(rc_file.stat().st_mode) == 0o600


def test_apply_skips_symlinked_rc_file_instead_of_clobbering_it(runner, macos_user):
    # A symlinked rc file is the normal setup for dotfile managers like
    # chezmoi, GNU Stow, dotbot, and yadm. os.replace() does not follow a
    # symlink -- it replaces the symlink's own directory entry -- so applying
    # the cleanup in place would silently turn the symlink into a plain
    # regular file, disconnecting it from the user's dotfiles repo.
    target = macos_user / "dotfiles_zprofile"
    original_target_contents = f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n"
    target.write_text(original_target_contents)

    rc_file = macos_user / ".zprofile"
    rc_file.symlink_to(target)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    _assert_skipped(result.output, rc_file)
    assert rc_file.is_symlink()
    assert rc_file.resolve() == target.resolve()
    assert target.read_text() == original_target_contents


@pytest.mark.skipif(IS_WINDOWS, reason="Unix-based permission system test")
def test_apply_skips_symlinked_rc_file_without_inheriting_permissive_target_mode(
    runner, macos_user
):
    # Regression for the permission-bit half of the bug: _write_cleaned_contents
    # _atomically() used to read the *target's* mode via Path.stat() (which
    # follows symlinks) and apply it to the replacement file. If the symlink's
    # target happened to be world-writable, the "cleaned" rc file would end up
    # world-writable too. Since the symlink must now be skipped entirely, no
    # file's permissions should be touched at all.
    target = macos_user / "dotfiles_zprofile"
    original_target_contents = f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n"
    target.write_text(original_target_contents)
    os.chmod(target, 0o777)

    rc_file = macos_user / ".zprofile"
    rc_file.symlink_to(target)

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    _assert_skipped(result.output, rc_file)
    assert rc_file.is_symlink()
    assert stat.S_IMODE(target.stat().st_mode) == 0o777
    assert target.read_text() == original_target_contents
    # No new regular file was created in place of the symlink.
    assert not rc_file.is_file() or rc_file.is_symlink()


def test_apply_leaves_no_stray_tmp_or_backup_file_when_write_fails(
    runner, macos_user, monkeypatch
):
    rc_file = macos_user / ".zprofile"
    original = f"keep\n{HISTORICAL_COMMENT}\n{HISTORICAL_PATH}\nafter\n"
    rc_file.write_text(original)

    monkeypatch.setattr(
        "snowflake.cli._plugins.helpers.installer_path.os.replace",
        Mock(side_effect=OSError("disk full")),
    )

    result = runner.invoke(["helpers", COMMAND, "--apply"])

    assert result.exit_code == 1
    _assert_skipped(result.output, rc_file)
    # The original file was never modified.
    assert rc_file.read_text() == original
    # No stray temp file survives a failed write (R1).
    assert list(macos_user.glob("*.snowflake-cleanup.tmp")) == []
    # No stray backup survives a write failure either, since the original
    # file was never actually changed (R2).
    assert list(macos_user.glob("*-snowflake-cleanup.bak")) == []
