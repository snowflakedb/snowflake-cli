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

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="The postinstall script needs a POSIX shell and POSIX file modes.",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
POSTINSTALL = REPO_ROOT / "scripts" / "packaging" / "macos" / "postinstall"
WELCOME_HTML = (
    REPO_ROOT
    / "scripts"
    / "packaging"
    / "macos"
    / "Resources"
    / "snowflake-cli_welcome.html"
)
CONCLUSION_HTML = (
    REPO_ROOT
    / "scripts"
    / "packaging"
    / "macos"
    / "Resources"
    / "snowflake-cli_get_started.html"
)

BEGIN_MARK = "# snowflake-cli PATH begin"
END_MARK = "# snowflake-cli PATH end"
HISTORICAL_COMMENT = "# added by Snowflake SnowflakeCLI installer v1.0"

MACOS_SUFFIX = "SnowflakeCLI.app/Contents/MacOS"


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _fake_dscl(tmp_path: Path, user_shell: str | None, *, fail: bool = False) -> Path:
    script = tmp_path / "fake_dscl"
    if fail:
        _write_executable(script, "#!/bin/bash\nexit 1\n")
        return script
    _write_executable(script, f"#!/bin/bash\necho 'UserShell: {user_shell}'\n")
    return script


def _run_postinstall(
    tmp_path: Path,
    dest: str,
    *,
    dscl: Path | None = None,
    extra_env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    paths_d = tmp_path / "etc" / "paths.d" / "snowflake-cli"
    fake_chown = tmp_path / "fake_chown"
    _write_executable(fake_chown, "#!/bin/bash\nexit 0\n")
    env = os.environ.copy()
    env["HOME"] = str(tmp_path / "installer-home")
    env["SNOWFLAKE_CLI_PATHS_D"] = str(paths_d)
    env["SNOWFLAKE_CLI_USERS_ROOT"] = str(tmp_path / "Users")
    env["SNOWFLAKE_CLI_CHOWN"] = str(fake_chown)
    if dscl is not None:
        env["SNOWFLAKE_CLI_DSCL"] = str(dscl)
    if extra_env:
        env.update(extra_env)
    (tmp_path / "installer-home").mkdir(exist_ok=True)
    (tmp_path / "Users").mkdir(exist_ok=True)
    return subprocess.run(
        ["bash", str(POSTINSTALL), "/tmp/SnowflakeCLI.pkg", dest],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
    )


def _macos_dir(dest: str) -> str:
    return f"{dest.rstrip('/')}/{MACOS_SUFFIX}"


def _path_export_line(dest: str) -> str:
    return f"export PATH='{_macos_dir(dest)}':$PATH"


def test_system_domain_writes_paths_d_and_does_not_edit_rc(tmp_path: Path):
    dest = "/Applications"
    installer_home = tmp_path / "installer-home"
    zprofile = installer_home / ".zprofile"
    zprofile.parent.mkdir(parents=True)
    zprofile.write_text("existing\n")

    result = _run_postinstall(tmp_path, dest)

    assert result.returncode == 0, result.stderr + result.stdout
    paths_d = tmp_path / "etc" / "paths.d" / "snowflake-cli"
    assert paths_d.read_text() == f"{_macos_dir(dest)}\n"
    assert zprofile.read_text() == "existing\n"
    assert not list(installer_home.glob("*-snowflake.bak"))


def test_system_domain_overwrites_paths_d_on_upgrade(tmp_path: Path):
    dest = "/Applications"
    paths_d = tmp_path / "etc" / "paths.d" / "snowflake-cli"
    paths_d.parent.mkdir(parents=True)
    paths_d.write_text("/old/SnowflakeCLI.app/Contents/MacOS\n")

    result = _run_postinstall(tmp_path, dest)

    assert result.returncode == 0, result.stderr + result.stdout
    assert paths_d.read_text() == f"{_macos_dir(dest)}\n"


def test_system_domain_still_writes_when_dest_already_on_process_path(tmp_path: Path):
    dest = "/Applications"
    macos = _macos_dir(dest)
    result = _run_postinstall(
        tmp_path,
        dest,
        extra_env={"PATH": f"{macos}:/usr/bin"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    paths_d = tmp_path / "etc" / "paths.d" / "snowflake-cli"
    assert paths_d.read_text() == f"{macos}\n"


def test_system_domain_write_failure_logs_and_exits_zero(tmp_path: Path):
    dest = "/Applications"
    blocker = tmp_path / "etc" / "paths.d" / "snowflake-cli"
    blocker.parent.mkdir(parents=True)
    blocker.mkdir()

    result = _run_postinstall(tmp_path, dest)

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Failed" in result.stdout or "failed" in result.stdout.lower()


def test_user_domain_zsh_writes_marked_block_in_zprofile(tmp_path: Path):
    dest = str(tmp_path / "Users" / "alice" / "Applications")
    zprofile = tmp_path / "Users" / "alice" / ".zprofile"
    zprofile.parent.mkdir(parents=True)
    zprofile.write_text("# keep me\n")
    historical_zshrc = tmp_path / "Users" / "alice" / ".zshrc"
    historical_zshrc.write_text(
        f"{HISTORICAL_COMMENT}\nexport PATH={dest}/{MACOS_SUFFIX}:$PATH\n"
    )

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (tmp_path / "etc" / "paths.d" / "snowflake-cli").exists()
    text = zprofile.read_text()
    assert text.startswith("# keep me\n")
    assert BEGIN_MARK in text
    assert END_MARK in text
    assert _path_export_line(dest) in text
    assert HISTORICAL_COMMENT not in text
    assert historical_zshrc.read_text().startswith(HISTORICAL_COMMENT)
    assert not list(zprofile.parent.glob("*-snowflake.bak"))


def test_user_domain_resolves_account_from_destination_not_installer_home(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "alice" / "Applications")
    installer_zprofile = tmp_path / "installer-home" / ".zprofile"
    installer_zprofile.parent.mkdir(parents=True)
    installer_zprofile.write_text("root-home\n")
    (tmp_path / "Users" / "alice").mkdir(parents=True)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert installer_zprofile.read_text() == "root-home\n"
    alice_zprofile = tmp_path / "Users" / "alice" / ".zprofile"
    assert BEGIN_MARK in alice_zprofile.read_text()


def test_user_domain_zsh_creates_zprofile(tmp_path: Path):
    dest = str(tmp_path / "Users" / "bob" / "Applications")
    home = tmp_path / "Users" / "bob"
    home.mkdir(parents=True)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    zprofile = home / ".zprofile"
    assert zprofile.exists()
    assert oct(zprofile.stat().st_mode & 0o777) == "0o644"
    text = zprofile.read_text()
    assert BEGIN_MARK in text
    assert _path_export_line(dest) in text


@pytest.mark.parametrize(
    ("existing_profiles", "selected_profile"),
    [
        ((".profile",), ".profile"),
        ((".bash_login", ".profile"), ".bash_login"),
        ((".bash_profile", ".bash_login", ".profile"), ".bash_profile"),
    ],
)
def test_user_domain_bash_writes_first_existing_login_profile(
    tmp_path: Path,
    existing_profiles: tuple[str, ...],
    selected_profile: str,
):
    dest = str(tmp_path / "Users" / "barbara" / "Applications")
    home = tmp_path / "Users" / "barbara"
    home.mkdir(parents=True)
    for profile in existing_profiles:
        (home / profile).write_text(f"existing {profile}\n")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/bash"))

    assert result.returncode == 0, result.stderr + result.stdout
    for profile in existing_profiles:
        text = (home / profile).read_text()
        if profile == selected_profile:
            assert BEGIN_MARK in text
            assert _path_export_line(dest) in text
        else:
            assert text == f"existing {profile}\n"


def test_user_domain_bash_without_login_profile_skips_rc(tmp_path: Path):
    dest = str(tmp_path / "Users" / "ben" / "Applications")
    home = tmp_path / "Users" / "ben"
    home.mkdir(parents=True)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/bash"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (home / ".bash_profile").exists()
    assert not (home / ".bash_login").exists()
    assert not (home / ".profile").exists()
    assert "no bash login profile found" in result.stdout.lower()


def test_user_domain_bash_skips_dangling_symlink_when_selecting_profile(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "bill" / "Applications")
    home = tmp_path / "Users" / "bill"
    home.mkdir(parents=True)
    bash_profile = home / ".bash_profile"
    bash_profile.symlink_to(home / "missing")
    profile = home / ".profile"
    profile.write_text("# keep me\n")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/bash"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert bash_profile.is_symlink()
    assert BEGIN_MARK in profile.read_text()
    assert _path_export_line(dest) in profile.read_text()


def test_user_domain_bash_skips_live_symlink_when_selecting_profile(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "beth" / "Applications")
    home = tmp_path / "Users" / "beth"
    home.mkdir(parents=True)
    bash_profile = home / ".bash_profile"
    bash_profile.symlink_to(".profile")
    profile = home / ".profile"
    profile.write_text("# keep me\n")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/bash"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert bash_profile.is_symlink()
    assert bash_profile.readlink() == Path(".profile")
    assert BEGIN_MARK in profile.read_text()
    assert _path_export_line(dest) in profile.read_text()


def test_user_domain_bash_does_not_fall_through_unrelated_live_symlink(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "bianca" / "Applications")
    home = tmp_path / "Users" / "bianca"
    home.mkdir(parents=True)
    sourced_profile = home / "custom-profile"
    sourced_profile.write_text("# bash reads me\n")
    bash_profile = home / ".bash_profile"
    bash_profile.symlink_to(sourced_profile.name)
    profile = home / ".profile"
    profile.write_text("# bash does not read me\n")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/bash"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert "symlink outside the bash login profile chain" in result.stdout
    assert sourced_profile.read_text() == "# bash reads me\n"
    assert profile.read_text() == "# bash does not read me\n"


def test_user_domain_bash_updates_symlink_target_not_intermediate_profile(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "brenda" / "Applications")
    home = tmp_path / "Users" / "brenda"
    home.mkdir(parents=True)
    bash_profile = home / ".bash_profile"
    bash_profile.symlink_to(".profile")
    bash_login = home / ".bash_login"
    bash_login.write_text("# bash does not read me\n")
    profile = home / ".profile"
    profile.write_text("# bash reads me through .bash_profile\n")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/bash"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert bash_login.read_text() == "# bash does not read me\n"
    assert BEGIN_MARK in profile.read_text()
    assert _path_export_line(dest) in profile.read_text()


def test_user_domain_bash_does_not_recreate_profile_removed_before_update(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "brian" / "Applications")
    home = tmp_path / "Users" / "brian"
    home.mkdir(parents=True)
    bash_profile = home / ".bash_profile"
    bash_profile.write_text("# existing\n")
    dscl = _fake_dscl(tmp_path, "/bin/bash")

    real_mv = shutil.which("mv")
    assert real_mv, "mv must be on PATH to run this test"
    shim_dir = tmp_path / "mv-shim-bin"
    shim_dir.mkdir()
    _write_executable(
        shim_dir / "mv",
        f"""#!/bin/bash
if [ "$1" = "--" ] && [ "$2" = "{bash_profile}" ]; then
  rm -f "{bash_profile}"
  exit 1
fi
exec "{real_mv}" "$@"
""",
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert not bash_profile.exists()
    assert "disappeared" in result.stdout.lower()


def test_user_domain_login_zsh_strips_leading_dash(tmp_path: Path):
    dest = str(tmp_path / "Users" / "cara" / "Applications")
    (tmp_path / "Users" / "cara").mkdir(parents=True)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "-zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert BEGIN_MARK in (tmp_path / "Users" / "cara" / ".zprofile").read_text()


def test_user_domain_other_shell_skips_rc(tmp_path: Path):
    dest = str(tmp_path / "Users" / "dana" / "Applications")
    home = tmp_path / "Users" / "dana"
    home.mkdir(parents=True)

    result = _run_postinstall(
        tmp_path, dest, dscl=_fake_dscl(tmp_path, "/usr/local/bin/fish")
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (home / ".zprofile").exists()
    assert not (home / ".bash_profile").exists()
    assert not (home / ".bash_login").exists()
    assert not (home / ".profile").exists()
    assert "skip" in result.stdout.lower() or "fish" in result.stdout.lower()


def test_user_domain_dscl_failure_skips_rc(tmp_path: Path):
    dest = str(tmp_path / "Users" / "erin" / "Applications")
    home = tmp_path / "Users" / "erin"
    home.mkdir(parents=True)

    result = _run_postinstall(
        tmp_path, dest, dscl=_fake_dscl(tmp_path, None, fail=True)
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (home / ".zprofile").exists()
    assert not (home / ".bash_profile").exists()


def test_user_domain_upsert_replaces_marked_block_not_historical_pair(tmp_path: Path):
    dest = str(tmp_path / "Users" / "alice" / "Applications")
    zprofile = tmp_path / "Users" / "alice" / ".zprofile"
    zprofile.parent.mkdir(parents=True)
    old_path = str(tmp_path / "Users" / "alice" / "OldApp" / MACOS_SUFFIX)
    zprofile.write_text(
        f"{HISTORICAL_COMMENT}\n"
        f"export PATH={old_path}:$PATH\n"
        f"{BEGIN_MARK}\n"
        f"export PATH={old_path}:$PATH\n"
        f"{END_MARK}\n"
    )

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    text = zprofile.read_text()
    assert text.count(BEGIN_MARK) == 1
    assert _path_export_line(dest) in text
    assert f"{HISTORICAL_COMMENT}\nexport PATH={old_path}:$PATH\n" in text


def test_user_domain_malformed_markers_leave_file_unchanged(tmp_path: Path):
    dest = str(tmp_path / "Users" / "alice" / "Applications")
    zprofile = tmp_path / "Users" / "alice" / ".zprofile"
    zprofile.parent.mkdir(parents=True)
    original = f"{BEGIN_MARK}\nexport PATH=/broken\n"
    zprofile.write_text(original)
    mode_before = zprofile.stat().st_mode

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert zprofile.read_text() == original
    assert zprofile.stat().st_mode == mode_before
    assert "malformed" in result.stdout.lower()


def test_user_domain_preserves_existing_file_mode(tmp_path: Path):
    dest = str(tmp_path / "Users" / "alice" / "Applications")
    zprofile = tmp_path / "Users" / "alice" / ".zprofile"
    zprofile.parent.mkdir(parents=True)
    zprofile.write_text("keep\n")
    zprofile.chmod(0o600)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert oct(zprofile.stat().st_mode & 0o777) == "0o600"
    assert "keep\n" in zprofile.read_text()


def test_user_domain_preserves_existing_file_mode_on_replace_path(tmp_path: Path):
    dest = str(tmp_path / "Users" / "rex" / "Applications")
    home = tmp_path / "Users" / "rex"
    home.mkdir(parents=True)
    zprofile = home / ".zprofile"
    old_path = str(tmp_path / "Users" / "rex" / "OldApp" / MACOS_SUFFIX)
    zprofile.write_text(f"{BEGIN_MARK}\nexport PATH={old_path}:$PATH\n{END_MARK}\n")
    zprofile.chmod(0o600)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert oct(zprofile.stat().st_mode & 0o777) == "0o600"
    text = zprofile.read_text()
    assert text.count(BEGIN_MARK) == 1
    assert _path_export_line(dest) in text


def test_user_domain_missing_home_skips_rc(tmp_path: Path):
    dest = str(tmp_path / "Users" / "greg" / "Applications")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert not (tmp_path / "Users" / "greg").exists()
    assert "missing" in result.stdout.lower()


def test_user_domain_symlinked_rc_is_refused_not_followed(tmp_path: Path):
    dest = str(tmp_path / "Users" / "frank" / "Applications")
    home = tmp_path / "Users" / "frank"
    home.mkdir(parents=True)
    target = tmp_path / "outside-target"
    target.write_text("untouched\n")
    zprofile = home / ".zprofile"
    zprofile.symlink_to(target)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert zprofile.is_symlink()
    assert zprofile.resolve() == target
    assert target.read_text() == "untouched\n"
    assert "symlink" in result.stdout.lower()


def test_user_domain_rc_content_is_read_from_detached_copy_not_home_path(
    tmp_path: Path,
):
    """Regression test for an arbitrary-root-readable-file-disclosure TOCTOU
    found in review: refuse_symlink("$rc") is a single check-then-act test.
    $home is writable by the target user, so that user could swap $rc for a
    symlink to a file only root can read in the window between that check
    and the later block_state/cat/awk reads -- which, if done by pathname,
    would have root copy the symlink target's content verbatim into the
    user's own, readable rc file. The fix moves $rc into a root-only staging
    directory via mv (which never follows a symlink at either end) before
    reading anything, so every content read should happen against that
    detached copy, never against a path inside $home."""
    dest = str(tmp_path / "Users" / "ruth" / "Applications")
    home = tmp_path / "Users" / "ruth"
    home.mkdir(parents=True)
    zprofile = home / ".zprofile"
    zprofile.write_text("# keep me\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_cat = shutil.which("cat")
    assert real_cat, "cat must be on PATH to run this test"

    shim_dir = tmp_path / "cat-shim-bin"
    shim_dir.mkdir()
    cat_log = tmp_path / "cat-calls.log"
    _write_executable(
        shim_dir / "cat",
        f'#!/bin/bash\necho "$@" >> "{cat_log}"\nexec "{real_cat}" "$@"\n',
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert BEGIN_MARK in zprofile.read_text()
    calls = cat_log.read_text().splitlines()
    assert calls, "expected build_appended_block to cat the existing rc content"
    for call in calls:
        assert str(home) not in call, (
            "cat was invoked on a path inside $home while reading the "
            "existing rc file, reopening the read-side TOCTOU this test "
            f"guards against: {call!r}"
        )


def test_user_domain_fresh_install_detaches_rc_path_before_any_read(
    tmp_path: Path,
):
    """Regression test for a read-side TOCTOU (round 5): the exists=0 branch
    used to skip the detach-then-inspect dance entirely and pass the live,
    attacker-writable $rc path straight into build_appended_block, whose own
    [ -s ]/cat are symlink-following -- so a fresh install (no pre-existing
    rc file) reopened exactly the disclosure the exists=1 fix above closes.
    The fix makes the mv-into-stage_dir attempt itself, not a separate racy
    stat, decide whether $rc existed, so it must run even when it is
    expected to fail with ENOENT. Verify structurally by shimming mv: it
    must be invoked with the literal $rc path as its source at least once,
    proving the detach happens unconditionally rather than only when $rc
    appeared to exist a moment earlier."""
    dest = str(tmp_path / "Users" / "sam" / "Applications")
    home = tmp_path / "Users" / "sam"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    assert not rc.exists()
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_mv = shutil.which("mv")
    assert real_mv, "mv must be on PATH to run this test"

    shim_dir = tmp_path / "mv-shim-bin"
    shim_dir.mkdir()
    mv_log = tmp_path / "mv-calls.log"
    _write_executable(
        shim_dir / "mv",
        f'#!/bin/bash\necho "$@" >> "{mv_log}"\nexec "{real_mv}" "$@"\n',
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert BEGIN_MARK in rc.read_text()
    detach_calls = [
        line
        for line in mv_log.read_text().splitlines()
        if line.split()[:2] == ["--", str(rc)]
    ]
    assert detach_calls, (
        "expected the script to attempt `mv -- "
        f"{rc} ...` even on a fresh install (no pre-existing rc file), "
        "closing the read-side TOCTOU this test guards against"
    )


def test_user_domain_restore_failure_preserves_original_instead_of_deleting_it(
    tmp_path: Path,
):
    """Regression test for round-5 W2/W3: none of the restore-via-mv call
    sites in configure_user_domain checked mv's exit status, and even the
    one that did was followed unconditionally by `rm -rf "$stage_dir"` --
    deleting the user's original file (parked at $src inside stage_dir)
    even when the restore itself failed. Simulates atomic_replace failing
    because something replaced $rc with a directory between the detach and
    the replace (mirroring atomic_replace's own [ -d "$dest" ] guard) via a
    chown shim that plants the directory as a side effect, then verifies
    the original content survives on disk at the location the error
    message points to, instead of being silently destroyed along with
    stage_dir."""
    dest = str(tmp_path / "Users" / "tina" / "Applications")
    home = tmp_path / "Users" / "tina"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    rc.write_text("original content\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_chown = shutil.which("chown")
    real_mktemp = shutil.which("mktemp")
    assert real_chown, "chown must be on PATH to run this test"
    assert real_mktemp, "mktemp must be on PATH to run this test"

    shim_dir = tmp_path / "shim-bin"
    shim_dir.mkdir()
    mktemp_log = tmp_path / "mktemp-calls.log"
    _write_executable(
        shim_dir / "mktemp",
        f"""#!/bin/bash
out=$("{real_mktemp}" "$@")
status=$?
if [ "$status" -eq 0 ]; then
  echo "$out" >> "{mktemp_log}"
fi
echo "$out"
exit "$status"
""",
    )
    _write_executable(
        shim_dir / "chown",
        f'#!/bin/bash\n"{real_chown}" "$@"\nmkdir -p "{rc}" 2>/dev/null\nexit 0\n',
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={
            "PATH": f"{shim_dir}:{os.environ['PATH']}",
            "SNOWFLAKE_CLI_CHOWN": str(shim_dir / "chown"),
        },
    )

    assert result.returncode == 0, result.stderr + result.stdout
    # Pin the exact wording, not just the substring "restore": pytest's
    # tmp_path is derived from this test's own (truncated) function name,
    # which itself contains "restore" and gets echoed into an unrelated
    # "[ERROR] Failed to write ..." log line -- so a looser substring check
    # would pass even if abort_rc's own diagnostic were deleted entirely.
    assert "could not restore original" in result.stdout.lower()
    stage_dirs = [Path(line) for line in mktemp_log.read_text().splitlines() if line]
    assert stage_dirs, "expected the script to stage a temp directory"
    recovered = [d for d in stage_dirs if (d / "orig").exists()]
    assert recovered, "original rc content was not preserved anywhere on failure"
    assert (recovered[0] / "orig").read_text() == "original content\n"
    # The buggy code's mv onto the now-directory $rc succeeded (POSIX mv
    # onto an existing directory renames *into* it), silently misplacing
    # the original at $rc/orig instead of leaving it at the logged stage_dir.
    assert not (rc / "orig").exists()


def test_user_domain_directory_raced_in_before_replace_is_not_false_success(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "ned" / "Applications")
    home = tmp_path / "Users" / "ned"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    rc.write_text("original content\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_mv = shutil.which("mv")
    real_mktemp = shutil.which("mktemp")
    assert real_mv, "mv must be on PATH to run this test"
    assert real_mktemp, "mktemp must be on PATH to run this test"

    shim_dir = tmp_path / "shim-bin"
    shim_dir.mkdir()
    mktemp_log = tmp_path / "mktemp-calls.log"
    _write_executable(
        shim_dir / "mktemp",
        f"""#!/bin/bash
out=$("{real_mktemp}" "$@")
status=$?
if [ "$status" -eq 0 ]; then
  echo "$out" >> "{mktemp_log}"
fi
echo "$out"
exit "$status"
""",
    )
    _write_executable(
        shim_dir / "mv",
        f"""#!/bin/bash
if [ "$1" = "-f" ] && [ "$3" = "{rc}" ]; then
  rm -f "{rc}"
  mkdir -p "{rc}"
fi
exec "{real_mv}" "$@"
""",
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "failed to write" in result.stdout.lower()
    assert rc.is_dir()
    assert not (rc / "rc").exists()
    stage_dirs = [Path(line) for line in mktemp_log.read_text().splitlines() if line]
    recovered = [d for d in stage_dirs if (d / "orig").exists()]
    assert recovered, "original rc content was not preserved anywhere on failure"
    assert (recovered[0] / "orig").read_text() == "original content\n"


def test_user_domain_detach_failure_preserves_original_content(tmp_path: Path):
    """Regression test for the elif branch: if the detach `mv -- "$rc" ...`
    fails for a reason other than $rc not existing (e.g. a permissions
    problem on $home itself), the script must not fall through to the
    fresh-install path and clobber whatever is actually at $rc. Shims mv so
    that only the detach call targeting $rc fails; every other mv call
    (including the ones abort_rc or atomic_replace might make) succeeds."""
    dest = str(tmp_path / "Users" / "uma" / "Applications")
    home = tmp_path / "Users" / "uma"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    rc.write_text("precious user content\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_mv = shutil.which("mv")
    assert real_mv, "mv must be on PATH to run this test"

    shim_dir = tmp_path / "mv-shim-bin"
    shim_dir.mkdir()
    _write_executable(
        shim_dir / "mv",
        f"""#!/bin/bash
if [ "$1" = "--" ] && [ "$2" = "{rc}" ]; then
  exit 1
fi
exec "{real_mv}" "$@"
""",
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "Failed to write" in result.stdout
    assert rc.read_text() == "precious user content\n"


def test_user_domain_signal_after_detach_restores_original_content(tmp_path: Path):
    dest = str(tmp_path / "Users" / "victor" / "Applications")
    home = tmp_path / "Users" / "victor"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    rc.write_text("precious user content\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_mv = shutil.which("mv")
    assert real_mv, "mv must be on PATH to run this test"

    shim_dir = tmp_path / "mv-shim-bin"
    shim_dir.mkdir()
    _write_executable(
        shim_dir / "mv",
        f"""#!/bin/bash
if [ "$1" = "--" ] && [ "$2" = "{rc}" ]; then
  "{real_mv}" "$@" || exit $?
  kill -TERM "$PPID"
  exit 0
fi
exec "{real_mv}" "$@"
""",
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 143, result.stderr + result.stdout
    assert rc.read_text() == "precious user content\n"


def test_user_domain_chown_failure_restores_existing_profile(tmp_path: Path):
    dest = str(tmp_path / "Users" / "wendy" / "Applications")
    home = tmp_path / "Users" / "wendy"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    rc.write_text("precious user content\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    shim_dir = tmp_path / "chown-shim-bin"
    shim_dir.mkdir()
    _write_executable(shim_dir / "chown", "#!/bin/bash\nexit 1\n")

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={
            "PATH": f"{shim_dir}:{os.environ['PATH']}",
            "SNOWFLAKE_CLI_CHOWN": str(shim_dir / "chown"),
        },
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "could not chown" in result.stdout.lower()
    assert rc.read_text() == "precious user content\n"


def test_user_domain_chown_failure_does_not_create_profile(tmp_path: Path):
    dest = str(tmp_path / "Users" / "xena" / "Applications")
    home = tmp_path / "Users" / "xena"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    shim_dir = tmp_path / "chown-shim-bin"
    shim_dir.mkdir()
    _write_executable(shim_dir / "chown", "#!/bin/bash\nexit 1\n")

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={
            "PATH": f"{shim_dir}:{os.environ['PATH']}",
            "SNOWFLAKE_CLI_CHOWN": str(shim_dir / "chown"),
        },
    )

    assert result.returncode == 0, result.stderr + result.stdout
    assert "could not chown" in result.stdout.lower()
    assert not rc.exists()


def test_user_domain_symlink_raced_in_after_detach_is_never_read_through(
    tmp_path: Path,
):
    """Property-level regression test: it isn't enough to prove mv was
    invoked correctly (as test_user_domain_rc_content_is_read_from_detached_copy_not_home_path
    does) -- the actual security property is that nothing downstream ever
    reads through the live $rc path again, even if an attacker manages to
    plant a symlink there in the instant right after the detach. Shims the
    detach `mv` to run the real move and then immediately symlink $rc to a
    root-only secret as a side effect, simulating the tightest possible
    race window; shims cat to log every path it is asked to read. If any
    later read followed $rc instead of the already-detached $src, cat would
    be invoked on $rc (and, since the symlink was raced in, on the secret's
    contents by extension)."""
    dest = str(tmp_path / "Users" / "vera" / "Applications")
    home = tmp_path / "Users" / "vera"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    rc.write_text("# keep me\n")
    dscl = _fake_dscl(tmp_path, "/bin/zsh")
    secret = tmp_path / "root-only-secret"
    secret.write_text("TOP SECRET\n")

    real_mv = shutil.which("mv")
    real_cat = shutil.which("cat")
    assert real_mv, "mv must be on PATH to run this test"
    assert real_cat, "cat must be on PATH to run this test"

    shim_dir = tmp_path / "shim-bin"
    shim_dir.mkdir()
    _write_executable(
        shim_dir / "mv",
        f"""#!/bin/bash
if [ "$1" = "--" ] && [ "$2" = "{rc}" ]; then
  "{real_mv}" "$@" || exit $?
  ln -sfn "{secret}" "{rc}"
  exit 0
fi
exec "{real_mv}" "$@"
""",
    )
    cat_log = tmp_path / "cat-calls.log"
    _write_executable(
        shim_dir / "cat",
        f'#!/bin/bash\necho "$@" >> "{cat_log}"\nexec "{real_cat}" "$@"\n',
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    calls = cat_log.read_text().splitlines() if cat_log.exists() else []
    for call in calls:
        assert str(rc) not in call, (
            "cat was invoked on the live $rc path after it had been raced "
            f"back into existence as a symlink, reopening the read-side "
            f"TOCTOU this test guards against: {call!r}"
        )
    # The race-planted symlink at $rc is left untouched by this script --
    # neither followed nor clobbered -- since abort_rc/atomic_replace both
    # refuse to write through a symlink at the destination.
    assert rc.is_symlink()
    assert secret.read_text() == "TOP SECRET\n"


def test_user_domain_fifo_at_rc_is_refused_instead_of_hanging(tmp_path: Path):
    """Regression test: a FIFO at $rc detaches via mv/rename(2) just like a
    regular file, but block_state's awk would then block in open(2) forever
    waiting for a writer that can never reach the root-only stage_dir --
    wedging this root-owned installer indefinitely and stranding the
    original (nonexistent, in this case) rc content. Bounded with an
    explicit timeout so a regression fails the test instead of hanging CI."""
    dest = str(tmp_path / "Users" / "walt" / "Applications")
    home = tmp_path / "Users" / "walt"
    home.mkdir(parents=True)
    rc = home / ".zprofile"
    os.mkfifo(rc)
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    try:
        result = _run_postinstall(tmp_path, dest, dscl=dscl, timeout=10)
    except subprocess.TimeoutExpired:
        pytest.fail("postinstall hung instead of refusing a non-regular file at $rc")

    assert result.returncode == 0, result.stderr + result.stdout
    assert "not a regular file" in result.stdout.lower()
    assert stat.S_ISFIFO(rc.lstat().st_mode)


def test_system_domain_symlinked_paths_d_is_refused_not_followed(tmp_path: Path):
    dest = "/Applications"
    paths_d = tmp_path / "etc" / "paths.d" / "snowflake-cli"
    paths_d.parent.mkdir(parents=True)
    target = tmp_path / "outside-target"
    target.write_text("untouched\n")
    paths_d.symlink_to(target)

    result = _run_postinstall(tmp_path, dest)

    assert result.returncode == 0, result.stderr + result.stdout
    assert paths_d.is_symlink()
    assert paths_d.resolve() == target
    assert target.read_text() == "untouched\n"
    assert "symlink" in result.stdout.lower()


def test_user_domain_quotes_install_path_against_shell_injection(tmp_path: Path):
    marker = tmp_path / "pwned"
    dest = str(
        tmp_path / "Users" / "harry" / f"$(touch {marker})' ; touch {marker} ; '"
    )
    home = tmp_path / "Users" / "harry"
    home.mkdir(parents=True)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert not marker.exists()
    zprofile = home / ".zprofile"
    text = zprofile.read_text()
    assert BEGIN_MARK in text

    sourced = subprocess.run(
        ["bash", "-c", f"source {zprofile} && echo sourced-ok"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert sourced.returncode == 0, sourced.stderr + sourced.stdout
    assert "sourced-ok" in sourced.stdout
    assert not marker.exists()


def test_user_domain_crlf_marked_block_is_recognized_and_replaced(tmp_path: Path):
    dest = str(tmp_path / "Users" / "ivy" / "Applications")
    home = tmp_path / "Users" / "ivy"
    home.mkdir(parents=True)
    zprofile = home / ".zprofile"
    old_path = str(tmp_path / "Users" / "ivy" / "OldApp" / MACOS_SUFFIX)
    zprofile.write_bytes(
        f"{BEGIN_MARK}\r\nexport PATH={old_path}:$PATH\r\n{END_MARK}\r\n".encode()
    )

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert "malformed" not in result.stdout.lower()
    text = zprofile.read_text()
    assert text.count(BEGIN_MARK) == 1
    assert old_path not in text
    assert _path_export_line(dest) in text


def test_user_domain_repeated_runs_are_idempotent(tmp_path: Path):
    dest = str(tmp_path / "Users" / "jane" / "Applications")
    home = tmp_path / "Users" / "jane"
    home.mkdir(parents=True)
    dscl = _fake_dscl(tmp_path, "/bin/zsh")
    zprofile = home / ".zprofile"

    first = _run_postinstall(tmp_path, dest, dscl=dscl)
    assert first.returncode == 0, first.stderr + first.stdout
    first_text = zprofile.read_text()

    second = _run_postinstall(tmp_path, dest, dscl=dscl)
    assert second.returncode == 0, second.stderr + second.stdout
    second_text = zprofile.read_text()

    assert first_text == second_text
    assert second_text.count(BEGIN_MARK) == 1
    assert second_text.count(END_MARK) == 1


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0,
    reason="root bypasses file permission bits",
)
def test_user_domain_unwritable_rc_fails_gracefully_without_corruption(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "kate" / "Applications")
    home = tmp_path / "Users" / "kate"
    home.mkdir(parents=True)
    zprofile = home / ".zprofile"
    zprofile.write_text("keep me\n")
    zprofile.chmod(0o444)

    try:
        result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))
    finally:
        zprofile.chmod(0o644)

    assert result.returncode == 0, result.stderr + result.stdout
    assert zprofile.read_text() == "keep me\n"
    assert "failed" in result.stdout.lower()


def test_system_domain_symlinked_parent_dir_is_refused_not_followed(tmp_path: Path):
    dest = "/Applications"
    paths_d_dir = tmp_path / "etc" / "paths.d"
    paths_d_dir.parent.mkdir(parents=True)
    target_dir = tmp_path / "outside-target-dir"
    target_dir.mkdir()
    outside_file = target_dir / "snowflake-cli"
    outside_file.write_text("untouched\n")
    paths_d_dir.symlink_to(target_dir)

    result = _run_postinstall(tmp_path, dest)

    assert result.returncode == 0, result.stderr + result.stdout
    assert paths_d_dir.is_symlink()
    assert outside_file.read_text() == "untouched\n"
    assert "symlink" in result.stdout.lower()


def test_user_domain_symlinked_home_is_refused_not_followed(tmp_path: Path):
    dest = str(tmp_path / "Users" / "leo" / "Applications")
    users_root = tmp_path / "Users"
    users_root.mkdir(parents=True)
    target_dir = tmp_path / "outside-home"
    target_dir.mkdir()
    outside_rc = target_dir / ".zprofile"
    outside_rc.write_text("untouched\n")
    home = users_root / "leo"
    home.symlink_to(target_dir)

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert home.is_symlink()
    assert outside_rc.read_text() == "untouched\n"
    assert "symlink" in result.stdout.lower()


def test_user_domain_apostrophe_in_dest_survives_replace_path(tmp_path: Path):
    dest = str(tmp_path / "Users" / "mona" / "App's Folder")
    home = tmp_path / "Users" / "mona"
    home.mkdir(parents=True)
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    first = _run_postinstall(tmp_path, dest, dscl=dscl)
    assert first.returncode == 0, first.stderr + first.stdout

    other_dest = str(tmp_path / "Users" / "mona" / "Another's Folder")
    second = _run_postinstall(tmp_path, other_dest, dscl=dscl)
    assert second.returncode == 0, second.stderr + second.stdout

    zprofile = home / ".zprofile"
    text = zprofile.read_text()
    assert text.count(BEGIN_MARK) == 1
    assert _macos_dir(dest) not in text

    sourced = subprocess.run(
        ["bash", "-c", f"source '{zprofile}' && printf '%s' \"$PATH\""],
        check=False,
        capture_output=True,
        text=True,
    )
    assert sourced.returncode == 0, sourced.stderr + sourced.stdout
    assert sourced.stdout.split(":")[0] == _macos_dir(other_dest)


def test_user_domain_lf_only_blank_line_between_markers_is_recognized(
    tmp_path: Path,
):
    dest = str(tmp_path / "Users" / "oscar" / "Applications")
    home = tmp_path / "Users" / "oscar"
    home.mkdir(parents=True)
    zprofile = home / ".zprofile"
    old_path = str(tmp_path / "Users" / "oscar" / "OldApp" / MACOS_SUFFIX)
    zprofile.write_text(f"{BEGIN_MARK}\nexport PATH={old_path}:$PATH\n\n{END_MARK}\n")

    result = _run_postinstall(tmp_path, dest, dscl=_fake_dscl(tmp_path, "/bin/zsh"))

    assert result.returncode == 0, result.stderr + result.stdout
    assert "malformed" not in result.stdout.lower()
    text = zprofile.read_text()
    assert text.count(BEGIN_MARK) == 1
    assert old_path not in text
    assert _path_export_line(dest) in text


def test_user_domain_bash_repeated_runs_are_idempotent(tmp_path: Path):
    dest = str(tmp_path / "Users" / "pia" / "Applications")
    home = tmp_path / "Users" / "pia"
    home.mkdir(parents=True)
    dscl = _fake_dscl(tmp_path, "/bin/bash")
    bash_profile = home / ".bash_profile"
    bash_profile.write_text("# keep me\n")

    first = _run_postinstall(tmp_path, dest, dscl=dscl)
    assert first.returncode == 0, first.stderr + first.stdout
    first_text = bash_profile.read_text()

    second = _run_postinstall(tmp_path, dest, dscl=dscl)
    assert second.returncode == 0, second.stderr + second.stdout
    second_text = bash_profile.read_text()

    assert first_text == second_text
    assert second_text.count(BEGIN_MARK) == 1
    assert second_text.count(END_MARK) == 1


def test_user_domain_temp_file_is_never_staged_inside_home(tmp_path: Path):
    """Regression test for a TOCTOU privilege escalation found in review:
    building the rc replacement in a temp file located inside $home let the
    target (non-root) user, who owns that directory, delete the freshly
    mktemp'd file and replace it with a symlink before root's later write,
    chmod, or chown -- each done by pathname -- ran on it, redirecting root
    onto a file of the attacker's choosing. Every mktemp call the script
    makes while handling an rc file must resolve outside $home."""
    dest = str(tmp_path / "Users" / "quinn" / "Applications")
    home = tmp_path / "Users" / "quinn"
    home.mkdir(parents=True)
    dscl = _fake_dscl(tmp_path, "/bin/zsh")

    real_mktemp = shutil.which("mktemp")
    assert real_mktemp, "mktemp must be on PATH to run this test"

    shim_dir = tmp_path / "shim-bin"
    shim_dir.mkdir()
    # configure_user_domain's mktemp -d call takes no template argument, so
    # logging *arguments* (as an earlier version of this test did) can never
    # observe a path inside $home no matter where mktemp actually resolves
    # its default directory to -- log the resolved *output* path instead,
    # which is what would actually leak PATH-block content to the attacker.
    mktemp_log = tmp_path / "mktemp-calls.log"
    _write_executable(
        shim_dir / "mktemp",
        f"""#!/bin/bash
out=$("{real_mktemp}" "$@")
status=$?
if [ "$status" -eq 0 ]; then
  echo "$out" >> "{mktemp_log}"
fi
echo "$out"
exit "$status"
""",
    )

    result = _run_postinstall(
        tmp_path,
        dest,
        dscl=dscl,
        extra_env={"PATH": f"{shim_dir}:{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr + result.stdout
    calls = mktemp_log.read_text().splitlines()
    assert calls, "expected the script to call mktemp while writing the rc file"
    for call in calls:
        assert str(home) not in call, (
            "mktemp resolved to a path inside $home, which is writable by "
            f"the target user, reopening the TOCTOU this test guards "
            f"against: {call!r}"
        )


def test_system_domain_no_stray_files_left_in_paths_d(tmp_path: Path):
    """path_helper(8) appends the contents of every file it finds directly
    inside /etc/paths.d to PATH, so a stray temp file left there (e.g. by a
    staging directory placed inside paths.d itself) would leak into every
    user's PATH, not just clutter the directory."""
    dest = "/Applications"
    paths_d_dir = tmp_path / "etc" / "paths.d"

    result = _run_postinstall(tmp_path, dest)

    assert result.returncode == 0, result.stderr + result.stdout
    assert [p.name for p in paths_d_dir.iterdir()] == ["snowflake-cli"]


def test_welcome_html_describes_both_domains_without_python_path_or_bak():
    text = WELCOME_HTML.read_text()
    lower = text.lower()
    assert "paths.d" in lower or "/etc/paths.d" in text
    assert ".zprofile" in text or "zprofile" in lower
    assert ".bash_profile" in text or "bash_profile" in lower
    assert ".bash_login" in text
    assert "~/.profile" in text
    assert 'PATH="$HOME/Applications/SnowflakeCLI.app/Contents/MacOS:$PATH"' in text
    assert "does not create one" in lower
    assert "python library" not in lower
    assert ".bak" not in lower
    assert "snowflake.bak" not in lower


def test_conclusion_html_keeps_new_terminal_and_connection_add_without_helper():
    text = CONCLUSION_HTML.read_text()
    lower = text.lower()
    assert "new terminal" in lower
    assert "snow connection add" in text
    assert "clean-installer-path" not in text
    assert "helpers" not in lower
