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

import json
import os
import shlex
import signal
import subprocess
import sys
from pathlib import Path

import pytest
from snowflake.cli._plugins.upgrade import layout as layout_mod
from snowflake.cli._plugins.upgrade import spawn
from snowflake.cli._plugins.upgrade.controls import AUTO_UPGRADE_DONE_ENV
from snowflake.cli._plugins.upgrade.layout import MANAGED_HOME_ENV, ManagedLayout
from snowflake.cli._plugins.upgrade.spawn import run_new_binary

from tests_common import IS_WINDOWS

_SENTINEL = "SNOW_SPAWN_SENTINEL"
_USER_ARGV = ["sql", "--format", "json", "-q", "select 1"]
_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def managed_home(tmp_path, monkeypatch):
    root = tmp_path / "snowflake-cli"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(root))
    return root


def _forbid_exec(name: str):
    def _boom(*_args, **_kwargs):
        raise AssertionError(name)

    return _boom


def _capture_spawn(monkeypatch, argv, system_name: str, returncode: int = 7):
    monkeypatch.setattr(layout_mod.platform, "system", lambda: system_name)
    monkeypatch.delenv(AUTO_UPGRADE_DONE_ENV, raising=False)
    monkeypatch.setenv(_SENTINEL, "kept")
    captured: dict = {}

    def fake_run(args, **kwargs):
        captured["args"] = [str(arg) for arg in args]
        captured["kwargs"] = kwargs
        captured["parent_sigint"] = signal.getsignal(signal.SIGINT)
        preexec = kwargs.get("preexec_fn")
        if preexec is not None:
            preexec()
            captured["child_sigint"] = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        return subprocess.CompletedProcess(list(args), returncode)

    monkeypatch.setattr(spawn.subprocess, "run", fake_run)

    def fake_exit(code):
        raise SystemExit(code)

    monkeypatch.setattr(spawn.os, "_exit", fake_exit)
    for name in ("execv", "execve", "execvpe", "execl", "execle"):
        monkeypatch.setattr(spawn.os, name, _forbid_exec(name), raising=False)

    previous_sigint = signal.getsignal(signal.SIGINT)
    try:
        with pytest.raises(SystemExit) as caught:
            run_new_binary("3.12.0", argv)
        expected = 128 - returncode if returncode < 0 else returncode
        assert caught.value.code == expected
    finally:
        signal.signal(signal.SIGINT, previous_sigint)
    assert os.environ.get(AUTO_UPGRADE_DONE_ENV) is None
    return captured


def _assert_inherited_call(captured, argv, system_name: str):
    kwargs = captured["kwargs"]
    assert "stdin" not in kwargs
    assert "stdout" not in kwargs
    assert "stderr" not in kwargs
    assert kwargs.get("shell") is not True
    assert kwargs["env"] is not os.environ
    assert kwargs["env"][AUTO_UPGRADE_DONE_ENV] == "1"
    assert kwargs["env"][_SENTINEL] == "kept"
    binary = Path(captured["args"][0])
    assert binary == ManagedLayout().binary_path("3.12.0")
    assert binary.parent.name == "3.12.0"
    assert "bin" not in binary.parts
    if system_name == "Windows":
        assert binary.name == "snow.exe"
    else:
        assert binary.name == "snow"
    assert captured["args"][1:] == list(argv)
    assert captured["parent_sigint"] is signal.SIG_IGN
    if os.name == "nt":
        assert "preexec_fn" not in kwargs
    else:
        assert captured["child_sigint"] is signal.SIG_DFL


def test_unix_spawn_keeps_argv_env_and_stdio(monkeypatch):
    captured = _capture_spawn(monkeypatch, _USER_ARGV, "Linux")
    _assert_inherited_call(captured, _USER_ARGV, "Linux")


def test_windows_spawn_targets_version_dir_exe(monkeypatch):
    captured = _capture_spawn(monkeypatch, _USER_ARGV, "Windows")
    _assert_inherited_call(captured, _USER_ARGV, "Windows")


def test_spawn_passes_empty_argv_unchanged(monkeypatch):
    captured = _capture_spawn(monkeypatch, [], "Linux")
    _assert_inherited_call(captured, [], "Linux")


def test_signal_death_maps_to_shell_status(monkeypatch):
    returncode = -signal.SIGTERM
    captured = _capture_spawn(monkeypatch, _USER_ARGV, "Linux", returncode=returncode)
    _assert_inherited_call(captured, _USER_ARGV, "Linux")


@pytest.mark.parametrize("exc_type", [FileNotFoundError, PermissionError])
def test_spawn_error_propagates_and_restores_sigint(monkeypatch, exc_type):
    def fake_run(*_args, **_kwargs):
        raise exc_type("spawn failed")

    monkeypatch.setattr(spawn.subprocess, "run", fake_run)
    monkeypatch.setattr(spawn.os, "_exit", _forbid_exec("os._exit"))
    previous = signal.getsignal(signal.SIGINT)
    with pytest.raises(exc_type, match="spawn failed"):
        run_new_binary("3.12.0", _USER_ARGV)
    assert signal.getsignal(signal.SIGINT) is previous


def _write_parent_driver(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "import sys",
                "from pathlib import Path",
                "marker = Path(sys.argv[1])",
                "version = sys.argv[2]",
                "user_argv = sys.argv[3:]",
                "def _run():",
                "    from snowflake.cli._plugins.upgrade.spawn import run_new_binary",
                "    try:",
                "        run_new_binary(version, user_argv)",
                "    finally:",
                '        marker.write_text("ran", encoding="utf-8", newline="\\n")',
                "if __name__ == '__main__':",
                "    _run()",
                "",
            ]
        ),
        encoding="utf-8",
        newline="\n",
    )


def _parent_env(managed_home: Path) -> dict:
    env = os.environ.copy()
    env[MANAGED_HOME_ENV] = str(managed_home)
    env[_SENTINEL] = "kept"
    env[AUTO_UPGRADE_DONE_ENV] = "0"
    env["PYTHONPATH"] = os.pathsep.join(
        [
            str(_REPO_ROOT / "src"),
            str(_REPO_ROOT / "tests_common"),
            env.get("PYTHONPATH", ""),
        ]
    )
    return env


def _run_parent(driver: Path, marker: Path, managed_home: Path, *argv: str):
    return subprocess.run(
        [sys.executable, str(driver), str(marker), "3.12.0", *argv],
        input="PING\n",
        capture_output=True,
        text=True,
        env=_parent_env(managed_home),
        check=False,
    )


def _write_sh_wrapper(binary: Path, child_source: str) -> None:
    """``#!/bin/sh`` then ``exec`` a quoted interpreter.

    ``#!{sys.executable}`` breaks when Hatch's Python lives under a path with
    spaces (default macOS Hatch). The kernel splits that shebang.
    """
    binary.parent.mkdir(parents=True, exist_ok=True)
    helper = binary.with_name("_spawn_child.py")
    helper.write_text(child_source, encoding="utf-8", newline="\n")
    binary.write_text(
        "\n".join(
            [
                "#!/bin/sh",
                (
                    "exec "
                    f"{shlex.quote(sys.executable)} {shlex.quote(str(helper))} "
                    '"$0" "$@"'
                ),
                "",
            ]
        ),
        encoding="utf-8",
        newline="\n",
    )
    binary.chmod(0o755)


@pytest.mark.skipif(IS_WINDOWS, reason="version-dir snow is a Python script")
def test_spawn_child_inherits_stdio_and_parent_exits_with_child_code(
    managed_home, tmp_path
):
    binary = ManagedLayout().binary_path("3.12.0")
    _write_sh_wrapper(
        binary,
        "\n".join(
            [
                "import json, os, sys",
                "payload = {",
                '    "argv0": sys.argv[1],',
                '    "argv": sys.argv[2:],',
                f'    "done": os.environ.get("{AUTO_UPGRADE_DONE_ENV}"),',
                f'    "sentinel": os.environ.get("{_SENTINEL}"),',
                '    "stdin": sys.stdin.readline().rstrip("\\n"),',
                "}",
                "sys.stdout.write(json.dumps(payload))",
                "sys.stdout.flush()",
                'sys.stderr.write("progress-stderr\\n")',
                "sys.stderr.flush()",
                "raise SystemExit(7)",
                "",
            ]
        ),
    )
    assert binary.read_text(encoding="utf-8").startswith("#!/bin/sh\n")

    marker = tmp_path / "finally-ran"
    driver = tmp_path / "driver.py"
    _write_parent_driver(driver)
    completed = _run_parent(driver, marker, managed_home, *_USER_ARGV)

    assert completed.returncode == 7
    assert not marker.exists()
    assert "progress-stderr" in completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["argv"] == _USER_ARGV
    assert "--no-auto-upgrade" not in payload["argv"]
    assert payload["done"] == "1"
    assert payload["sentinel"] == "kept"
    assert payload["stdin"] == "PING"
    assert Path(payload["argv0"]).resolve() == binary.resolve()


@pytest.mark.skipif(IS_WINDOWS, reason="POSIX wait status for a signal death")
def test_spawn_real_sigterm_exits_128_plus_signal(managed_home, tmp_path):
    binary = ManagedLayout().binary_path("3.12.0")
    _write_sh_wrapper(
        binary,
        "\n".join(
            [
                "import os, signal",
                "os.kill(os.getpid(), signal.SIGTERM)",
                "",
            ]
        ),
    )
    marker = tmp_path / "finally-ran"
    driver = tmp_path / "driver.py"
    _write_parent_driver(driver)
    completed = _run_parent(driver, marker, managed_home)

    assert completed.returncode == 128 + signal.SIGTERM
    assert not marker.exists()


def test_missing_binary_runs_parent_finally(managed_home, tmp_path):
    marker = tmp_path / "finally-ran"
    driver = tmp_path / "driver.py"
    _write_parent_driver(driver)
    completed = _run_parent(driver, marker, managed_home, *_USER_ARGV)

    assert completed.returncode != 0
    assert marker.read_text(encoding="utf-8") == "ran"
