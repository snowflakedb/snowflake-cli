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
import subprocess
import sys
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union
from venv import EnvBuilder

from click.exceptions import ClickException

EnvVars = Mapping[str, str]  # Only support str -> str for cross-platform compatibility


class SandboxExecutionError(ClickException):
    """An error occurred while executing a python script."""

    def __init__(self, error: str):
        super().__init__(f"Failed to execute python script. {error}")


def _get_active_venv_dir() -> Optional[str]:
    return os.environ.get("VIRTUAL_ENV")


def _get_active_conda_env() -> Optional[str]:
    return os.environ.get("CONDA_DEFAULT_ENV")


def _is_venv_active() -> bool:
    return _get_active_venv_dir() is not None


def _is_conda_active() -> bool:
    return _get_active_conda_env() is not None


def _is_ms_windows() -> bool:
    return sys.platform == "win32"


def _execute_python_interpreter(
    python_executable: Optional[Union[str, Path, Sequence[str]]],
    script_source: str,
    cwd: Optional[Union[str, Path]],
    timeout: Optional[int],
    env_vars: Optional[EnvVars],
) -> subprocess.CompletedProcess:
    if not python_executable:
        raise SandboxExecutionError("No python executable found")

    if isinstance(python_executable, str) or isinstance(python_executable, Path):
        args = [python_executable]
    else:
        args = [arg for arg in python_executable]
    args.append("-")
    return subprocess.run(
        args,
        capture_output=True,
        text=True,
        input=script_source,
        timeout=timeout,
        cwd=cwd,
        env=env_vars,
    )


def _execute_in_venv(
    script_source: str,
    venv_path: Optional[Union[str, Path]] = None,
    cwd: Optional[Union[str, Path]] = None,
    timeout: Optional[int] = None,
    env_vars: Optional[EnvVars] = None,
) -> subprocess.CompletedProcess:
    resolved_venv_path = None
    if venv_path is None:
        active_venv_dir = _get_active_venv_dir()
        if active_venv_dir is not None:
            resolved_venv_path = Path(active_venv_dir).resolve()
    elif isinstance(venv_path, str):
        resolved_venv_path = Path(venv_path).resolve()
    else:
        resolved_venv_path = venv_path.resolve()

    if resolved_venv_path is None:
        raise SandboxExecutionError("No venv root found")

    if not resolved_venv_path.is_dir():
        raise SandboxExecutionError(
            f"venv path must be an existing directory: {resolved_venv_path}"
        )

    # find the python interpreter for this environment. There is no need to activate environment prior to invoking the
    # interpreter, as venv maintains the invariant that invoking any of the scripts will set up the virtual environment
    # correctly. activation scripts are only used for convenience in interactive shells.
    if _is_ms_windows():
        python_executable = resolved_venv_path / "Scripts" / "python.exe"
    else:
        python_executable = resolved_venv_path / "bin" / "python3"

    if not python_executable.is_file():
        raise SandboxExecutionError(
            f"No venv python executable found: {resolved_venv_path}"
        )

    return _execute_python_interpreter(
        python_executable, script_source, timeout=timeout, cwd=cwd, env_vars=env_vars
    )


def _execute_in_conda_env(
    script_source: str,
    env_name: Optional[str] = None,
    cwd: Optional[Union[str, Path]] = None,
    timeout: Optional[int] = None,
    env_vars: Optional[EnvVars] = None,
) -> subprocess.CompletedProcess:
    conda_env = env_name
    if conda_env is None:
        conda_env = _get_active_conda_env()
        if conda_env is None:
            raise SandboxExecutionError("No conda environment found")

    conda_exec = shutil.which("conda")
    if conda_exec is None:
        raise SandboxExecutionError(
            "conda command not found, make sure it is installed on your system and in your PATH"
        )

    # conda run removes the need to activate the environment, as would typically be done in an interactive shell
    return _execute_python_interpreter(
        [conda_exec, "run", "-n", conda_env, "--no-capture-output", "python"],
        script_source,
        timeout=timeout,
        cwd=cwd,
        env_vars=env_vars,
    )


def _execute_with_system_path_python(
    script_source: str,
    cwd: Optional[Union[str, Path]] = None,
    timeout: Optional[int] = None,
    env_vars: Optional[EnvVars] = None,
) -> subprocess.CompletedProcess:
    python_executable = (
        shutil.which("python3") or shutil.which("python") or sys.executable
    )

    return _execute_python_interpreter(
        python_executable,
        script_source,
        timeout=timeout,
        cwd=cwd,
        env_vars=env_vars,
    )


class ExecutionEnvironmentType(Enum):
    AUTO_DETECT = 0  # auto-detect the current python interpreter by looking for an active virtual environment
    VENV = 1  # use the python interpreter specified by a venv environment
    CONDA = 2  # use the python interpreter specified by a conda environment
    SYSTEM_PATH = 3  # search for a python interpreter in the system path
    CURRENT = 4  # Use the python interpreter that is currently executing (i.e. `sys.executable`)


def execute_script_in_sandbox(
    script_source: str,
    env_type: ExecutionEnvironmentType = ExecutionEnvironmentType.AUTO_DETECT,
    cwd: Optional[Union[str, Path]] = None,
    timeout: Optional[int] = None,
    env_vars: Optional[EnvVars] = None,
    **kwargs,
) -> subprocess.CompletedProcess:
    """
    Executes a python script in a sandboxed environment, and returns its output. The script is executed in a different
    process. The execution environment is determined by the `env_type` argument. By default, the logic will attempt
    to auto-detect the correct environment by looking for an active venv or conda environment. If none can be found, it
    will use the system's default python executable, as determined by the user's path. As a last resort, the current
    python execution environment will be used (still in a subprocess).

    Parameters:
        script_source (str): The python script to be executed, as a string.
        env_type: The type of execution environment to use (default: ExecutionEnvironmentType.AUTO_DETECT).
        cwd (Optional[Union[str, Path]]): An optional path to use as the current directory when executing the script.
        timeout (Optional[int]): An optional timeout in seconds when executing the script. Defaults to no timeout.
        **kwargs: Additional keyword arguments used by specific execution environments, as follows:
            - venv environments accept a 'path' argument to specify the venv root directory.
            - conda environments accept a 'name' argument to specify the name of the conda environment to use.
    Returns:
        A subprocess.CompletedProcess object containing the output of the script, if any.
    """
    if env_type == ExecutionEnvironmentType.AUTO_DETECT:
        if _is_venv_active():
            return _execute_in_venv(
                script_source, cwd=cwd, timeout=timeout, env_vars=env_vars
            )
        elif _is_conda_active():
            return _execute_in_conda_env(
                script_source, cwd=cwd, timeout=timeout, env_vars=env_vars
            )
        else:
            return _execute_with_system_path_python(
                script_source, cwd=cwd, timeout=timeout, env_vars=env_vars
            )
    elif env_type == ExecutionEnvironmentType.VENV:
        return _execute_in_venv(
            script_source,
            kwargs.get("path"),
            cwd=cwd,
            timeout=timeout,
            env_vars=env_vars,
        )
    elif env_type == ExecutionEnvironmentType.CONDA:
        return _execute_in_conda_env(
            script_source,
            kwargs.get("name"),
            cwd=cwd,
            timeout=timeout,
            env_vars=env_vars,
        )
    elif env_type == ExecutionEnvironmentType.SYSTEM_PATH:
        return _execute_with_system_path_python(
            script_source, cwd=cwd, timeout=timeout, env_vars=env_vars
        )
    else:  # ExecutionEnvironmentType.CURRENT
        return _execute_python_interpreter(
            sys.executable, script_source, cwd=cwd, timeout=timeout, env_vars=env_vars
        )


class SandboxEnvBuilder(EnvBuilder):
    """
    A virtual environment builder that can be used to build an environment suitable for
    executing user-provided python scripts in an isolated sandbox.
    """

    def __init__(self, path: Path, **kwargs) -> None:
        """
        Creates a new builder with the specified destination path. The path need not
        exist, it will be created when needed (recursively if necessary).

        Parameters:
            path (Path): The directory in which the sandbox environment will be created.
        """
        super().__init__(**kwargs)
        self.path = path
        self._context: Any = None  # cached context

    def post_setup(self, context) -> None:
        self._context = context

    def ensure_created(self) -> None:
        """
        Ensures that the sandbox environment has been created and correctly initialized.
        """
        if self.path.exists():
            self._context = self.ensure_directories(self.path)
        else:
            self.path.mkdir(parents=True, exist_ok=True)
            self.create(
                self.path
            )  # will set self._context through the post_setup callback

    def run_python(self, *args) -> str:
        """
        Executes the python interpreter in the sandboxed environment with the provided arguments.
        This raises a CalledProcessError if the python interpreter was not executed successfully.

        Returns:
            The output of running the command.
        """
        positional_args = [
            self._context.env_exe,
            "-E",  # passing -E ignores all PYTHON* env vars
            *args,
        ]
        kwargs = {
            "cwd": self._context.env_dir,
            "stderr": subprocess.STDOUT,
        }
        env = dict(os.environ)
        env["VIRTUAL_ENV"] = self._context.env_dir
        return subprocess.check_output(positional_args, **kwargs)

    def pip_install(self, *args: Any) -> None:
        """
        Invokes pip install with the provided arguments.
        """
        self.run_python("-m", "pip", "install", *[str(arg) for arg in args])
