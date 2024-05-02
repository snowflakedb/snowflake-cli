import os
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Optional

from click.exceptions import ClickException


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


@contextmanager
def _temp_script_file(script_source: str):
    script_file = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=True)
    try:
        script_file.write(script_source)
        script_file.flush()

        yield script_file.name
    finally:
        script_file.close()


def _execute_in_venv(
    script_source: str, venv_path: Optional[Path] = None
) -> subprocess.CompletedProcess:
    resolved_venv_path = None
    if venv_path is None:
        active_venv_dir = _get_active_venv_dir()
        if active_venv_dir is not None:
            resolved_venv_path = Path(active_venv_dir).resolve()
    else:
        resolved_venv_path = venv_path.resolve()

    if resolved_venv_path is None:
        raise SandboxExecutionError("No venv root found")

    if not resolved_venv_path.is_dir():
        raise SandboxExecutionError(
            f"venv path should be an existing directory: {resolved_venv_path}"
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

    with _temp_script_file(script_source) as script_file:
        return subprocess.run(
            [python_executable, script_file], capture_output=True, text=True
        )


def _execute_in_conda_env(
    script_source: str, env_name: Optional[str] = None
) -> subprocess.CompletedProcess:
    conda_env = env_name
    if conda_env is None:
        conda_env = _get_active_conda_env()
    if conda_env is None:
        raise SandboxExecutionError("No conda environment found")

    if shutil.which("conda") is None:
        raise SandboxExecutionError(
            "conda command not found, make sure it is installed on your system and in your PATH"
        )

    with _temp_script_file(script_source) as script_file:
        # conda run removes the need to activate the environment, as would typically be done in an interactive shell
        return subprocess.run(
            [
                "conda",
                "run",
                "-n",
                conda_env,
                "--no-capture-output",
                "python3",
                script_file,
            ],
            capture_output=True,
            text=True,
        )


def _execute_with_system_python(script_source: str) -> subprocess.CompletedProcess:
    python_executable = (
        shutil.which("python3") or shutil.which("python") or sys.executable
    )
    if not python_executable:
        raise SandboxExecutionError("No python executable found")

    with _temp_script_file(script_source) as script_file:
        return subprocess.run(
            [python_executable, script_file], capture_output=True, text=True
        )


class ExecutionEnvironmentType(Enum):
    AUTO_DETECT = 0  # auto-detect the current python interpreter by looking for an active virtual environment
    VENV = 1  # use the python interpreter specified by a venv environment
    CONDA = 2  # use the python interpreter specified by a conda environment
    SYSTEM_DEFAULT = 3  # use the system's default python interpreter


def execute_script_in_sandbox(
    script_source: str,
    env_type: ExecutionEnvironmentType = ExecutionEnvironmentType.AUTO_DETECT,
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
        **kwargs: Additional keyword arguments used by specific execution environments, as follows:
            - venv environments accept a 'path' argument to specify the venv root directory.
            - conda environments accept a 'name' argument to specify the name of the conda environment to use.
    Returns:
        A subprocess.CompletedProcess object containing the output of the script, if any.
    """
    if env_type == ExecutionEnvironmentType.AUTO_DETECT:
        if _is_venv_active():
            return _execute_in_venv(script_source)
        elif _is_conda_active():
            return _execute_in_conda_env(script_source)
        else:
            return _execute_with_system_python(script_source)
    elif env_type == ExecutionEnvironmentType.VENV:
        return _execute_in_venv(script_source, kwargs.get("path"))
    elif env_type == ExecutionEnvironmentType.CONDA:
        return _execute_in_conda_env(script_source, kwargs.get("name"))
    else:
        return _execute_with_system_python(script_source)
