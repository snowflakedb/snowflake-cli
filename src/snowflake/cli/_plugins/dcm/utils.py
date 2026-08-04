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
import logging
import os
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Generator

from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import EmptyResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.stage_path import StagePath

log = logging.getLogger(__name__)

OUTPUT_FOLDER = "out"

# raw-analyze's artifacts are named after the backend's own compile output, so
# AnalyzeReporter must write and look for that name rather than the command's.
RAW_ANALYZE_COMMAND_NAME = "compile"


def result_file_name(command_name: str) -> str:
    return f"{command_name}_result.json"


def prepare_output_folder() -> None:
    """Recreate the out/ directory empty, so a run never mixes its artifacts with
    anything an earlier one left behind."""
    output_dir = SecurePath(OUTPUT_FOLDER)
    if output_dir.exists():
        log.debug("Dropping previous output folder %s", output_dir.path.resolve())
        output_dir.rmdir(recursive=True)

    output_dir.mkdir(parents=True)


def result_file_exists(command_name: str) -> bool:
    return (SecurePath(OUTPUT_FOLDER) / result_file_name(command_name)).exists()


def _output_folder_has_files() -> bool:
    output_dir = SecurePath(OUTPUT_FOLDER)
    return output_dir.exists() and any(
        path.is_file() for path in output_dir.path.rglob("*")
    )


def announce_output_artifacts() -> None:
    if _output_folder_has_files():
        cli_console.step(
            f"Artifacts saved to: {SecurePath(OUTPUT_FOLDER).path.resolve()}"
        )


def save_command_response(
    command_name: str,
    raw_data: Dict[str, Any] | str,
) -> None:
    """Save raw JSON response to out/<command>_result.json.

    Does nothing when the file is already there - out/ is recreated empty at
    command entry, so it can only exist because the backend's download produced
    it, and that copy is richer than the raw response.
    """
    if result_file_exists(command_name):
        log.debug("Response file already exists. Will not recreate it.")
        return

    output_dir = SecurePath(OUTPUT_FOLDER)
    json_file = output_dir / result_file_name(command_name)
    log.debug("Saving response to %s", json_file.path.resolve())
    try:
        if isinstance(raw_data, str):
            json_file.write_text(raw_data)
        else:
            json_file.write_text(json.dumps(raw_data))
    except Exception as e:
        raise CliError(
            f"Failed to save command response to {json_file.path.resolve()}: {e}"
        )
    log.info(
        "Saved raw JSON response for command '%s' in %s.",
        command_name,
        json_file.path.resolve(),
    )


@contextmanager
def command_artifacts(save_output: bool) -> Generator[None, None, None]:
    """Recreate the out/ folder, then announce whatever the command produced.

    Both only happen with --save-output, so a run that writes nothing back leaves
    an earlier run's artifacts alone and stays quiet about them.

    Announcing from a finally covers the failure path too: collect_output performs
    a best-effort download when the command fails, so the user is told where those
    artifacts landed even though the command errors out.
    """
    if save_output:
        prepare_output_folder()
    try:
        yield
    finally:
        if save_output:
            announce_output_artifacts()


@contextmanager
def collect_output(
    project_identifier: FQN,
    command_name: str,
) -> Generator[str, None, None]:
    """
    Context manager for handling command output artifacts - creates temporary stage,
    downloads files to the out/ folder after execution.

    Args:
        project_identifier: The DCM project identifier
        command_name: Name of the command, used for logging

    Yields:
        str: The output stage path to use in the DCM command
    """
    stage_manager = StageManager()
    temp_stage_fqn = FQN.from_resource(
        ObjectType.DCM_PROJECT, project_identifier, "OUTPUT_TMP_STAGE"
    )
    log.info(
        "Creating temporary output stage for DCM %s artifacts (project_identifier=%s, stage=%s).",
        command_name,
        project_identifier,
        temp_stage_fqn.identifier,
    )
    stage_manager.create(temp_stage_fqn, temporary=True)
    effective_output_path = StagePath.from_stage_str(
        temp_stage_fqn.identifier
    ).joinpath("/outputs")
    output_dir = SecurePath(OUTPUT_FOLDER)

    def _download_artifacts() -> None:
        log.info(
            "Downloading DCM %s artifacts from stage to local path (project_identifier=%s, stage_path=%s, local_path=%s).",
            command_name,
            project_identifier,
            effective_output_path.absolute_path(),
            output_dir.path.resolve(),
        )
        stage_manager.get_recursive(
            stage_path=effective_output_path.absolute_path(),
            dest_path=output_dir.path,
        )

    try:
        yield effective_output_path.absolute_path()
    except Exception:
        try:
            _download_artifacts()
        except Exception as download_error:
            log.warning(
                "Failed to download DCM %s artifacts after failure (project_identifier=%s): %s",
                command_name,
                project_identifier,
                download_error,
            )
        raise
    else:
        _download_artifacts()


class FakeCursor:
    def __init__(self, data: Any):
        self._data = data
        self._fetched = False

    def fetchone(self):
        if self._fetched:
            return None
        self._fetched = True
        return (json.dumps(self._data),)


def _get_debug_file_number():
    dcm_debug = os.environ.get("DCM_DEBUG")
    if dcm_debug:
        try:
            return int(dcm_debug)
        except ValueError:
            return None
    return None


def _load_debug_data(command_name: str, file_number: int):
    results_dir = Path.cwd() / "results"

    debug_file = results_dir / f"{command_name}{file_number}.json"

    if not debug_file.exists():
        raise FileNotFoundError(f"Debug file not found: {debug_file}")

    with open(debug_file, "r") as f:
        data = json.load(f)

    if isinstance(data, list) and len(data) > 0:
        if command_name in ("test", "refresh", "analyze"):
            data = data[0]

    return data


def mock_dcm_response(command_name: str):
    # testing utility to test different reporting styles on mocked responses without touching the backend
    def decorator(func):
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any):
            file_number = _get_debug_file_number()
            if file_number is None:
                return func(*args, **kwargs)

            actual_command = "plan" if command_name == "deploy" else command_name
            try:
                data = _load_debug_data(actual_command, file_number)
            except Exception:
                return func(*args, **kwargs)

            if data is None:
                return func(*args, **kwargs)

            # Lazy imports to avoid circular dependency with reporters.
            from snowflake.cli._plugins.dcm.reporters import (
                PlanReporter,
                RefreshReporter,
                TestReporter,
            )

            cursor = FakeCursor(data)
            reporter_mapping = {
                "refresh": RefreshReporter,
                "test": TestReporter,
                "plan": PlanReporter,
            }

            reporter = reporter_mapping[command_name]()
            reporter.process(cursor)
            return EmptyResult()

        return wrapper

    return decorator
