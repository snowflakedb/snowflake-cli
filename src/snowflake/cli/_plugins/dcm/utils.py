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
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import EmptyResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.stage_path import StagePath

log = logging.getLogger(__name__)

OUTPUT_FOLDER = "out"


def clear_command_artifacts(command_name: str) -> None:
    """Clear previous artifacts for the given command from the out/ directory."""
    output_dir = SecurePath(OUTPUT_FOLDER)
    if not output_dir.exists():
        return

    json_file = output_dir / f"{command_name}.json"
    if json_file.exists():
        json_file.unlink()

    artifacts_dir = output_dir / command_name
    if artifacts_dir.exists():
        artifacts_dir.rmdir(recursive=True)

    log.info("Cleared previous artifacts for command '%s'.", command_name)


def save_command_response(command_name: str, raw_data: Dict[str, Any] | str) -> None:
    """Save raw JSON response to out/<command>.json."""
    output_dir = SecurePath(OUTPUT_FOLDER)
    output_dir.mkdir(exist_ok=True)
    json_file = output_dir / f"{command_name}.json"
    try:
        if isinstance(raw_data, str):
            json_file.write_text(raw_data)
        else:
            json_file.write_text(json.dumps(raw_data))
    except Exception as e:
        log.error("Failed to save command response: %s", e)
        return
    log.info(
        "Saved raw JSON response for command '%s' in %s.",
        command_name,
        json_file.resolve(),
    )
    cli_console.step(f"Artifacts saved to: {output_dir.path.resolve()}")


@contextmanager
def collect_output(
    project_identifier: FQN, command_name: str = "plan"
) -> Generator[str, None, None]:
    """
    Context manager for handling command output artifacts - creates temporary stage,
    downloads files to out/<command_name>/ folder after execution.

    Args:
        project_identifier: The DCM project identifier
        command_name: Name of the command, used for the output subdirectory

    Yields:
        str: The effective output path to use in the DCM command
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
    local_output_path = SecurePath(OUTPUT_FOLDER) / command_name

    try:
        yield effective_output_path.absolute_path()
    finally:
        log.info(
            "Downloading DCM %s artifacts from stage to local path (project_identifier=%s, stage_path=%s, local_path=%s).",
            command_name,
            project_identifier,
            effective_output_path.absolute_path(),
            local_output_path.resolve(),
        )
        local_output_path.mkdir(parents=True, exist_ok=True)
        stage_manager.get_recursive(
            stage_path=effective_output_path.absolute_path(),
            dest_path=local_output_path.path,
        )


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
