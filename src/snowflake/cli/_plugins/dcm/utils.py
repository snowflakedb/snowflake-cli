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
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Generator

from rich.style import Style
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import EmptyResult
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.stage_path import StagePath

log = logging.getLogger(__name__)

OUTPUT_FOLDER = "out"
# Subfolder (under out/) into which the backend nests the rendered project
# definitions. Every ``--save-output`` command surfaces it at the same
# canonical ``out/rendered/`` location, regardless of which command produced it.
RENDERED_FOLDER = "rendered"
# Provenance file written alongside the rendered definitions so users can tell
# when (and by which command) the current out/rendered/ snapshot was produced.
# It is wiped and rewritten with the folder on every ``--save-output`` run.
RENDERED_METADATA_FILE = "rendered_metadata.json"


def clear_command_artifacts(command_name: str) -> None:
    """Clear previous artifacts for the given command from the out/ directory.

    Removes the command's ``<command_name>_result.json`` (and ``.md``) files.
    The shared ``out/rendered/`` folder is owned by :func:`collect_output`, which
    wipes and rewrites it on each ``--save-output`` run.
    """
    output_dir = SecurePath(OUTPUT_FOLDER)
    if not output_dir.exists():
        return

    json_file = output_dir / f"{command_name}_result.json"
    if json_file.exists():
        json_file.unlink()

    # Some commands (e.g. ``dependencies``) also emit a Markdown artifact.
    markdown_file = output_dir / f"{command_name}.md"
    if markdown_file.exists():
        markdown_file.unlink()

    log.info("Cleared previous artifacts for command '%s'.", command_name)


def _write_rendered_metadata(
    rendered_folder: SecurePath, project_identifier: FQN, command_name: str
) -> None:
    """Record when (and by which command) the rendered snapshot was produced."""
    metadata = {
        "rendered_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "project": project_identifier.identifier,
        "command": command_name,
    }
    try:
        (rendered_folder / RENDERED_METADATA_FILE).write_text(json.dumps(metadata))
    except Exception as e:  # never fail the command just because the marker failed
        log.warning("Failed to write rendered definitions metadata: %s", e)


def _read_rendered_timestamp(rendered_folder: SecurePath) -> str | None:
    """Return the ``rendered_at`` timestamp from the marker file, if available."""
    marker = rendered_folder / RENDERED_METADATA_FILE
    if not marker.exists():
        return None
    try:
        return json.loads(marker.read_text(file_size_limit_mb=1)).get("rendered_at")
    except Exception:
        return None


def announce_rendered_definitions() -> None:
    """Print a label and a gray, clickable line to the rendered definitions folder.

    No-op when the folder doesn't exist (e.g. the backend produced no rendered
    output). Used by the ``compile`` and ``dependencies`` commands after a
    ``--save-output`` run to point the user at the downloaded definitions.
    """
    folder = SecurePath(OUTPUT_FOLDER) / RENDERED_FOLDER
    if not folder.exists():
        return
    abs_path = folder.path.resolve()
    cli_console.styled_message("\n")
    cli_console.styled_message("Rendered definitions saved to: ")
    cli_console.styled_message("\n")
    cli_console.styled_message(
        f"{abs_path}",
        style=Style(color="grey50", link=f"file://{abs_path}"),
    )
    rendered_at = _read_rendered_timestamp(folder)
    if rendered_at:
        cli_console.styled_message("\n")
        cli_console.styled_message(
            f"Rendered at: {rendered_at}", style=Style(color="grey50")
        )
    cli_console.styled_message("\n")


def save_command_response(
    command_name: str,
    raw_data: Dict[str, Any] | str,
    announce: bool = True,
) -> None:
    """Save raw JSON response to out/<command_name>_result.json.

    When ``announce`` is False the "Artifacts saved to" step is suppressed (the
    file is still written) so callers can present their own output layout.
    """
    output_dir = SecurePath(OUTPUT_FOLDER)
    output_dir.mkdir(exist_ok=True)
    json_file = output_dir / f"{command_name}_result.json"
    try:
        # Force UTF-8 so non-ASCII payloads (e.g. a backend error message
        # captured as the fallback result) never hit the platform default
        # encoding (cp1252 on Windows) and raise UnicodeEncodeError.
        if isinstance(raw_data, str):
            json_file.write_text(raw_data, encoding="utf-8")
        else:
            json_file.write_text(json.dumps(raw_data), encoding="utf-8")
    except Exception as e:
        log.error("Failed to save command response: %s", e)
        return
    log.info(
        "Saved raw JSON response for command '%s' in %s.",
        command_name,
        json_file.resolve(),
    )
    if announce:
        cli_console.step(f"Artifacts saved to: {output_dir.path.resolve()}")


def _save_error_result(command_name: str, error: Exception) -> None:
    """Fallback: persist a failed command's error as out/<command>_result.json.

    Only writes when the backend didn't already download its own result file —
    a successful run's richer file is never clobbered.
    """
    result_file = SecurePath(OUTPUT_FOLDER) / f"{command_name}_result.json"
    if result_file.exists():
        return
    message = str(error)
    # DCM backend errors frequently arrive as a JSON body; preserve it as JSON
    # when so, otherwise store the raw message the CLI displayed.
    payload: Any
    try:
        payload = json.loads(message)
    except (ValueError, TypeError):
        payload = message
    try:
        save_command_response(command_name, payload, announce=False)
    except Exception as e:  # never mask the original failure
        log.warning("Failed to write error fallback result file: %s", e)


@contextmanager
def save_error_result_on_failure(
    command_name: str, save_output: bool
) -> Generator[None, None, None]:
    """Write out/<command>_result.json from the raised error when a run fails.

    When a command fails before the backend produced its own
    ``<command>_result.json`` (e.g. a ``plan`` that errors during compilation),
    the raised error is the only diagnostic the user gets — and today it lives
    only in the terminal. With ``--save-output`` set, this captures that error
    into ``out/<command>_result.json`` (the file the run would otherwise be
    missing) before re-raising, so the CLI still surfaces the error as usual.
    """
    try:
        yield
    except Exception as e:
        if save_output:
            _save_error_result(command_name, e)
        raise


@contextmanager
def collect_output(
    project_identifier: FQN, command_name: str
) -> Generator[str, None, None]:
    """
    Context manager for handling command output artifacts - creates temporary stage,
    downloads files directly into the out/ folder after execution.

    The backend nests the rendered project definitions under a ``rendered/``
    subfolder of the ``OUTPUT_PATH`` it's given and writes the ``*_result.json``
    files as siblings of it. Downloading straight into ``out/`` therefore lands
    the definitions at ``out/rendered/`` (no intermediate level) and the result
    files directly under ``out/``.

    Args:
        project_identifier: The DCM project identifier
        command_name: Name of the command, used for logging

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
    output_dir = SecurePath(OUTPUT_FOLDER)

    try:
        yield effective_output_path.absolute_path()
    finally:
        log.info(
            "Downloading DCM %s artifacts from stage to local path (project_identifier=%s, stage_path=%s, local_path=%s).",
            command_name,
            project_identifier,
            effective_output_path.absolute_path(),
            output_dir.resolve(),
        )
        # Delete-then-write: wipe any previous run's rendered definitions so
        # stale files never linger, no matter which command produced them.
        (output_dir / RENDERED_FOLDER).rmdir(recursive=True, missing_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        stage_manager.get_recursive(
            stage_path=effective_output_path.absolute_path(),
            dest_path=output_dir.path,
        )
        # Stamp the freshly downloaded snapshot so users can tell when it was
        # produced. Only when the backend actually rendered definitions.
        rendered_dir = output_dir / RENDERED_FOLDER
        if rendered_dir.exists():
            _write_rendered_metadata(rendered_dir, project_identifier, command_name)


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
        if command_name in (
            "test",
            "refresh",
            "compile",
            "dependencies",
        ):
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
