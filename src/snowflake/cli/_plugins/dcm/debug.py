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

"""
DCM Debug utilities for local development without backend calls.

Set DCM_DEBUG environment variable to use mock data from results/ directory:
  DCM_DEBUG=1 snow dcm plan my_project    # Uses results/plan1.json
  DCM_DEBUG=2 snow dcm plan my_project    # Uses results/plan2.json
  DCM_DEBUG=1 snow dcm test my_project    # Uses results/test1.json
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


class FakeCursor:
    """Fake cursor that returns data from JSON files for debugging."""

    def __init__(self, data: Any):
        self._data = data
        self._fetched = False

    def fetchone(self):
        """Return the data as if fetched from database."""
        if self._fetched:
            return None
        self._fetched = True
        # Return as tuple with JSON string (mimics real cursor behavior)
        return (json.dumps(self._data),)


def get_debug_file_number() -> Optional[int]:
    """
    Get the debug file number from DCM_DEBUG environment variable.

    Returns:
        Integer file number if DCM_DEBUG is set, None otherwise
    """
    dcm_debug = os.environ.get("DCM_DEBUG")
    if dcm_debug:
        try:
            return int(dcm_debug)
        except ValueError:
            return None
    return None


def load_debug_data(command_name: str, file_number: int) -> Optional[Dict[str, Any]]:
    """
    Load debug data from results directory.

    Args:
        command_name: Command name (plan, test, refresh, analyze)
        file_number: File number to load (1 for plan1.json, 2 for plan2.json, etc.)

    Returns:
        Parsed JSON data or None if file doesn't exist
    """
    # Look for results directory in workspace root
    results_dir = Path.cwd() / "results"
    if not results_dir.exists():
        # Try relative to this file
        results_dir = (
            Path(__file__).parent.parent.parent.parent.parent.parent / "results"
        )

    debug_file = results_dir / f"{command_name}{file_number}.json"

    if not debug_file.exists():
        print(f"Debug file not found: {debug_file}")
        return None

    with open(debug_file, "r") as f:
        data = json.load(f)

    # Some result files wrap data in an array - unwrap if so
    if isinstance(data, list) and len(data) > 0:
        # For test, refresh, analyze - result is wrapped in array with single object
        if command_name in ("test", "refresh", "analyze"):
            data = data[0]

    return data


def get_debug_cursor(command_name: str) -> Optional[FakeCursor]:
    """
    Get a fake cursor with debug data if DCM_DEBUG is set.

    Args:
        command_name: Command name (plan, test, refresh, analyze, deploy)

    Returns:
        FakeCursor with debug data if DCM_DEBUG is set, None otherwise
    """
    file_number = get_debug_file_number()
    if file_number is None:
        return None

    # For deploy, use plan data (as mentioned in requirements)
    actual_command = "plan" if command_name == "deploy" else command_name

    data = load_debug_data(actual_command, file_number)
    if data is None:
        return None

    print(
        f"[DCM_DEBUG] Using {actual_command}{file_number}.json for {command_name} command"
    )

    return FakeCursor(data)
