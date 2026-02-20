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
from functools import wraps
from pathlib import Path
from typing import Any

from snowflake.cli._plugins.dcm.reporters import (
    PlanReporter,
    RefreshReporter,
    TestReporter,
)
from snowflake.cli.api.output.types import EmptyResult


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
