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
from pathlib import Path

from snowflake.cli._plugins.dcm.reporters.plan import PlanReporter

from tests.dcm.test_reporters.utils import FakeCursor, capture_reporter_output

_PLANS_DIR = Path(__file__).resolve().parents[3] / "plans" / "plan_refactor"


def _load_plan_data(filename: str):
    with open(_PLANS_DIR / filename) as f:
        data = json.load(f)
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        if isinstance(first, dict) and "result" in first:
            return first["result"]
    return data


class TestPlanReporterTerse:
    def test_empty_changeset(self):
        data = {"version": 2, "metadata": {}, "changeset": []}
        output = capture_reporter_output(PlanReporter(verbose=False), FakeCursor(data))
        assert "No changes detected." in output

    def test_single_create(self, snapshot):
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "CREATE",
                    "object_id": {
                        "domain": "TABLE",
                        "name": '"ORDERS"',
                        "fqn": '"DB"."SCH"."ORDERS"',
                        "database": '"DB"',
                        "schema": '"SCH"',
                    },
                    "changes": [],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(verbose=False), FakeCursor(data))
        assert output == snapshot

    def test_mixed_operations(self, snapshot):
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "CREATE",
                    "object_id": {
                        "domain": "DATABASE",
                        "name": '"MY_DB"',
                        "fqn": '"MY_DB"',
                    },
                    "changes": [],
                },
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "WAREHOUSE",
                        "name": '"MY_WH"',
                        "fqn": '"MY_WH"',
                    },
                    "changes": [],
                },
                {
                    "type": "DROP",
                    "object_id": {
                        "domain": "ROLE",
                        "name": '"OLD_ROLE"',
                        "fqn": '"OLD_ROLE"',
                    },
                    "changes": [],
                },
            ],
        }
        output = capture_reporter_output(PlanReporter(verbose=False), FakeCursor(data))
        assert output == snapshot

    def test_plan5_terse(self, snapshot):
        data = _load_plan_data("plan5.json")
        output = capture_reporter_output(PlanReporter(verbose=False), FakeCursor(data))
        assert output == snapshot

    def test_deploy_summary_prefix(self):
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "CREATE",
                    "object_id": {
                        "domain": "TABLE",
                        "name": '"ORDERS"',
                        "fqn": '"DB"."SCH"."ORDERS"',
                        "database": '"DB"',
                        "schema": '"SCH"',
                    },
                    "changes": [],
                }
            ],
        }
        reporter = PlanReporter(verbose=False)
        reporter.command_name = "deploy"
        output = capture_reporter_output(reporter, FakeCursor(data))
        assert "Deployed 1 entities (1 created)." in output
