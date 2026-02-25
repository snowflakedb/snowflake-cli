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
from snowflake.cli._plugins.dcm.reporters.plan import PlanReporter, PlanRow
from snowflake.cli.api.identifiers import FQN

from tests.dcm.test_reporters.utils import FakeCursor, capture_reporter_output


def plan_entity_change_factory(operation: str, domain: str, name: str):
    return {
        "type": operation,
        "object_id": {"domain": domain, "name": f'"{name}"', "fqn": f'"{name}"'},
    }


class TestPlanReporterTerse:
    def test_empty_changeset(self):
        data = {"version": 2, "metadata": {}, "changeset": []}
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
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
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
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
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
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
        reporter = PlanReporter(command_name="deploy")

        output = capture_reporter_output(reporter, FakeCursor(data))

        assert "Deployed 1 entity (1 created, 0 altered, 0 dropped)." in output

    def test_empty_cursor(self):
        output = capture_reporter_output(PlanReporter(), FakeCursor(None))

        assert "No data." in output

    def test_version_1_raises_error(self):
        data = {"version": 1, "changeset": []}

        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        assert "Only version 2+ plan responses are supported." in output

    def test_version_3_renders_in_compatibility_mode(self):
        data = {
            "version": 3,
            "metadata": {},
            "changeset": [
                {
                    "type": "CREATE",
                    "object_id": {
                        "domain": "TABLE",
                        "name": '"T"',
                        "fqn": '"T"',
                    },
                }
            ],
        }

        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        assert "CREATE" in output
        assert "Planned 1 entity (1 to create, 0 to alter, 0 to drop)." in output

    def _output_lines(self, changeset):
        data = {"version": 2, "metadata": {}, "changeset": changeset}
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
        return [line for line in output.strip().split("\n") if line.strip()]

    def test_orders_by_operation_type(self):
        changeset = [
            plan_entity_change_factory("DROP", "ROLE", "R1"),
            plan_entity_change_factory("CREATE", "TABLE", "T1"),
            plan_entity_change_factory("ALTER", "WAREHOUSE", "W1"),
        ]

        lines = self._output_lines(changeset)

        assert lines[0].startswith("CREATE")
        assert lines[1].startswith("ALTER")
        assert lines[2].startswith("DROP")

    def test_orders_by_domain_within_same_operation(self):
        changeset = [
            plan_entity_change_factory("CREATE", "WAREHOUSE", "W1"),
            plan_entity_change_factory("CREATE", "DATABASE", "D1"),
            plan_entity_change_factory("CREATE", "TABLE", "T1"),
        ]

        lines = self._output_lines(changeset)

        assert "DATABASE" in lines[0]
        assert "TABLE" in lines[1]
        assert "WAREHOUSE" in lines[2]

    def test_full_ordering(self, snapshot):
        changeset = [
            plan_entity_change_factory("ALTER", "WAREHOUSE", "W1"),
            plan_entity_change_factory("DROP", "TABLE", "T_OLD"),
            plan_entity_change_factory("CREATE", "TABLE", "T1"),
            plan_entity_change_factory("ALTER", "DATABASE", "D1"),
            plan_entity_change_factory("CREATE", "ROLE", "R1"),
            plan_entity_change_factory("DROP", "ROLE", "R_OLD"),
            plan_entity_change_factory("CREATE", "DATABASE", "D1"),
        ]

        data = {"version": 2, "metadata": {}, "changeset": changeset}
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
        lines = [line for line in output.strip().split("\n") if line.strip()]

        # CREATEs first, sorted by domain
        assert lines[0].startswith("CREATE") and "DATABASE" in lines[0]
        assert lines[1].startswith("CREATE") and "ROLE" in lines[1]
        assert lines[2].startswith("CREATE") and "TABLE" in lines[2]
        # ALTERs next, sorted by domain
        assert lines[3].startswith("ALTER") and "DATABASE" in lines[3]
        assert lines[4].startswith("ALTER") and "WAREHOUSE" in lines[4]
        # DROPs last, sorted by domain
        assert lines[5].startswith("DROP") and "ROLE" in lines[5]
        assert lines[6].startswith("DROP") and "TABLE" in lines[6]

        assert output == snapshot

    def test_unknown_operations_sort_last(self):
        changeset = [
            plan_entity_change_factory("WEIRD", "TABLE", "T1"),
            plan_entity_change_factory("CREATE", "TABLE", "T2"),
        ]

        lines = self._output_lines(changeset)

        assert lines[0].startswith("CREATE")
        assert lines[1].startswith("WEIRD")


class TestPlanRow:
    def test_from_dict_valid_entry(self):
        entry = {
            "type": "CREATE",
            "object_id": {
                "domain": "TABLE",
                "name": '"ORDERS"',
                "fqn": '"DB"."SCH"."ORDERS"',
                "database": '"DB"',
                "schema": '"SCH"',
            },
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "CREATE"
        assert row.domain == "TABLE"
        assert row.fqn is not None
        assert row.display_fqn() == "DB.SCH.ORDERS"

    def test_from_dict_fallback_on_missing_required_fields(self):
        entry = {
            "type": "ALTER",
            "object_id": "not_a_dict",
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "ALTER"
        assert row.domain == "UNKNOWN"
        assert row.fqn is None

    def test_from_dict_fallback_missing_type(self):
        entry = {"object_id": "bad"}

        row = PlanRow.from_dict(entry)

        assert row.operation == "UNKNOWN"

    def test_from_dict_fallback_with_parseable_fqn(self):
        entry = {
            "type": "DROP",
            "object_id": {
                "domain": "ROLE",
                "fqn": '"MY_ROLE"',
            },
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "DROP"
        assert row.domain == "ROLE"
        assert row.fqn is not None
        assert row.display_fqn() == "MY_ROLE"

    def test_from_dict_sanitizes_ansi_codes(self):
        entry = {
            "type": "CREATE",
            "object_id": {
                "domain": "TABLE\x1b[31m",
                "name": '"T"',
                "fqn": '"DB\x1b[0m"."SCH"."T"',
            },
        }

        row = PlanRow.from_dict(entry)

        assert "\x1b" not in row.domain
        assert row.fqn is not None
        assert "\x1b" not in row.display_fqn()

    def test_from_dict_fallback_sanitizes_ansi_codes(self):
        entry = {
            "type": "ALTER\x1b[31m",
            "object_id": {
                "domain": "TABLE\x1b[0m",
                "fqn": "unparseable\x1b[32m",
            },
        }

        row = PlanRow.from_dict(entry)

        assert "\x1b" not in row.operation
        assert "\x1b" not in row.domain

    def test_from_dict_fallback_defaults_when_entry_is_missing(self):
        entry = {}

        row = PlanRow.from_dict(entry)

        assert row.operation == "UNKNOWN"
        assert row.domain == "UNKNOWN"
        assert row.fqn is None

    def test_from_dict_fallback_unparseable_fqn_yields_none(self):
        entry = {
            "type": "CREATE",
            "object_id": {
                "domain": "TABLE",
                "fqn": "completely invalid fqn!!!",
            },
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "CREATE"
        assert row.domain == "TABLE"
        assert row.fqn is None

    def test_from_dict_fallback_no_fqn_key_yields_none(self):
        entry = {
            "type": "DROP",
            "object_id": {
                "domain": "ROLE",
            },
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "DROP"
        assert row.domain == "ROLE"
        assert row.fqn is None

    def test_from_dict_fallback_all_keys_wrong(self):
        entry = {
            "type_v2": "CREATE",
            "object_id": {
                "DOMAIN_v2": "TABLE",
                "FQN_v2": '"DB"."SCH"."ORDERS"',
            },
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "UNKNOWN"
        assert row.domain == "UNKNOWN"
        assert row.fqn is None

    def test_display_fqn_with_none(self):
        row = PlanRow(operation="CREATE", domain="TABLE", fqn=None)

        assert row.display_fqn() == "UNKNOWN"

    def test_display_fqn_name_only(self):
        fqn = FQN(database=None, schema=None, name='"MY_TABLE"')

        row = PlanRow(operation="CREATE", domain="TABLE", fqn=fqn)

        assert row.display_fqn() == "MY_TABLE"

    def test_display_fqn_fully_qualified(self):
        fqn = FQN(database='"DB"', schema='"SCH"', name='"TBL"')

        row = PlanRow(operation="CREATE", domain="TABLE", fqn=fqn)

        assert row.display_fqn() == "DB.SCH.TBL"

    def test_display_fqn_schema_and_name(self):
        fqn = FQN(database=None, schema='"SCH"', name='"TBL"')

        row = PlanRow(operation="CREATE", domain="TABLE", fqn=fqn)

        assert row.display_fqn() == "SCH.TBL"
