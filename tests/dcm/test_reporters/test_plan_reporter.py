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
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.plan import (
    _MAX_VALUE_LEN,
    PlanDetail,
    PlanReporter,
    PlanRow,
    _truncate_inline,
)
from snowflake.cli.api.identifiers import FQN

from tests.dcm.test_reporters.utils import (
    CLI_CONSOLE_PATH,
    FakeCursor,
    capture_reporter_output,
)


def plan_entity_change_factory(operation: str, domain: str, name: str):
    return {
        "type": operation,
        "object_id": {"domain": domain, "name": f'"{name}"', "fqn": f'"{name}"'},
    }


class TestTruncateInline:
    def test_short_value_is_unchanged(self):
        assert _truncate_inline("SMALL") == "SMALL"

    def test_collapses_internal_whitespace_and_newlines(self):
        assert _truncate_inline("a\n  b\t c") == "a b c"

    def test_value_at_limit_is_not_truncated(self):
        value = "x" * _MAX_VALUE_LEN
        assert _truncate_inline(value) == value

    def test_long_value_is_truncated_with_ellipsis(self):
        result = _truncate_inline("y" * (_MAX_VALUE_LEN + 10))
        assert result == "y" * _MAX_VALUE_LEN + "…"
        assert len(result) == _MAX_VALUE_LEN + 1


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

    def test_signature_in_object_id(self):
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "CREATE",
                    "object_id": {
                        "domain": "FUNCTION",
                        "database": '"DB"',
                        "schema": '"SCH"',
                        "name": "'AREA_OF_CIRCLE'",
                        "fqn": '"DB"."SCH"."AREA_OF_CIRCLE"(FLOAT, FLOAT)',
                    },
                },
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
        assert "AREA_OF_CIRCLE(FLOAT, FLOAT)" in output

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

    def test_alter_with_removed_data_metric_function(self, snapshot):
        """Single nested collection wrapper around a single 'removed' leaf change.

        Real payload shape from a TABLE ALTER that drops one data metric
        function; the leaf's ``item_id.desc`` carries the human-readable
        identifier we want to surface.
        """
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "TABLE",
                        "name": '"ENRICHED_ORDER_DETAILS"',
                        "fqn": (
                            '"DCM_DEMO_1_DEV2"."ANALYTICS"."ENRICHED_ORDER_DETAILS"'
                        ),
                        "database": '"DCM_DEMO_1_DEV2"',
                        "schema": '"ANALYTICS"',
                    },
                    "changes": [
                        {
                            "kind": "collection",
                            "collection_name": "data_metric_functions",
                            "changes": [
                                {
                                    "kind": "removed",
                                    "item_id": {
                                        "columns": ["CUSTOMER_CITY"],
                                        "desc": (
                                            "SNOWHOUSE_IMPORT.CORE.NULL_COUNT$V1"
                                            "(CUSTOMER_CITY)"
                                        ),
                                        "metric_name": (
                                            "SNOWHOUSE_IMPORT.CORE.NULL_COUNT$V1"
                                            "(TABLE(VARCHAR))"
                                        ),
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
        assert output == snapshot
        assert "removed SNOWHOUSE_IMPORT.CORE.NULL_COUNT$V1(CUSTOMER_CITY)" in output

    def test_alter_with_modified_grants_nested(self, snapshot):
        """ROLE ALTER with mixed added/removed leaves plus a 'modified' parent.

        Validates that:
        - ``collection`` wrappers are unwrapped (no header line emitted).
        - 5 leaf changes are surfaced under the ALTER row.
        - The 'modified' entry's nested ``added OWNERSHIP`` renders deeper
          (header_plus_indent rule).
        """
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "ROLE",
                        "name": '"DCM_DEVELOPER"',
                        "fqn": '"DCM_DEVELOPER"',
                    },
                    "changes": [
                        {
                            "kind": "collection",
                            "collection_name": "grants",
                            "changes": [
                                {
                                    "kind": "removed",
                                    "item_id": {
                                        "desc": (
                                            "DATABASE_ROLE "
                                            "DCM_DEMO_1_DEV2.ADMIN_DEV2"
                                        ),
                                        "securable_object_domain": "DATABASE_ROLE",
                                        "securable_object_name": (
                                            "DCM_DEMO_1_DEV2.ADMIN_DEV2"
                                        ),
                                    },
                                },
                                {
                                    "kind": "added",
                                    "item_id": {
                                        "desc": (
                                            "DATABASE_ROLE "
                                            "DCM_DEMO_1_DEV3.ADMIN_DEV3"
                                        ),
                                        "securable_object_domain": "DATABASE_ROLE",
                                        "securable_object_name": (
                                            "DCM_DEMO_1_DEV3.ADMIN_DEV3"
                                        ),
                                    },
                                },
                                {
                                    "kind": "modified",
                                    "item_id": {
                                        "desc": ("ON SCHEMA DCM_DEMO_1_DEV2.TEST_TEAM"),
                                        "securable_object_domain": "SCHEMA",
                                        "securable_object_name": (
                                            "DCM_DEMO_1_DEV2.TEST_TEAM"
                                        ),
                                    },
                                    "changes": [
                                        {
                                            "kind": "collection",
                                            "collection_name": "privileges",
                                            "changes": [
                                                {
                                                    "kind": "added",
                                                    "item_id": {
                                                        "desc": "OWNERSHIP",
                                                        "privilege": "OWNERSHIP",
                                                    },
                                                }
                                            ],
                                        }
                                    ],
                                },
                                {
                                    "kind": "removed",
                                    "item_id": {
                                        "desc": "ROLE TEST_TEAM_OWNER_DEV2",
                                        "securable_object_domain": "ROLE",
                                        "securable_object_name": (
                                            "TEST_TEAM_OWNER_DEV2"
                                        ),
                                    },
                                },
                                {
                                    "kind": "added",
                                    "item_id": {
                                        "desc": "ROLE TEST_TEAM_OWNER_DEV3",
                                        "securable_object_domain": "ROLE",
                                        "securable_object_name": (
                                            "TEST_TEAM_OWNER_DEV3"
                                        ),
                                    },
                                },
                            ],
                        }
                    ],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        lines = [line for line in output.split("\n") if line.strip()]
        assert lines[0].startswith("ALTER")
        assert "ROLE" in lines[0] and "DCM_DEVELOPER" in lines[0]
        # All 5 leaves at depth 1; nested OWNERSHIP at depth 2.
        leaf_descs = [
            "DATABASE_ROLE DCM_DEMO_1_DEV2.ADMIN_DEV2",
            "DATABASE_ROLE DCM_DEMO_1_DEV3.ADMIN_DEV3",
            "ON SCHEMA DCM_DEMO_1_DEV2.TEST_TEAM",
            "ROLE TEST_TEAM_OWNER_DEV2",
            "ROLE TEST_TEAM_OWNER_DEV3",
        ]
        for desc in leaf_descs:
            assert desc in output
        # Nested 'added OWNERSHIP' should be indented one level deeper than
        # its parent 'modified ON SCHEMA …'. Compare the column where each
        # kind keyword starts — that survives the tree-prefix characters
        # without needing to strip them out.
        modified_line = next(line for line in lines if " modified " in line)
        owner_line = next(
            line for line in lines if " added " in line and "OWNERSHIP" in line
        )
        assert owner_line.index("added") > modified_line.index("modified")

        assert output == snapshot

    def test_alter_summary_counts_entities_not_details(self):
        """Sub-change leaves must not inflate the ALTER summary count."""
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "ROLE",
                        "name": '"R"',
                        "fqn": '"R"',
                    },
                    "changes": [
                        {
                            "kind": "collection",
                            "collection_name": "grants",
                            "changes": [
                                {
                                    "kind": "added",
                                    "item_id": {"desc": "ROLE A"},
                                },
                                {
                                    "kind": "removed",
                                    "item_id": {"desc": "ROLE B"},
                                },
                            ],
                        }
                    ],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        assert "Planned 1 entity (0 to create, 1 to alter, 0 to drop)." in output

    def test_alter_with_added_dmf_and_nested_expectation(self, snapshot):
        """Real-world ALTER adding a DMF with a nested expectation.

        Exercises three previously-unhandled shapes in one entry:
        - ``added`` with a dict ``item_id`` (DMF identifier).
        - ``added`` with a string ``item_id`` (expectation name).
        - ``set`` with ``attribute_name`` + scalar ``value``.
        """
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "TABLE",
                        "name": '"ENRICHED_ORDER_DETAILS"',
                        "fqn": (
                            '"DCM_DEMO_1_DEV3"."ANALYTICS"."ENRICHED_ORDER_DETAILS"'
                        ),
                        "database": '"DCM_DEMO_1_DEV3"',
                        "schema": '"ANALYTICS"',
                    },
                    "changes": [
                        {
                            "kind": "collection",
                            "collection_name": "data_metric_functions",
                            "changes": [
                                {
                                    "kind": "added",
                                    "item_id": {
                                        "columns": ["CUSTOMER_CITY"],
                                        "desc": (
                                            "SNOWFLAKE.CORE.NULL_COUNT$V1"
                                            "(CUSTOMER_CITY)"
                                        ),
                                        "metric_name": (
                                            "SNOWFLAKE.CORE.NULL_COUNT$V1"
                                            "(TABLE(VARCHAR))"
                                        ),
                                    },
                                    "changes": [
                                        {
                                            "kind": "collection",
                                            "collection_name": "expectations",
                                            "changes": [
                                                {
                                                    "kind": "added",
                                                    "item_id": "NO_MISSING_CITIES",
                                                    "changes": [
                                                        {
                                                            "kind": "set",
                                                            "attribute_name": (
                                                                "expression"
                                                            ),
                                                            "value": "value = 0",
                                                        }
                                                    ],
                                                }
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        assert "added SNOWFLAKE.CORE.NULL_COUNT$V1(CUSTOMER_CITY)" in output
        assert "added NO_MISSING_CITIES" in output
        assert "set expression = value = 0" in output
        assert output == snapshot

    def test_create_with_changes_does_not_render_details(self):
        """CREATE rows must stay terse even when the payload includes ``changes``.

        Real server payloads carry ``set``/``unset`` attribute dumps under
        CREATE and DROP entries; surfacing those would balloon the output
        with low-signal lines.
        """
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "CREATE",
                    "object_id": {
                        "domain": "TABLE",
                        "name": '"T"',
                        "fqn": '"DB"."SCH"."T"',
                    },
                    "changes": [
                        {
                            "kind": "set",
                            "attribute_name": "comment",
                            "value": "hello",
                        },
                        {
                            "kind": "set",
                            "attribute_name": "data_retention_time_in_days",
                            "value": 1,
                        },
                    ],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        lines = [line for line in output.split("\n") if line.strip()]
        assert lines[0].startswith("CREATE")
        # No sub-lines under CREATE.
        assert all(not line.startswith(" ") for line in lines)
        assert "comment" not in output
        assert "data_retention_time_in_days" not in output

    def test_drop_with_changes_does_not_render_details(self):
        """DROP rows must not surface their ``unset`` attribute dump."""
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "DROP",
                    "object_id": {
                        "domain": "SCHEMA",
                        "name": '"S"',
                        "fqn": '"DB"."S"',
                    },
                    "changes": [
                        {
                            "kind": "unset",
                            "attribute_name": "data_retention_time_in_days",
                            "prev_value": 1,
                        },
                        {
                            "kind": "unset",
                            "attribute_name": "log_level",
                            "prev_value": "WARN",
                        },
                    ],
                }
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        lines = [line for line in output.split("\n") if line.strip()]
        assert lines[0].startswith("DROP")
        assert all(not line.startswith(" ") for line in lines)
        assert "data_retention_time_in_days" not in output
        assert "log_level" not in output

    @pytest.mark.parametrize(
        "kind, expected_style",
        [
            ("added", styles.CREATE_STYLE),
            ("set", styles.NEUTRAL_STYLE),
            ("removed", styles.DROP_STYLE),
            ("unset", styles.DROP_STYLE),
            ("modified", styles.ALTER_STYLE),
            ("changed", styles.ALTER_STYLE),
            ("renamed", styles.ALTER_STYLE),
        ],
    )
    def test_detail_kind_keyword_is_colored_desc_is_default(self, kind, expected_style):
        """Only the operation keyword is colored; the description stays plain.

        On indented sub-lines under an ALTER row we want the eye to land on
        the verb (added/removed/modified/set/…) without coloring the whole
        line, which would otherwise drown out the entity names that follow.
        """
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {"domain": "TABLE", "fqn": '"T"'},
                    "changes": [
                        {
                            "kind": kind,
                            "item_id": {"desc": "SOME_DESC"},
                            "attribute_name": "an_attr",
                            "value": "v",
                        }
                    ],
                }
            ],
        }
        calls = []

        def record(text, style=""):
            calls.append((str(text), style))

        with mock.patch(CLI_CONSOLE_PATH, side_effect=record):
            PlanReporter().process(FakeCursor(data))

        kind_call = next(c for c in calls if c[0].strip() == kind)
        desc_call = next(c for c in calls if "SOME_DESC" in c[0])
        assert kind_call[1] == expected_style
        # The description part must render with the default style so the
        # entity name doesn't pick up the kind color.
        assert desc_call[1] == ""


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
        assert row.details == []

    def test_from_dict_extracts_alter_details(self):
        entry = {
            "type": "ALTER",
            "object_id": {
                "domain": "TABLE",
                "name": '"T"',
                "fqn": '"DB"."SCH"."T"',
            },
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "data_metric_functions",
                    "changes": [
                        {
                            "kind": "removed",
                            "item_id": {
                                "desc": "M.F$V1(C)",
                                "columns": ["C"],
                            },
                        }
                    ],
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        # Sole child at depth 1 → ``is_last_chain=(True,)``.
        assert row.details == [
            PlanDetail(kind="removed", desc="M.F$V1(C)", is_last_chain=(True,))
        ]

    def test_from_dict_recurses_into_modified_with_nested_changes(self):
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "ROLE", "fqn": '"R"'},
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "grants",
                    "changes": [
                        {
                            "kind": "modified",
                            "item_id": {"desc": "ON SCHEMA S"},
                            "changes": [
                                {
                                    "kind": "collection",
                                    "collection_name": "privileges",
                                    "changes": [
                                        {
                                            "kind": "added",
                                            "item_id": {"desc": "OWNERSHIP"},
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        # The ``modified`` entry is the sole leaf at depth 1; its nested
        # ``added`` is the sole leaf at depth 2 → both ``is_last`` chains end
        # in ``True``.
        assert row.details == [
            PlanDetail(kind="modified", desc="ON SCHEMA S", is_last_chain=(True,)),
            PlanDetail(kind="added", desc="OWNERSHIP", is_last_chain=(True, True)),
        ]

    def test_from_dict_details_sanitize_ansi_codes(self):
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "ROLE", "fqn": '"R"'},
            "changes": [
                {
                    "kind": "added\x1b[31m",
                    "item_id": {"desc": "ROLE \x1b[0mX"},
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert len(row.details) == 1
        detail = row.details[0]
        assert "\x1b" not in detail.kind
        assert "\x1b" not in detail.desc

    def test_from_dict_empty_changes_yields_no_details(self):
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [],
        }

        row = PlanRow.from_dict(entry)

        assert row.details == []

    def test_from_dict_handles_string_item_id(self):
        """``item_id`` may be a bare string (e.g. a column or expectation name)."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "expectations",
                    "changes": [
                        {"kind": "added", "item_id": "NO_MISSING_CITIES"},
                    ],
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.details == [
            PlanDetail(kind="added", desc="NO_MISSING_CITIES", is_last_chain=(True,)),
        ]

    def test_from_dict_handles_set_with_scalar_value(self):
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "WAREHOUSE", "fqn": '"WH"'},
            "changes": [
                {
                    "kind": "set",
                    "attribute_name": "warehouse_size",
                    "value": "LARGE",
                },
                {
                    "kind": "set",
                    "attribute_name": "auto_suspend",
                    "value": 60,
                },
                {
                    "kind": "set",
                    "attribute_name": "auto_resume",
                    "value": True,
                },
            ],
        }

        row = PlanRow.from_dict(entry)

        # Three siblings at depth 1: only the last is_last=True.
        assert row.details == [
            PlanDetail(
                kind="set",
                desc="WAREHOUSE_SIZE = LARGE",
                is_last_chain=(False,),
                attr="WAREHOUSE_SIZE",
            ),
            PlanDetail(
                kind="set",
                desc="AUTO_SUSPEND = 60",
                is_last_chain=(False,),
                attr="AUTO_SUSPEND",
            ),
            PlanDetail(
                kind="set",
                desc="AUTO_RESUME = true",
                is_last_chain=(True,),
                attr="AUTO_RESUME",
            ),
        ]

    def test_from_dict_set_with_complex_value_drops_rhs(self):
        """Complex (dict/list) values are too verbose to inline; show attr only."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {
                    "kind": "set",
                    "attribute_name": "columns",
                    "value": [{"name": "C", "datatype": "VARCHAR"}],
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.details == [
            PlanDetail(
                kind="set", desc="COLUMNS", is_last_chain=(True,), attr="COLUMNS"
            )
        ]

    def test_from_dict_handles_unset(self):
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "WAREHOUSE", "fqn": '"WH"'},
            "changes": [
                {
                    "kind": "unset",
                    "attribute_name": "comment",
                    "prev_value": "old",
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.details == [
            PlanDetail(
                kind="unset", desc="COMMENT", is_last_chain=(True,), attr="COMMENT"
            )
        ]

    def test_from_dict_modified_property_shows_prev_and_new(self):
        """A changed property reports both values; render ``prev → new``."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "WAREHOUSE", "fqn": '"WH"'},
            "changes": [
                {
                    "kind": "set",
                    "attribute_name": "warehouse_size",
                    "prev_value": "SMALL",
                    "value": "LARGE",
                },
                {
                    "kind": "modified",
                    "attribute_name": "auto_suspend",
                    "prev_value": 60,
                    "value": 120,
                },
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.details == [
            PlanDetail(
                kind="set",
                desc="WAREHOUSE_SIZE: SMALL → LARGE",
                is_last_chain=(False,),
                attr="WAREHOUSE_SIZE",
            ),
            PlanDetail(
                kind="modified",
                desc="AUTO_SUSPEND: 60 → 120",
                is_last_chain=(True,),
                attr="AUTO_SUSPEND",
            ),
        ]

    def test_from_dict_modified_property_truncates_long_multiline_values(self):
        """Multi-line / long SQL bodies are collapsed to one line and cut."""
        prev_body = "SELECT 1"
        new_body = "SELECT\n  a,\n  b,\n  c\nFROM " + ("x" * 80)
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "VIEW", "fqn": '"V"'},
            "changes": [
                {
                    "kind": "modified",
                    "attribute_name": "text",
                    "prev_value": prev_body,
                    "value": new_body,
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert len(row.details) == 1
        desc = row.details[0].desc
        prev_rendered, new_rendered = desc.removeprefix("TEXT: ").split(" → ")
        # Short previous value is shown verbatim; long new value is collapsed
        # to a single line (no newlines) and truncated with an ellipsis.
        assert prev_rendered == "SELECT 1"
        assert "\n" not in new_rendered
        assert new_rendered.endswith("…")
        assert len(new_rendered) == _MAX_VALUE_LEN + 1

    def test_from_dict_create_skips_details(self):
        """Only ALTER rows render sub-changes; CREATE stays terse."""
        entry = {
            "type": "CREATE",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {
                    "kind": "set",
                    "attribute_name": "comment",
                    "value": "x",
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "CREATE"
        assert row.details == []

    def test_from_dict_drop_skips_details(self):
        entry = {
            "type": "DROP",
            "object_id": {"domain": "SCHEMA", "fqn": '"S"'},
            "changes": [
                {
                    "kind": "unset",
                    "attribute_name": "comment",
                    "prev_value": "x",
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "DROP"
        assert row.details == []

    def test_from_dict_fallback_still_extracts_details(self):
        entry = {
            "type": "ALTER",
            "object_id": "not_a_dict",
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "grants",
                    "changes": [
                        {"kind": "added", "item_id": {"desc": "ROLE A"}},
                        {"kind": "removed", "item_id": {"desc": "ROLE B"}},
                    ],
                }
            ],
        }

        row = PlanRow.from_dict(entry)

        assert row.operation == "ALTER"
        assert row.domain == "UNKNOWN"
        # Two siblings under the collection wrapper → is_last_chain (False,) then (True,).
        assert row.details == [
            PlanDetail(kind="added", desc="ROLE A", is_last_chain=(False,)),
            PlanDetail(kind="removed", desc="ROLE B", is_last_chain=(True,)),
        ]

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

    @pytest.mark.parametrize(
        "fqn,expected_display",
        [
            (FQN(database=None, schema=None, name='"TBL"'), "TBL"),
            (FQN(database=None, schema='"SCH"', name='"TBL"'), "SCH.TBL"),
            (FQN(database='"DB"', schema='"SCH"', name='"TBL"'), "DB.SCH.TBL"),
            (
                FQN(database='"DB"', schema='"SCH"', name='"FUNC"', signature="()"),
                "DB.SCH.FUNC()",
            ),
            (
                FQN(
                    database='"DB"', schema='"SCH"', name='"FUNC"', signature="(FLOAT)"
                ),
                "DB.SCH.FUNC(FLOAT)",
            ),
            (
                FQN(
                    database='"DB"',
                    schema='"SCH"',
                    name='"FUNC"',
                    signature="(FLOAT, FLOAT)",
                ),
                "DB.SCH.FUNC(FLOAT, FLOAT)",
            ),
            (None, "UNKNOWN"),
        ],
    )
    def test_display_fqn(self, fqn, expected_display):
        row = PlanRow(operation="CREATE", domain="TABLE", fqn=fqn)

        assert row.display_fqn() == expected_display
