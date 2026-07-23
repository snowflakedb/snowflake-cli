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
from io import StringIO
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm import styles
from snowflake.cli._plugins.dcm.reporters.plan import (
    _DIFF_CONTEXT,
    _MAX_VALUE_LEN,
    PlanEntityChange,
    PlanReporter,
    PlanRow,
    _render_nodes,
    _truncate_inline,
    _truncate_value_pair,
)
from snowflake.cli.api.exceptions import CliError
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


def plan_row(entry):
    """Validate a raw changeset entry into a model and build its PlanRow —
    mirroring the production path (extract_data validates, parse_data builds)."""
    return PlanRow.from_entity(PlanEntityChange.model_validate(entry))


def render_detail_lines(entry):
    """Render the sub-change tree for a single changeset entry's details —
    the indented lines with tree connectors, exactly as the user sees them
    under the entity header.
    """
    row = plan_row(entry)
    output = StringIO()

    def mock_print(text, style=""):
        output.write(text.plain if hasattr(text, "plain") else str(text))

    with mock.patch(CLI_CONSOLE_PATH, side_effect=mock_print):
        _render_nodes(row.details)
    return [line for line in output.getvalue().split("\n") if line]


class TestTruncateInline:
    def test_short_value_is_unchanged(self):
        value = "SMALL"

        result = _truncate_inline(value)

        assert result == "SMALL"

    def test_collapses_internal_whitespace_and_newlines(self):
        value = "a\n  b\t c"

        result = _truncate_inline(value)

        assert result == "a b c"

    def test_value_at_limit_is_not_truncated(self):
        value = "x" * _MAX_VALUE_LEN

        result = _truncate_inline(value)

        assert result == value

    def test_long_value_is_truncated_with_ellipsis(self):
        value = "y" * (_MAX_VALUE_LEN + 10)

        result = _truncate_inline(value)

        assert result == "y" * _MAX_VALUE_LEN + "…"
        assert len(result) == _MAX_VALUE_LEN + 1

    def test_truncation_boundary_on_space_drops_trailing_space(self):
        value = "a" * (_MAX_VALUE_LEN - 1) + " tail"

        result = _truncate_inline(value)

        assert result == "a" * (_MAX_VALUE_LEN - 1) + "…"
        assert " …" not in result


class TestTruncateValuePair:
    def test_short_values_shown_verbatim(self):
        prev, new = "SMALL", "LARGE"

        result = _truncate_value_pair(prev, new)

        assert result == ("SMALL", "LARGE")

    def test_collapses_whitespace_on_both(self):
        prev, new = "a\n b", "a\tc"

        result = _truncate_value_pair(prev, new)

        assert result == ("a b", "a c")

    def test_difference_near_start_uses_head_truncation(self):
        prev = "AAAA" + "x" * _MAX_VALUE_LEN
        new = "BBBB" + "x" * _MAX_VALUE_LEN

        result = _truncate_value_pair(prev, new)

        assert result == (
            "AAAA" + "x" * (_MAX_VALUE_LEN - 4) + "…",
            "BBBB" + "x" * (_MAX_VALUE_LEN - 4) + "…",
        )

    def test_trailing_difference_is_windowed_and_visible(self):
        shared = "SELECT a, b, c FROM my_very_long_table_name WHERE flag = TRUE "
        prev = shared + "GROUP BY a"
        new = shared + "GROUP BY b"

        result = _truncate_value_pair(prev, new)

        assert result == ("…UE GROUP BY a", "…UE GROUP BY b")

    def test_windowed_values_share_aligned_context(self):
        shared = "x" * (_MAX_VALUE_LEN * 2)
        prev = shared + "ONE"
        new = shared + "TWO"

        result = _truncate_value_pair(prev, new)

        assert result == (
            "…" + "x" * _DIFF_CONTEXT + "ONE",
            "…" + "x" * _DIFF_CONTEXT + "TWO",
        )

    def test_change_in_middle_is_windowed_with_both_ellipses(self):
        lead, tail = "L" * 60, "T" * 60
        prev = lead + "1" + tail
        new = lead + "2" + tail

        result = _truncate_value_pair(prev, new)

        window_tail = _MAX_VALUE_LEN - _DIFF_CONTEXT - 1
        assert result == (
            "…" + "L" * _DIFF_CONTEXT + "1" + "T" * window_tail + "…",
            "…" + "L" * _DIFF_CONTEXT + "2" + "T" * window_tail + "…",
        )

    def test_only_one_value_exceeds_limit(self):
        prev = "SHORT"
        new = "z" * (_MAX_VALUE_LEN + 30)

        result = _truncate_value_pair(prev, new)

        assert result == ("SHORT", "z" * _MAX_VALUE_LEN + "…")

    def test_window_boundary_on_space_absorbed_by_leading_ellipsis(self):
        context_run = "w" * (_DIFF_CONTEXT - 1)
        shared = "A" * _MAX_VALUE_LEN + " " + context_run
        prev = shared + "1"
        new = shared + "2"

        result = _truncate_value_pair(prev, new)

        assert result == ("…" + context_run + "1", "…" + context_run + "2")

    def test_appended_clause_is_windowed_from_shared_tail(self):
        prev = "SELECT id, name, price FROM orders WHERE status = 'OPEN'"
        new = prev + " GROUP BY region"

        result = _truncate_value_pair(prev, new)

        assert result == ("…tus = 'OPEN'", "…tus = 'OPEN' GROUP BY region")


class TestPlanReporter:
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

    def test_unparseable_fqn_renders_without_crashing(self):
        """A valid-string but non-parseable FQN must degrade to UNKNOWN, not crash.

        The entry passes ``PlanResponse`` validation (``fqn`` is a string) and
        reaches the reporter as a model; FQN parsing fails and must be handled
        locally rather than dropping into a dict-only fallback.
        """
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "TABLE",
                        "fqn": "completely invalid fqn!!!",
                    },
                    "changes": [
                        {"kind": "set", "attribute_name": "comment", "value": "x"}
                    ],
                }
            ],
        }

        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        assert "ALTER" in output
        assert "UNKNOWN" in output
        assert "set COMMENT: x" in output

    @pytest.mark.parametrize(
        "entry",
        [
            pytest.param(
                {"type": "ALTER", "object_id": "not_a_dict"}, id="non-dict-object_id"
            ),
            pytest.param(
                {"type": "ALTER", "object_id": {"domain": "TABLE"}}, id="missing-fqn"
            ),
            pytest.param(
                {"type": "ALTER", "object_id": {"fqn": '"T"'}}, id="missing-domain"
            ),
            pytest.param({"type": "ALTER"}, id="missing-object_id"),
        ],
    )
    def test_extract_data_rejects_malformed_entry(self, entry):
        """A structurally malformed entry fails validation wholesale — best-effort
        resilience lives here in extract_data, not in per-entry salvage."""
        result_json = {"version": 2, "metadata": {}, "changeset": [entry]}
        with pytest.raises(CliError):
            PlanReporter().extract_data(result_json)

    def _output_lines(self, changeset):
        data = {"version": 2, "metadata": {}, "changeset": changeset}
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))
        return [line for line in output.strip().split("\n") if line.strip()]

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


class TestPlanRow:
    @pytest.mark.parametrize("operation", ["CREATE", "DROP"])
    def test_non_alter_stays_terse(self, operation):
        """Only ALTER entities expand sub-changes; CREATE/DROP stay terse even
        when the payload carries ``changes``."""
        entry = {
            "type": operation,
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [{"kind": "set", "attribute_name": "comment", "value": "x"}],
        }

        assert plan_row(entry).details == []

    def test_from_entity_valid_entry(self):
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

        row = plan_row(entry)

        assert row.operation == "CREATE"
        assert row.domain == "TABLE"
        assert row.fqn is not None
        assert row.display_fqn() == "DB.SCH.ORDERS"
        assert row.details == []

    def test_from_entity_empty_changes_yields_no_details(self):
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [],
        }

        row = plan_row(entry)

        assert row.details == []

    def test_from_entity_parses_fqn(self):
        entry = {
            "type": "DROP",
            "object_id": {"domain": "ROLE", "fqn": '"MY_ROLE"'},
        }

        row = plan_row(entry)

        assert row.operation == "DROP"
        assert row.domain == "ROLE"
        assert row.display_fqn() == "MY_ROLE"

    def test_from_entity_missing_type_yields_unknown_operation(self):
        """A missing ``type`` validates and degrades to the UNKNOWN operation."""
        entry = {"object_id": {"domain": "TABLE", "fqn": '"T"'}}

        row = plan_row(entry)

        assert row.operation == "UNKNOWN"

    def test_from_entity_sanitizes_ansi_codes(self):
        entry = {
            "type": "CREATE\x1b[1m",
            "object_id": {
                "domain": "TABLE\x1b[31m",
                "name": '"T"',
                "fqn": '"DB\x1b[0m"."SCH"."T"',
            },
        }

        row = plan_row(entry)

        assert row.operation == "CREATE"
        assert "\x1b" not in row.operation
        assert "\x1b" not in row.domain
        assert row.fqn is not None
        assert "\x1b" not in row.display_fqn()

    def test_from_entity_unparseable_fqn_yields_none(self):
        """A valid string that isn't a parseable FQN degrades to None, not a crash."""
        entry = {
            "type": "CREATE",
            "object_id": {"domain": "TABLE", "fqn": "completely invalid fqn!!!"},
        }

        row = plan_row(entry)

        assert row.operation == "CREATE"
        assert row.domain == "TABLE"
        assert row.fqn is None
        assert row.display_fqn() == "UNKNOWN"

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


class TestChangeTreeRendering:
    """Change-tree rendering — both isolated detail lines (via
    ``render_detail_lines``) and full reporter output (snapshots + inter-tree
    spacing)."""

    def test_collection_renders_named_node(self):
        """A named collection renders as a parent node; items nest one deeper."""
        entry = {
            "type": "ALTER",
            "object_id": {
                "domain": "TABLE",
                "name": '"CUSTOMERS"',
                "fqn": '"DB"."SCH"."CUSTOMERS"',
            },
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "columns",
                    "changes": [
                        {"kind": "added", "item_id": "EMAIL"},
                        {"kind": "removed", "item_id": "NICKNAME"},
                    ],
                }
            ],
        }

        assert render_detail_lines(entry) == [
            "└─ columns",
            "   ├─ added EMAIL",
            "   └─ removed NICKNAME",
        ]

    def test_nested_renders_named_node(self):
        """A nested group renders as an attribute-named node with children below."""
        entry = {
            "type": "ALTER",
            "object_id": {
                "domain": "TABLE",
                "name": '"T"',
                "fqn": '"DB"."SCH"."T"',
            },
            "changes": [
                {
                    "kind": "nested",
                    "attribute_name": "clustering",
                    "changes": [
                        {"kind": "set", "attribute_name": "automatic", "value": True},
                        {
                            "kind": "changed",
                            "attribute_name": "expression",
                            "prev_value": "(A)",
                            "value": "(A, B)",
                        },
                    ],
                }
            ],
        }

        assert render_detail_lines(entry) == [
            "└─ clustering",
            "   ├─ set AUTOMATIC: true",
            "   └─ changed EXPRESSION: (A) → (A, B)",
        ]

    def test_recurses_into_nested_changes(self):
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

        assert render_detail_lines(entry) == [
            "└─ grants",
            "   └─ modified ON SCHEMA S",
            "      └─ privileges",
            "         └─ added OWNERSHIP",
        ]

    def test_removed_collection_item_hides_nested_details(self):
        """A removed item renders as a single line; its nested changes are dropped."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "ROLE", "fqn": '"R"'},
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "grants",
                    "changes": [
                        {
                            "kind": "removed",
                            "item_id": {"desc": "ON SCHEMA S"},
                            "changes": [
                                {
                                    "kind": "collection",
                                    "collection_name": "privileges",
                                    "changes": [
                                        {
                                            "kind": "removed",
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

        assert render_detail_lines(entry) == [
            "└─ grants",
            "   └─ removed ON SCHEMA S",
        ]

    def test_all_node_types_render_in_sort_order(self):
        """Every node type at one level sorts deterministically regardless of
        server order: leaf sub-changes grouped by category (create → alter →
        drop, item kind before property kind within a category), then named
        containers, then unrecognized (generic) kinds last."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {"kind": "frobnicated", "item_id": {"desc": "MYSTERY"}},
                {"kind": "unset", "attribute_name": "u_attr", "prev_value": "x"},
                {
                    "kind": "collection",
                    "collection_name": "cols",
                    "changes": [{"kind": "added", "item_id": "NESTED"}],
                },
                {"kind": "added", "item_id": "COL_A"},
                {
                    "kind": "changed",
                    "attribute_name": "c_attr",
                    "prev_value": "old",
                    "value": "new",
                },
                {"kind": "set", "attribute_name": "s_attr", "value": "1"},
                {"kind": "removed", "item_id": "REM_ITEM"},
                {"kind": "modified", "item_id": {"desc": "MOD_ITEM"}},
            ],
        }

        assert render_detail_lines(entry) == [
            "├─ added COL_A",
            "├─ set S_ATTR: 1",
            "├─ modified MOD_ITEM",
            "├─ changed C_ATTR: old → new",
            "├─ removed REM_ITEM",
            "├─ unset U_ATTR",
            "├─ cols",
            "│  └─ added NESTED",
            "└─ frobnicated MYSTERY",
        ]

    def test_non_last_container_renders_pipe_continuation(self):
        """A non-last container's descendants get the │ continuation prefix."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {
                    "kind": "collection",
                    "collection_name": "columns",
                    "changes": [{"kind": "added", "item_id": "A"}],
                },
                {
                    "kind": "collection",
                    "collection_name": "constraints",
                    "changes": [{"kind": "added", "item_id": "B"}],
                },
            ],
        }
        assert render_detail_lines(entry) == [
            "├─ columns",
            "│  └─ added A",
            "└─ constraints",
            "   └─ added B",
        ]

    def test_set_scalar_values(self):
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

        # Three siblings; only the last uses the └─ connector.
        assert render_detail_lines(entry) == [
            "├─ set WAREHOUSE_SIZE: LARGE",
            "├─ set AUTO_SUSPEND: 60",
            "└─ set AUTO_RESUME: true",
        ]

    def test_set_complex_value_is_serialized(self):
        """Complex (dict/list) values are serialized to JSON and shown, capped by
        the shared width budget."""
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

        assert render_detail_lines(entry) == [
            '└─ set COLUMNS: [{"name": "C", "datatype": "VARCHAR"}]'
        ]

    def test_unset(self):
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

        assert render_detail_lines(entry) == ["└─ unset COMMENT"]

    def test_changed_shows_prev_and_new(self):
        """A changed property reports both values; render ``prev → new``."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "WAREHOUSE", "fqn": '"WH"'},
            "changes": [
                {
                    "kind": "changed",
                    "attribute_name": "warehouse_size",
                    "prev_value": "SMALL",
                    "value": "LARGE",
                },
                {
                    "kind": "changed",
                    "attribute_name": "auto_suspend",
                    "prev_value": 60,
                    "value": 120,
                },
            ],
        }

        assert render_detail_lines(entry) == [
            "├─ changed WAREHOUSE_SIZE: SMALL → LARGE",
            "└─ changed AUTO_SUSPEND: 60 → 120",
        ]

    def test_changed_dict_value_renders_json_transition(self):
        """A ``changed`` on a dict-valued attribute serializes both sides to JSON
        and renders them as a ``prev → new`` transition, truncated to the shared
        width budget."""
        entry = {
            "type": "ALTER",
            "object_id": {
                "domain": "TASK",
                "name": '"MY_TASK"',
                "fqn": '"MY_DB"."ANALYTICS"."MY_TASK_1"',
                "database": '"MY_DB"',
                "schema": '"ANALYTICS"',
            },
            "changes": [
                {
                    "kind": "changed",
                    "attribute_name": "config",
                    "value": {
                        "path": "/prod_directory/abc",
                        "environment": "production",
                    },
                    "prev_value": {
                        "path": "/prod_directory/xyz",
                        "environment": "production",
                    },
                }
            ],
        }

        assert render_detail_lines(entry) == [
            '└─ changed CONFIG: {"path": "/prod_directory/xyz", "environment": "pr… '
            '→ {"path": "/prod_directory/abc", "environment": "pr…'
        ]

    def test_sanitizes_ansi_in_detail(self):
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

        lines = render_detail_lines(entry)
        assert lines == ["└─ added ROLE X"]
        assert "\x1b" not in lines[0]

    def test_generic_node_unknown_kind_with_item_id(self):
        """An unrecognized kind renders via the generic fallback using item_id."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [{"kind": "frobnicated", "item_id": {"desc": "SOME_ITEM"}}],
        }
        assert render_detail_lines(entry) == ["└─ frobnicated SOME_ITEM"]

    def test_generic_node_unknown_kind_with_attribute(self):
        """An unrecognized kind with attribute fields renders like a property."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {
                    "kind": "frobnicated",
                    "attribute_name": "size",
                    "prev_value": "S",
                    "value": "L",
                }
            ],
        }
        assert render_detail_lines(entry) == ["└─ frobnicated SIZE: S → L"]

    def test_generic_node_unusable_item_id_falls_back_to_attribute(self):
        """A present-but-labelless item_id must not block attribute rendering."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [
                {
                    "kind": "frobnicated",
                    "item_id": {"schema": "S"},
                    "attribute_name": "size",
                    "value": "L",
                }
            ],
        }
        assert render_detail_lines(entry) == ["└─ frobnicated SIZE: L"]

    def test_empty_unknown_change_is_dropped(self):
        """A change with no kind and no content produces no line."""
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "TABLE", "fqn": '"T"'},
            "changes": [{"kind": ""}],
        }
        assert render_detail_lines(entry) == []

    def test_changed_truncates_long_multiline_values(self):
        """Multi-line / long SQL bodies are collapsed to one line and cut."""
        prev_body = "SELECT 1"
        new_body = "SELECT\n  a,\n  b,\n  c\nFROM " + ("x" * 80)
        entry = {
            "type": "ALTER",
            "object_id": {"domain": "VIEW", "fqn": '"V"'},
            "changes": [
                {
                    "kind": "changed",
                    "attribute_name": "text",
                    "prev_value": prev_body,
                    "value": new_body,
                }
            ],
        }

        lines = render_detail_lines(entry)
        assert len(lines) == 1
        prev_rendered, new_rendered = (
            lines[0].removeprefix("└─ changed TEXT: ").split(" → ")
        )
        # Short previous value is shown verbatim; long new value is collapsed
        # to a single line (no newlines) and truncated with an ellipsis.
        assert prev_rendered == "SELECT 1"
        assert "\n" not in new_rendered
        assert new_rendered.endswith("…")
        assert len(new_rendered) == _MAX_VALUE_LEN + 1

    def _record_process(self, data):
        calls = []

        def record(text, style=""):
            calls.append((str(text), style))

        with mock.patch(CLI_CONSOLE_PATH, side_effect=record):
            PlanReporter().process(FakeCursor(data))
        return calls

    @pytest.mark.parametrize(
        "kind, expected_style",
        [
            ("added", styles.CREATE_STYLE),
            ("removed", styles.DROP_STYLE),
            ("modified", styles.ALTER_STYLE),
        ],
    )
    def test_item_keyword_is_colored_desc_is_default(self, kind, expected_style):
        """A collection item's keyword is colored; its description stays plain,
        so the entity name that follows doesn't pick up the kind color."""
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {"domain": "TABLE", "fqn": '"T"'},
                    "changes": [
                        {
                            "kind": "collection",
                            "collection_name": "columns",
                            "changes": [
                                {"kind": kind, "item_id": {"desc": "SOME_DESC"}}
                            ],
                        }
                    ],
                }
            ],
        }
        calls = self._record_process(data)

        kind_call = next(c for c in calls if c[0].strip() == kind)
        desc_call = next(c for c in calls if "SOME_DESC" in c[0])
        assert kind_call[1] == expected_style
        assert desc_call[1] == ""

    @pytest.mark.parametrize(
        "kind, expected_style",
        [
            ("set", styles.CREATE_STYLE),
            ("unset", styles.DROP_STYLE),
            ("changed", styles.ALTER_STYLE),
        ],
    )
    def test_property_keyword_is_colored(self, kind, expected_style):
        """A property change's keyword is colored per its kind category."""
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
                            "attribute_name": "an_attr",
                            "value": "v",
                            "prev_value": "p",
                        }
                    ],
                }
            ],
        }
        calls = self._record_process(data)

        kind_call = next(c for c in calls if c[0].strip() == kind)
        assert kind_call[1] == expected_style

    def test_blank_line_separates_consecutive_trees(self):
        """A rendered ALTER tree is followed by a blank line before the next
        entity, but not before the trailing summary (which adds its own)."""
        data = {
            "version": 2,
            "metadata": {},
            "changeset": [
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "WAREHOUSE",
                        "name": '"WH1"',
                        "fqn": '"WH1"',
                    },
                    "changes": [
                        {
                            "kind": "set",
                            "attribute_name": "warehouse_size",
                            "value": "SMALL",
                        }
                    ],
                },
                {
                    "type": "ALTER",
                    "object_id": {
                        "domain": "WAREHOUSE",
                        "name": '"WH2"',
                        "fqn": '"WH2"',
                    },
                    "changes": [
                        {
                            "kind": "set",
                            "attribute_name": "warehouse_size",
                            "value": "LARGE",
                        }
                    ],
                },
            ],
        }
        output = capture_reporter_output(PlanReporter(), FakeCursor(data))

        # First tree is separated from the next entity by a blank line.
        assert "set WAREHOUSE_SIZE: SMALL\n\nALTER" in output
        # The last tree is not double-spaced: only the summary's own blank line
        # sits between it and the summary text.
        assert "set WAREHOUSE_SIZE: LARGE\n\nPlanned" in output

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
        - the ``grants`` collection renders as a named parent node.
        - 5 leaf changes are surfaced nested under it.
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
        assert "set EXPRESSION: value = 0" in output
        assert output == snapshot
