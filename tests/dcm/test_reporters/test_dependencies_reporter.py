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
from snowflake.cli._plugins.dcm.reporters.dependencies import (
    DependenciesReporter,
    _sanitize_mermaid_id,
    build_dependency_graph,
    render_dependencies_markdown,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

from tests.dcm.test_reporters.utils import FakeCursor, capture_reporter_output


def _id(name, domain, schema=None, database=None):
    out = {"name": name, "domain": domain}
    if schema is not None:
        out["schema"] = schema
    if database is not None:
        out["database"] = database
    return out


def _definition(id_dict, refined_domain, dependencies=None, properties=None):
    definition = {
        "id": id_dict,
        "refined_domain": refined_domain,
        "dependencies": [{"source_id": dep} for dep in (dependencies or [])],
        "issues": [],
    }
    if properties is not None:
        definition["properties"] = properties
    return definition


_FUNCTION_NAME = "CALCULATE_PROFIT_MARGIN(REVENUE NUMBER, COST NUMBER)"


def _sample_files():
    """A representative project mixing graphable + structural objects."""
    db = _id("DB", "DATABASE")
    raw = _id("RAW", "SCHEMA", database="DB")
    analytics = _id("ANALYTICS", "SCHEMA", database="DB")
    wh = _id("WH", "WAREHOUSE")

    customer = _id("CUSTOMER", "TABLE", schema="RAW", database="DB")
    menu = _id("MENU", "TABLE", schema="RAW", database="DB")
    enriched = _id("ENRICHED", "TABLE", schema="ANALYTICS", database="DB")
    func = _id(_FUNCTION_NAME, "FUNCTION", schema="ANALYTICS", database="DB")
    view = _id("V_DASH", "TABLE", schema="SERVE", database="DB")
    proc = _id("SP_DO(X VARCHAR)", "PROCEDURE", schema="RAW", database="DB")
    task1 = _id("T1", "TASK", schema="RAW", database="DB")
    task2 = _id("T2", "TASK", schema="RAW", database="DB")

    return [
        {
            "source_path": "sources/definitions/raw.sql",
            "definitions": [
                _definition(db, "database"),
                _definition(raw, "schema", [db]),
                _definition(customer, "table", [db, raw]),
                _definition(menu, "table", [db, raw]),
            ],
            "issues": [],
        },
        {
            "source_path": "sources/definitions/analytics.sql",
            "definitions": [
                _definition(analytics, "schema", [db]),
                _definition(
                    enriched,
                    "dynamic_table",
                    [db, analytics, raw, customer, menu, wh],
                    properties=[{"name": "TARGET_LAG", "value": "DOWNSTREAM"}],
                ),
                _definition(func, "function", [db, analytics]),
            ],
            "issues": [],
        },
        {
            "source_path": "sources/definitions/serve.sql",
            "definitions": [
                _definition(view, "view", [db, analytics, enriched]),
            ],
            "issues": [],
        },
        {
            "source_path": "sources/definitions/ingest.sql",
            "definitions": [
                _definition(proc, "procedure", [db, raw]),
                _definition(task1, "task", [db, raw, wh]),
                _definition(task2, "task", [db, raw, task1, wh]),
            ],
            "issues": [],
        },
    ]


class TestSanitizeMermaidId:
    def test_replaces_signature_punctuation_with_underscores(self):
        assert (
            _sanitize_mermaid_id(_FUNCTION_NAME)
            == "CALCULATE_PROFIT_MARGIN_REVENUE_NUMBER__COST_NUMBER_"
        )

    def test_plain_name_unchanged(self):
        assert _sanitize_mermaid_id("CUSTOMER") == "CUSTOMER"


class TestBuildDependencyGraph:
    def test_only_graphable_objects_become_nodes(self):
        graph = build_dependency_graph(_sample_files())

        # Structural objects (database/schema/warehouse) are excluded.
        assert graph.node_count == 8
        assert set(graph.nodes_by_category) == {
            "table",
            "dynamic_table",
            "view",
            "function",
            "procedure",
            "task",
        }
        assert [n.display_name for n in graph.nodes_by_category["table"]] == [
            "CUSTOMER",
            "MENU",
        ]

    def test_node_order_follows_file_then_definition_order(self):
        graph = build_dependency_graph(_sample_files())
        assert [n.display_name for n in graph.nodes_by_category["task"]] == ["T1", "T2"]

    def test_dynamic_table_captures_target_lag(self):
        graph = build_dependency_graph(_sample_files())
        (enriched,) = graph.nodes_by_category["dynamic_table"]
        assert enriched.lag == "DOWNSTREAM"
        assert enriched.qualifier == "DB.ANALYTICS"

    def test_function_display_name_strips_signature(self):
        graph = build_dependency_graph(_sample_files())
        (func,) = graph.nodes_by_category["function"]
        assert func.display_name == "CALCULATE_PROFIT_MARGIN"
        assert func.mermaid_id == "CALCULATE_PROFIT_MARGIN_REVENUE_NUMBER__COST_NUMBER_"

    def test_edges_only_between_graphable_nodes_in_target_order(self):
        graph = build_dependency_graph(_sample_files())
        assert graph.edges == [
            ("CUSTOMER", "ENRICHED"),
            ("MENU", "ENRICHED"),
            ("ENRICHED", "V_DASH"),
            ("T1", "T2"),
        ]

    def test_empty_response_yields_empty_graph(self):
        graph = build_dependency_graph([])
        assert graph.node_count == 0
        assert graph.edge_count == 0


class TestRenderDependenciesMarkdown:
    def test_contains_diagram_scaffold_and_nodes(self):
        graph = build_dependency_graph(_sample_files())
        md = render_dependencies_markdown(graph, "MY_PROJECT")

        assert "# DCM Project dependencies for MY_PROJECT" in md
        assert "```mermaid" in md
        assert "flowchart LR" in md
        assert "%% ── Tables ──" in md
        assert 'CUSTOMER("Table: CUSTOMER\\nDB.RAW")' in md
        assert (
            'ENRICHED("Dynamic Table [lag: DOWNSTREAM]\\nENRICHED\\nDB.ANALYTICS")'
            in md
        )
        assert "CUSTOMER --> ENRICHED" in md
        assert "classDef table" in md
        assert "class CUSTOMER,MENU table" in md

    def test_empty_graph_notes_no_objects(self):
        md = render_dependencies_markdown(build_dependency_graph([]), "EMPTY")
        assert "No graphable objects found" in md
        assert "```mermaid" in md


class TestDependenciesReporter:
    def test_process_writes_file_and_reports_path(self, tmp_path):
        output = SecurePath(tmp_path) / "dependencies.md"
        reporter = DependenciesReporter(
            project_identifier=FQN.from_string("MY_PROJECT"),
            output_path=output,
        )
        cursor = FakeCursor({"files": _sample_files()})

        out = capture_reporter_output(reporter, cursor)

        assert output.path.exists()
        content = output.path.read_text()
        assert "flowchart LR" in content
        assert "CUSTOMER --> ENRICHED" in content

        # Summary points at the written file and never fails the command.
        assert str(output.path.resolve()) in out
        assert "Dependency diagram" in out

    def test_process_creates_missing_output_directory(self, tmp_path):
        output = SecurePath(tmp_path) / "nested" / "deep" / "dependencies.md"
        reporter = DependenciesReporter(output_path=output)
        cursor = FakeCursor({"files": _sample_files()})

        capture_reporter_output(reporter, cursor)
        assert output.path.exists()

    def test_process_with_no_graphable_objects(self, tmp_path):
        output = SecurePath(tmp_path) / "dependencies.md"
        reporter = DependenciesReporter(output_path=output)
        cursor = FakeCursor({"files": []})

        out = capture_reporter_output(reporter, cursor)
        assert output.path.exists()
        assert "No objects were found to graph" in out
