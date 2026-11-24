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

"""Lint for metrics span names.

Parses every ``span(...)`` / ``metrics.span(...)`` call under ``src/`` and
checks the name against the ``core.*`` / ``cmd.*`` grammars. Legacy names that
predate the grammar are allowlisted exactly. Three dynamic (non-literal) sites
are allowlisted by file + pattern.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import pytest
import snowflake.cli
from snowflake.cli._app.commands_registration.command_plugins_loader import (
    load_only_builtin_command_plugins,
)

TOKEN_RE = re.compile(r"^[a-z][a-z0-9_]*$")

CORE_AREAS = frozenset(
    {"startup", "config", "plugins", "project", "connection", "output"}
)

PHASES = frozenset(
    {
        "validate",
        "resolve",
        "build",
        "provision",
        "upload",
        "process",
        "wait",
        "fetch",
        "cleanup",
        "confirm",
    }
)

# Frozen names already in src/ that are neither core.* nor cmd.*.
LEGACY_SPAN_NAMES = frozenset(
    {
        "artifact_processors",
        "bundle",
        "get_snowsight_url_for_app",
        "post_deploy_hooks",
        "snowflake_app.bundle",
        "snowflake_app.bundle.clean_up_output",
        "snowflake_app.build",
        "snowflake_app.build.drop_stage",
        "snowflake_app.build.drop_workspace",
        "snowflake_app.build.ensure_artifact_repo",
        "snowflake_app.build.submit",
        "snowflake_app.build.wait",
        "snowflake_app.deploy",
        "snowflake_app.deploy.cng_cert_precheck",
        "snowflake_app.deploy.resolve_defaults",
        "snowflake_app.deploy_service",
        "snowflake_app.deploy_service.create",
        "snowflake_app.deploy_service.create_or_alter",
        "snowflake_app.deploy_service.upgrade",
        "snowflake_app.endpoint_provision",
        "snowflake_app.endpoint_provision.wait_for_endpoint",
        "snowflake_app.endpoint_provision.wait_for_upgrade",
        "snowflake_app.events",
        "snowflake_app.events.fetch_logs",
        "snowflake_app.open",
        "snowflake_app.open.resolve_endpoint",
        "snowflake_app.open.resolve_settings_url",
        "snowflake_app.open.wait_for_endpoint",
        "snowflake_app.setup",
        "snowflake_app.setup.check_existing_app_service",
        "snowflake_app.setup.fetch_capabilities",
        "snowflake_app.setup.get_personal_database",
        "snowflake_app.setup.resolve_defaults",
        "snowflake_app.setup.write_manifest",
        "snowflake_app.teardown",
        "snowflake_app.teardown.clear_workspace",
        "snowflake_app.teardown.confirm",
        "snowflake_app.teardown.drop_service",
        "snowflake_app.teardown.drop_stage",
        "snowflake_app.teardown.drop_workspace",
        "snowflake_app.teardown.resolve_defaults",
        "snowflake_app.upload",
        "snowflake_app.upload.prepare_stage",
        "snowflake_app.upload.prepare_workspace",
        "snowflake_app.upload.push_stage_files",
        "snowflake_app.upload.push_workspace_files",
        "snowflake_app.validate",
        "snowflake_app.validate.bundle",
        "snowflake_app.validate.check_database",
        "snowflake_app.validate.check_schema",
        "snowpark_processor",
        "sql.client_query",
        "sync_deploy_root_with_stage",
        "templates_processor",
        "update_app_object",
        "validate_setup_script",
    }
)

SITE_ACTION_ENTITY = "snowflake/cli/api/entities/common.py:action_entity"
SITE_EVENTS_FETCH_STREAM = "snowflake/cli/_plugins/apps/commands.py:events_fetch_stream"
SITE_PARENT_PREFIX = "snowflake/cli/_plugins/apps/manager.py:parent_prefix"

ALLOWED_DYNAMIC_SITES = frozenset(
    {
        SITE_ACTION_ENTITY,
        SITE_EVENTS_FETCH_STREAM,
        SITE_PARENT_PREFIX,
    }
)

DECORATOR_FORWARD_PATH = "snowflake/cli/api/cli_global_context.py"


@dataclass(frozen=True)
class SpanCall:
    rel_path: str
    lineno: int
    name: str | None
    site_id: str | None


def src_root() -> Path:
    return Path(snowflake.cli.__file__).resolve().parents[2]


def _is_span_call(call: ast.Call) -> bool:
    func = call.func
    if isinstance(func, ast.Name) and func.id == "span":
        return True
    return isinstance(func, ast.Attribute) and func.attr == "span"


def _span_name_arg(call: ast.Call) -> ast.AST | None:
    if call.args:
        return call.args[0]
    for keyword in call.keywords:
        if keyword.arg in {"name", "span_name"}:
            return keyword.value
    return None


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    constants: dict[str, str] = {}
    for node in tree.body:
        target: ast.AST | None = None
        value: ast.AST | None = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            target, value = node.target, node.value
        if (
            isinstance(target, ast.Name)
            and isinstance(value, ast.Constant)
            and isinstance(value.value, str)
        ):
            constants[target.id] = value.value
    return constants


class _LocalAssignFinder(ast.NodeVisitor):
    def __init__(self, name: str) -> None:
        self._name = name
        self.value: ast.AST | None = None

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        return

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == self._name:
                self.value = node.value
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802
        if (
            isinstance(node.target, ast.Name)
            and node.target.id == self._name
            and node.value is not None
        ):
            self.value = node.value
        self.generic_visit(node)


def _local_assign(
    func: ast.FunctionDef | ast.AsyncFunctionDef, name: str
) -> ast.AST | None:
    finder = _LocalAssignFinder(name)
    for stmt in func.body:
        finder.visit(stmt)
    return finder.value


def _joined_str_kind(node: ast.JoinedStr) -> str | None:
    values = node.values
    if not values:
        return None
    first, *rest = values
    if (
        isinstance(first, ast.Constant)
        and first.value == "snowflake_app.events.fetch_"
        and rest
    ):
        return "events_fetch_stream"
    if (
        isinstance(first, ast.Constant)
        and isinstance(first.value, str)
        and first.value.startswith("action.")
    ):
        return "action_entity"
    if (
        isinstance(first, ast.FormattedValue)
        and rest
        and isinstance(rest[0], ast.Constant)
        and rest[0].value in {".fetch_default_app_space", ".fetch_app_service_defaults"}
    ):
        return "parent_prefix"
    return None


def _is_decorator_forward(
    rel_path: str,
    arg: ast.AST,
    func_stack: list[ast.FunctionDef | ast.AsyncFunctionDef],
) -> bool:
    if rel_path != DECORATOR_FORWARD_PATH:
        return False
    if not isinstance(arg, ast.Name) or arg.id != "span_name":
        return False
    return any(func.name == "span" for func in func_stack)


class _SpanCallVisitor(ast.NodeVisitor):
    def __init__(self, rel_path: str, constants: dict[str, str]) -> None:
        self.rel_path = rel_path
        self.constants = constants
        self.func_stack: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
        self.calls: list[SpanCall] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self.func_stack.append(node)
        self.generic_visit(node)
        self.func_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self.func_stack.append(node)
        self.generic_visit(node)
        self.func_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if _is_span_call(node):
            classified = self._classify(node)
            if classified is not None:
                self.calls.append(classified)
        self.generic_visit(node)

    def _resolve(self, arg: ast.AST) -> ast.AST:
        if not isinstance(arg, ast.Name):
            return arg
        if arg.id in self.constants:
            return ast.Constant(value=self.constants[arg.id])
        if self.func_stack:
            assigned = _local_assign(self.func_stack[-1], arg.id)
            if assigned is not None:
                return assigned
        return arg

    def _classify(self, node: ast.Call) -> SpanCall | None:
        arg = _span_name_arg(node)
        lineno = node.lineno
        if arg is None:
            return SpanCall(self.rel_path, lineno, name=None, site_id=None)
        if _is_decorator_forward(self.rel_path, arg, self.func_stack):
            return None
        resolved = self._resolve(arg)
        if isinstance(resolved, ast.Constant) and isinstance(resolved.value, str):
            return SpanCall(self.rel_path, lineno, name=resolved.value, site_id=None)
        if isinstance(resolved, ast.JoinedStr):
            kind = _joined_str_kind(resolved)
            site_id = f"{self.rel_path}:{kind}" if kind else None
            return SpanCall(self.rel_path, lineno, name=None, site_id=site_id)
        return SpanCall(self.rel_path, lineno, name=None, site_id=None)


def collect_span_calls(root: Path) -> list[SpanCall]:
    """Walk ``root/**/*.py`` and return every metrics span call site."""
    calls: list[SpanCall] = []
    for path in sorted(root.rglob("*.py")):
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
        except (OSError, SyntaxError):
            continue
        rel_path = path.relative_to(root).as_posix()
        visitor = _SpanCallVisitor(rel_path, _module_string_constants(tree))
        visitor.visit(tree)
        calls.extend(visitor.calls)
    return calls


def _tokens_ok(parts: list[str]) -> bool:
    return all(TOKEN_RE.fullmatch(part) for part in parts)


def command_path_from_cmd_name(name: str) -> tuple[str, ...] | None:
    """Return the CLI path encoded in a ``cmd.*`` span name, or None if invalid."""
    parts = name.split(".")
    if parts[0] != "cmd" or len(parts) < 3 or not _tokens_ok(parts):
        return None
    if parts[-1] in PHASES:
        path_parts = parts[1:-1]
    elif len(parts) >= 4 and parts[-2] in PHASES:
        path_parts = parts[1:-2]
    else:
        return None
    if not path_parts:
        return None
    return tuple(part.replace("_", "-") for part in path_parts)


def span_name_errors(name: str, command_paths: frozenset[tuple[str, ...]]) -> list[str]:
    if not name:
        return ["span name must not be empty"]
    parts = name.split(".")
    if not _tokens_ok(parts):
        return [f"{name!r} tokens must match [a-z][a-z0-9_]*"]

    if name in LEGACY_SPAN_NAMES:
        return []

    if parts[0] == "core":
        if len(parts) < 3 or len(parts) > 4:
            return [f"{name!r} must be core.<area>.<step>[.<substep>]"]
        if parts[1] not in CORE_AREAS:
            return [f"{name!r} uses unknown core area {parts[1]!r}"]
        return []

    if parts[0] == "cmd":
        path = command_path_from_cmd_name(name)
        if path is None:
            return [
                f"{name!r} must be cmd.<group>[.<subgroup>].<command>.<phase>[.<step>] "
                "with a known phase word"
            ]
        if path not in command_paths:
            return [f"{name!r} does not match a builtin command path {path}"]
        return []

    return [f"{name!r} is not a core.*, cmd.*, or allowlisted legacy name"]


def _collect_click_paths(
    command, path: tuple[str, ...], out: set[tuple[str, ...]]
) -> None:
    if path:
        out.add(path)
    children = getattr(command, "commands", None)
    if not children:
        return
    for name, child in children.items():
        _collect_click_paths(child, path + (name,), out)


@cache
def builtin_command_paths() -> frozenset[tuple[str, ...]]:
    """Every group and leaf on the builtin plugin tree, including hidden commands."""
    paths: set[tuple[str, ...]] = set()
    for plugin in load_only_builtin_command_plugins():
        spec = plugin.command_spec
        root_segments = tuple(spec.full_command_path.path_segments)
        _collect_click_paths(spec.command, root_segments, paths)
    return frozenset(paths)


def _format_call(call: SpanCall) -> str:
    return f"{call.rel_path}:{call.lineno}"


def span_call_errors(
    calls: list[SpanCall], command_paths: frozenset[tuple[str, ...]]
) -> list[str]:
    errors: list[str] = []
    for call in calls:
        if call.name is not None:
            for err in span_name_errors(call.name, command_paths):
                errors.append(f"{_format_call(call)}: {err}")
            continue
        if call.site_id in ALLOWED_DYNAMIC_SITES:
            continue
        errors.append(
            f"{_format_call(call)}: dynamic span name is not one of the allowed sites"
        )
    return errors


# ── grammar unit tests ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "name",
    [
        "core.config.load",
        "core.startup.auto_upgrade",
        "core.plugins.load",
        "core.plugins.load_failed",
        "core.plugins.register_failed",
        "core.project.load",
        "core.project.validate",
        "core.connection.open",
        "core.connection.resolve_params",
        "core.output.render",
        "bundle",
        "sql.client_query",
        "snowflake_app.deploy",
        "sync_deploy_root_with_stage",
    ],
)
def test_valid_span_names_without_command_path(name: str) -> None:
    assert span_name_errors(name, frozenset()) == []


@pytest.mark.parametrize(
    "name",
    [
        "",
        "Core.config.load",
        "core.unknown.step",
        "core.config",
        "core.config.load.extra.too_deep",
        "cmd.streamlit.deploy.not_a_phase",
        "cmd.init",
        "not_a_namespace",
        "snowflake_app.new_thing",
        "action.app.deploy",
    ],
)
def test_invalid_span_names(name: str) -> None:
    assert span_name_errors(name, frozenset({("init",), ("streamlit", "deploy")}))


@pytest.mark.parametrize(
    ("name", "path"),
    [
        ("cmd.init.fetch", ("init",)),
        ("cmd.streamlit.deploy.provision.grants", ("streamlit", "deploy")),
        (
            "cmd.feature.online_service.create.wait",
            ("feature", "online-service", "create"),
        ),
        ("cmd.custom_image.validate.resolve", ("custom-image", "validate")),
        ("cmd.helpers.v1_to_v2.process", ("helpers", "v1-to-v2")),
        ("cmd.dbt.execute.process", ("dbt", "execute")),
        ("cmd.app.deploy.build", ("app", "deploy")),
        ("cmd.dcm.raw_analyze.fetch", ("dcm", "raw-analyze")),
        ("cmd.spcs.service.build_image.upload", ("spcs", "service", "build-image")),
        ("cmd.ws.deploy.process", ("ws", "deploy")),
        ("cmd.cortex.complete.process", ("cortex", "complete")),
    ],
)
def test_cmd_span_encodes_command_path(name: str, path: tuple[str, ...]) -> None:
    assert command_path_from_cmd_name(name) == path
    assert span_name_errors(name, frozenset({path})) == []


def test_cmd_span_unknown_command_path_fails() -> None:
    errors = span_name_errors(
        "cmd.no_such_group.list.process",
        frozenset({("init",)}),
    )
    assert errors
    assert "does not match a builtin command path" in errors[0]


def test_planted_bad_name_in_file(tmp_path: Path) -> None:
    planted = tmp_path / "planted.py"
    planted.write_text('span("Core.config.load")\n', encoding="utf-8")
    calls = collect_span_calls(tmp_path)
    errors = span_call_errors(calls, frozenset())
    assert errors
    assert "Core.config.load" in errors[0]


def test_planted_fstring_in_new_file_is_rejected(tmp_path: Path) -> None:
    planted = tmp_path / "planted.py"
    planted.write_text('metrics.span(f"cmd.foo.bar.{x}")\n', encoding="utf-8")
    calls = collect_span_calls(tmp_path)
    errors = span_call_errors(calls, frozenset())
    assert errors
    assert "dynamic span name is not one of the allowed sites" in errors[0]


def test_copied_action_fstring_outside_allowlisted_file_fails(tmp_path: Path) -> None:
    planted = tmp_path / "copied.py"
    planted.write_text(
        'span(f"action.{entity_name}.{attr_name_without_action_prefix}")\n',
        encoding="utf-8",
    )
    calls = collect_span_calls(tmp_path)
    errors = span_call_errors(calls, frozenset())
    assert errors
    assert "dynamic span name is not one of the allowed sites" in errors[0]


def test_same_module_constant_is_treated_as_literal(tmp_path: Path) -> None:
    planted = tmp_path / "const.py"
    planted.write_text(
        'NAME = "sql.client_query"\nmetrics.span(NAME)\n',
        encoding="utf-8",
    )
    calls = collect_span_calls(tmp_path)
    assert len(calls) == 1
    assert calls[0].name == "sql.client_query"
    assert span_call_errors(calls, frozenset()) == []


# ── src/ walk ────────────────────────────────────────────────────────


def test_production_span_names_are_allowed() -> None:
    calls = collect_span_calls(src_root())
    assert calls, "expected to find span() call sites under src/"
    errors = span_call_errors(calls, builtin_command_paths())
    assert errors == []


def test_dynamic_sites_are_exactly_the_allowlist() -> None:
    observed = {call.site_id for call in collect_span_calls(src_root()) if call.site_id}
    assert observed == ALLOWED_DYNAMIC_SITES


def test_cmd_names_map_onto_the_full_plugin_tree() -> None:
    paths = builtin_command_paths()
    # Plugins that iter_through_all_commands in tests/test_help_messages.py skips.
    assert ("helpers", "v1-to-v2") in paths
    assert ("cortex", "complete") in paths
    assert ("ws", "deploy") in paths
    # Hidden command.
    assert ("dcm", "raw-analyze") in paths
    assert span_name_errors("cmd.init.fetch", paths) == []
    assert span_name_errors("cmd.helpers.v1_to_v2.process", paths) == []
    assert span_name_errors("cmd.dcm.raw_analyze.process", paths) == []
    assert span_name_errors("cmd.no_such_group.list.process", paths)
