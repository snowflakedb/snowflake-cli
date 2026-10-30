# Copyright (c) 2026 Snowflake Inc.
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

"""Phase 1 contract tests: the `snow bundle` command surface on its own.

No handler exists yet, so these check the spec and the handler ABC only.
"""

import inspect

from snowflake.cli._plugins.bundle.interface import BUNDLE_SPEC, BundleHandler
from snowflake.cli.api.plugins.command.interface import REQUIRED, ParamKind
from snowflake.cli.api.plugins.command.testing import assert_interface_well_formed


def test_interface_is_well_formed():
    assert_interface_well_formed(BUNDLE_SPEC)


def test_group_attaches_at_root():
    assert BUNDLE_SPEC.name == "bundle"
    assert BUNDLE_SPEC.parent_path == ()
    assert BUNDLE_SPEC.subgroups == ()


def test_declares_expected_commands():
    assert sorted(c.name for c in BUNDLE_SPEC.commands) == [
        "alter",
        "cancel",
        "create",
        "delete",
        "execute",
        "history",
        "list",
        "status",
    ]


def test_every_command_requires_a_connection():
    # All eight commands issue SQL, so none of them is usable offline.
    assert all(c.requires_connection for c in BUNDLE_SPEC.commands)


def test_handler_abc_declares_one_method_per_command():
    declared = {c.handler_method for c in BUNDLE_SPEC.commands}
    assert declared == set(BundleHandler.__abstractmethods__)


def test_handler_signatures_match_declared_params():
    by_method = {c.handler_method: c for c in BUNDLE_SPEC.commands}
    for method_name, cmd in by_method.items():
        params = list(inspect.signature(getattr(BundleHandler, method_name)).parameters)
        assert params == ["self"] + [p.name for p in cmd.params], method_name


def test_execute_forwards_unknown_arguments_to_the_bundle():
    execute = next(c for c in BUNDLE_SPEC.commands if c.name == "execute")
    assert execute.context_settings == {
        "allow_extra_args": True,
        "ignore_unknown_options": True,
    }
    # The leftover tokens land in a variadic argument, declared last so the
    # bundle identifier is consumed first.
    assert execute.params[-1].name == "arguments"
    assert execute.params[-1].kind is ParamKind.ARGUMENT


def test_required_params_are_the_ones_without_a_default():
    required = {
        (c.name, p.name)
        for c in BUNDLE_SPEC.commands
        for p in c.params
        if p.default is REQUIRED
    }
    assert required == {
        ("create", "identifier"),
        ("create", "source"),
        ("delete", "identifier"),
        ("alter", "identifier"),
        ("execute", "identifier"),
        ("execute", "entrypoint"),
        ("status", "query_id"),
        ("cancel", "query_id"),
    }
