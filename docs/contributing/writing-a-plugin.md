<!--
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# Writing a Plugin (Interface-First)

Snowflake CLI supports an **interface-first** plugin pattern that separates a
command's *surface* — its name, parameters, and help text — from its
*implementation*. The surface is declared as plain data in `interface.py` and
can be reviewed on its own; the business logic follows in `handler.py`.

This is an alternative to the classic plugin layout in
[adding-commands.md](adding-commands.md), where commands are defined directly
with `@app.command()` in `commands.py`. Reach for interface-first when the
command surface should be signed off **before** any implementation is written —
most useful for plugins contributed by teams outside the CLI core, where the two
phases are reviewed by different people.

The design principles and the sign-off requirement from
[adding-commands.md](adding-commands.md#design-sign-off-before-writing-code)
still apply: get the command surface approved before writing code.

---

## Two-phase contribution workflow

```
Phase 1 PR:  interface.py                 -->  review command surface  -->  merge
Phase 2 PR:  handler.py + plugin_spec.py  -->  review implementation   -->  merge
```

Phase 1 is reviewable on its own: reviewers evaluate the complete command
surface — names, parameters, help text, connection requirements — as plain data,
without any implementation to wade through.

---

## Quickstart with the plugin template

The fastest way to start is the cookiecutter template under
[`plugin-template/`](../../plugin-template):

```bash
pip install cookiecutter
cookiecutter plugin-template/
```

You will be prompted for:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `plugin_name` | Package name (used in `pip install`) | `snow-analytics` |
| `plugin_module` | Python module name (auto-derived) | `snow_analytics` |
| `cli_command_name` | CLI command name under `snow` | `analytics` |
| `command_type` | `group` (multiple subcommands) or `single` | `group` |
| `requires_connection` | Whether commands need a Snowflake connection | `true` |

This generates a complete, installable project with `interface.py`,
`handler.py`, `plugin_spec.py`, and a passing contract test — the structure
described below.

---

## Phase 1: define the interface

`interface.py` holds two things:

1. **A command spec** — frozen dataclasses describing every command, its
   parameters, help text, and connection requirements.
2. **A handler ABC** — an abstract class with one method per command.

Example for a group with two commands (`snow analytics run` and
`snow analytics report`):

```python
from __future__ import annotations

from abc import abstractmethod

from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.plugins.command.interface import (
    REQUIRED,
    CommandDef,
    CommandGroupSpec,
    CommandHandler,
    ParamDef,
    ParamKind,
)

ANALYTICS_SPEC = CommandGroupSpec(
    name="analytics",
    help="Run analytics queries.",
    parent_path=(),  # () = attach at root: `snow analytics`
    commands=(
        CommandDef(
            name="run",
            help="Run an analytics query.",
            handler_method="run",
            requires_connection=True,
            params=(
                ParamDef(
                    name="query_name",
                    type=str,
                    kind=ParamKind.ARGUMENT,
                    help="Name of the query to run.",
                ),
                ParamDef(
                    name="limit",
                    type=int,
                    kind=ParamKind.OPTION,
                    cli_names=("--limit", "-l"),
                    help="Maximum rows to return.",
                    default=100,  # any value other than REQUIRED makes it optional
                ),
            ),
            output_type="QueryResult",
        ),
        CommandDef(
            name="report",
            help="Generate a summary report.",
            handler_method="report",
            requires_connection=True,
            output_type="MessageResult",
        ),
    ),
)


class AnalyticsHandler(CommandHandler):
    # @abstractmethod is optional here. CommandHandler is a marker base with no
    # abstract methods of its own; the framework enforces the contract via
    # validate_interface_handler at build time, not through ABC. The decorators
    # are kept for IDE/type-checker support and to document the contract.
    @abstractmethod
    def run(self, query_name: str, limit: int) -> CommandResult: ...

    @abstractmethod
    def report(self) -> CommandResult: ...
```

The spec variable name (`ANALYTICS_SPEC` here) is yours to choose, but it must
match the name imported in `plugin_spec.py` (see
[Phase 2](#phase-2-implement-the-handler)). The cookiecutter template names it
`PLUGIN_SPEC` — if you start from the template, either keep that name or rename
it in both `interface.py` and `plugin_spec.py`.

**Key spec types** (from `snowflake.cli.api.plugins.command.interface`):

| Dataclass | Purpose |
|-----------|---------|
| `CommandGroupSpec` | A command group with subcommands (e.g. `snow notebook`) |
| `SingleCommandSpec` | A single command with no subcommands |
| `CommandDef` | One command: name, help, params, connection requirements |
| `ParamDef` | One parameter: name, type, argument vs option, CLI names |
| `CommandHandler` | The ABC your handler subclasses |

The example above uses `CommandGroupSpec`. A plugin that contributes a **single**
command uses `SingleCommandSpec` instead (import it in place of
`CommandGroupSpec`) — note the singular `command=` field, not `commands=(...)`:

```python
ANALYTICS_SPEC = SingleCommandSpec(
    parent_path=(),  # () = attach at root: `snow analytics`
    command=CommandDef(
        name="analytics",
        help="Run an analytics query.",
        handler_method="run",
        requires_connection=True,
    ),
)
```

**`ParamDef` fields:**

| Field | Description |
|-------|-------------|
| `name` | Python parameter name (passed as a kwarg to the handler method) |
| `type` | Python type (`str`, `int`, `bool`, `FQN`, `Path`, ...) |
| `kind` | `ParamKind.ARGUMENT` or `ParamKind.OPTION` |
| `help` | Help text shown in `--help` |
| `cli_names` | Explicit CLI names, e.g. `("--limit", "-l")`. Empty = auto-derived from `name` |
| `default` | Default value. `REQUIRED` (the default) marks the parameter required; any other value (e.g. `None`, `100`) makes it optional |
| `is_flag` | `True` for boolean flags such as `--replace` (give flags an explicit `default`, e.g. `False`) |
| `click_type` | Custom Click `ParamType` for non-standard types (e.g. `IdentifierType()` for `FQN`) |

Submit the interface for review on its own. Reviewers can evaluate the whole
command surface without seeing any implementation.

### CODEOWNERS for Phase 2

For a **built-in** plugin (one that lives in this repository), add yourself and a
colleague as `CODEOWNERS` for your plugin directory in the Phase 1 PR, so the
Phase 2 implementation only needs review from your team:

```
# analytics plugin
/src/snowflake/cli/_plugins/analytics/   @your-handle @colleague-handle
```

For a plugin in its own repository this is unnecessary — you already own the repo.

---

## Phase 2: implement the handler

After the interface is approved, implement each handler method in `handler.py`.
Follow [conventions.md](conventions.md) — in particular, **never interpolate
user input into SQL**. Escape string values with `to_string_literal`, wrap
object identifiers with `FQN.sql_identifier`, and prefer the helpers over
f-strings:

```python
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.sql_execution import SqlExecutionMixin

from .interface import AnalyticsHandler


class AnalyticsHandlerImpl(AnalyticsHandler, SqlExecutionMixin):
    def run(self, query_name: str, limit: int) -> CommandResult:
        # query_name is user input -> escape it with to_string_literal.
        # limit is a validated int, so it is safe to format directly.
        # For object identifiers use FQN.sql_identifier (see conventions.md).
        cursor = self.execute_query(
            f"SELECT * FROM analytics_queries "
            f"WHERE name = {to_string_literal(query_name)} "
            f"LIMIT {limit}"
        )
        return QueryResult(cursor)

    def report(self) -> CommandResult:
        return MessageResult("Report generated.")
```

The handler mixes in `SqlExecutionMixin` to get `self.execute_query(...)` —
`SqlExecutionMixin` is meant to be inherited, not instantiated. Built-in plugins
more commonly keep SQL in a dedicated `AnalyticsManager(SqlExecutionMixin)` and
have the handler delegate to it; either layout works, as long as the mixin is
inherited rather than constructed inline.

Then wire the spec and handler together in `plugin_spec.py`:

```python
from snowflake.cli.api.plugins.command import plugin_hook_impl
from snowflake.cli.api.plugins.command.bridge import build_command_spec

from .handler import AnalyticsHandlerImpl
from .interface import ANALYTICS_SPEC


@plugin_hook_impl
def command_spec():
    return build_command_spec(ANALYTICS_SPEC, AnalyticsHandlerImpl())
```

`build_command_spec` validates that the handler implements every method declared
in the spec, then produces a standard `CommandSpec`. The rest of the CLI is
unaware of the interface/handler split.

**Built-in plugins:** wiring `plugin_spec.py` is enough for a plugin distributed
as its own package, but a plugin that lives in this repository must also be
registered in `builtin_plugins.py` (add an import and a dict entry), as described
in [Registering the plugin](adding-commands.md#registering-the-plugin). Without
that step the plugin is valid Python but is never loaded by the CLI.

---

## Testing your plugin

Use the helpers from `snowflake.cli.api.plugins.command.testing` to validate the
interface and the handler contract without a Snowflake connection:

```python
from snowflake.cli.api.plugins.command.testing import (
    assert_builds_valid_spec,
    assert_handler_satisfies,
    assert_interface_well_formed,
)

from .handler import AnalyticsHandlerImpl
from .interface import ANALYTICS_SPEC


def test_interface_is_well_formed():
    assert_interface_well_formed(ANALYTICS_SPEC)


def test_handler_satisfies_interface():
    assert_handler_satisfies(ANALYTICS_SPEC, AnalyticsHandlerImpl())


def test_builds_valid_command_spec():
    assert_builds_valid_spec(ANALYTICS_SPEC, AnalyticsHandlerImpl())
```

| Helper | Checks |
|--------|--------|
| `assert_interface_well_formed` | The spec tree is complete and consistent (names, help text, unique handler methods) |
| `assert_handler_satisfies` | The handler implements every method the spec declares |
| `assert_builds_valid_spec` | The spec + handler build into a valid Click command tree |

See [testing.md](testing.md) for the wider test setup (the `runner` fixture,
snapshots, and feature-flag helpers) once your commands need integration tests.

---

## Installing and enabling your plugin

```bash
# Install in development mode
pip install -e /path/to/your/plugin

# Enable it (use the plugin module name)
snow plugin enable <plugin_module>

# Verify it is registered
snow plugin list
```

`snow plugin enable` only finds a plugin whose `pyproject.toml` declares the CLI
plugin entry point — this is what makes the installed package discoverable:

```toml
[project.entry-points."snowflake.cli.plugin.command"]
<plugin_module> = "<package_namespace>.<plugin_module>.plugin_spec"
```

The cookiecutter template generates this automatically. Without it, `snow plugin
enable` reports the plugin as not installed even after a successful `pip install`.

---

## Using decorators

Some commands need extra decorators such as `with_project_definition`. Name them
in `CommandDef.decorators`; the bridge applies them when building the command:

```python
CommandDef(
    name="deploy",
    help="Deploy from a project definition.",
    handler_method="deploy",
    requires_connection=True,
    decorators=("with_project_definition",),
)
```

The built-in registry currently provides `with_project_definition`. Register
additional decorators with `register_decorator("name", factory_fn)` from
`snowflake.cli.api.plugins.command.bridge`. `factory_fn` is called with **no
arguments** and must return the decorator — pass the factory, not the decorator
itself:

```python
from snowflake.cli.api.plugins.command.bridge import register_decorator

# factory_fn takes no arguments and returns a decorator:
register_decorator("with_my_decorator", lambda: with_my_decorator)
```

---

## Reference example

The cookiecutter template under [`plugin-template/`](../../plugin-template) is
the canonical, working example. Generate a project from it (see
[Quickstart](#quickstart-with-the-plugin-template)) to get `interface.py`,
`handler.py`, `plugin_spec.py`, and a passing contract test to build on.
