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

# Adding Commands

> **Interface-first plugins:** to declare a reviewable command surface separately
> from its implementation (the two-phase contribution workflow used by teams
> outside the CLI core), see [writing-a-plugin.md](writing-a-plugin.md).

## Design sign-off before writing code

Before implementing any change to the user-facing interface — new commands,
command groups, arguments, options, or output format — get maintainer sign-off.

**What sign-off covers:**

- Command and subcommand names
- Flag names, types, and defaults
- Lifecycle stage (PrPr, PuPr, or GA) — see [lifecycle.md](lifecycle.md)
- Output format and result type
- Where the command lives (new group or existing one)

**What sign-off does not cover:** implementation details inside the plugin
directory — service/manager structure, helper functions, file layout. Those are
yours to decide as long as they stay encapsulated within `_plugins/<your-plugin>/`
and do not add to `api/` or `_app/`.

**Where to get sign-off:** open a GitHub Issue describing the interface and tag a
maintainer for review.

---

## Design principles

**Verbs for commands, nouns for groups.** The group is the entity (`snow git`,
`snow stage`), the command is the action (`fetch`, `create`, `list`). Avoid dashes in
command names unless the concept truly cannot be expressed otherwise.

**Find the closest existing command first.** Match its flag names, output
type, and default behavior. Consistency across groups beats local cleverness.

**Commands are what, flags are how.** Flags are configuration and conditions for a
command (`--force`, `--delta`, `--if-exists`), not an alternate operation.

---

## Which case are you in?

**Adding a command to an existing group** — skip to
[Writing commands](#writing-commands). No registration changes needed.

**New top-level command group** — read
[Creating a command group](#creating-a-command-group) first, then
[Registering the plugin](#registering-the-plugin).

---

## Creating a command group

### Flat structure

Use a flat layout when the group manages a single domain (one entity type or
one set of related operations). `snow git` is the canonical example:

```
src/snowflake/cli/_plugins/<name>/
    __init__.py
    commands.py       # @app.command() definitions
    manager.py        # business logic / Snowflake API calls
    plugin_spec.py    # pluggy hook — wires commands into the CLI
```

`commands.py` creates the `SnowTyperFactory` app and defines all commands on it.
See `src/snowflake/cli/_plugins/git/` as a reference.

### Nested structure

Use a nested layout when the group is a capability or platform that hosts
multiple independent entity types — not just because the group is large. `snow spcs`
hosts compute pools, services, image registries, and image repositories as
separate sub-groups because each is a distinct entity type with its own lifecycle.

```
src/snowflake/cli/_plugins/<name>/
    __init__.py           # assembles the top-level app from sub-typers
    plugin_spec.py
    <entity-a>/
        commands.py
        manager.py
    <entity-b>/
        commands.py
        manager.py
```

`__init__.py` creates the parent app and adds each sub-typer:

```python
from snowflake.cli._plugins.<name>.<entity_a>.commands import app as entity_a_app
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

app = SnowTyperFactory(name="<name>", help="...")
app.add_typer(entity_a_app)
```

See `src/snowflake/cli/_plugins/spcs/__init__.py` as a reference.

Start flat. Move to nested only when a second distinct entity type is needed —
the nesting should reflect domain structure, not anticipate it.

---

## Registering the plugin

Two files need to change for a new top-level group.

**`plugin_spec.py`** — the pluggy hook that attaches the app to the CLI:

```python
from snowflake.cli._plugins.<name> import commands
from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)

@plugin_hook_impl
def command_spec():
    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=commands.app.create_instance(),
    )
```

For a nested layout, `app` is defined in `__init__.py`, so import it from the
package directly:

```python
from snowflake.cli._plugins.<name> import app
...
    typer_instance=app.create_instance(),
```

See `src/snowflake/cli/_plugins/spcs/plugin_spec.py` as a reference.

**`src/snowflake/cli/_app/commands_registration/builtin_plugins.py`** — add an
import and a dict entry in `get_builtin_plugin_name_to_plugin_spec()`:

```python
from snowflake.cli._plugins.<name> import plugin_spec as <name>_plugin_spec

def get_builtin_plugin_name_to_plugin_spec():
    plugin_specs = {
        ...
        "<name>": <name>_plugin_spec,
    }
    return plugin_specs
```

The plugin name string is what appears in `snow --help`. Pick a short,
lowercase, hyphen-separated name that matches the `SnowTyperFactory` `name=`
argument.

---

## Writing commands

Every command must return a `CommandResult` subtype. Never call `print()`
directly — use `cli_console` for progress messages (see
[conventions.md](conventions.md)) and return a `CommandResult` for the
command's final output.

The function's docstring becomes the command's `--help` text. Write it as a
short, imperative sentence describing what the command does from the user's
perspective:

```python
@app.command()
def create(name: str):
    """Creates a new table."""
    ...
```

### Return types

All types are in `src/snowflake/cli/api/output/types.py`:

| Type | When to use |
|------|-------------|
| `MessageResult` | Plain string confirmation — "Statement executed successfully." |
| `QueryResult` | Multi-row cursor result (SHOW, SELECT returning many rows) |
| `SingleQueryResult` | Single-row cursor result (DESCRIBE, CREATE ... RETURN ...) |
| `CollectionResult` | Iterable of dicts with no cursor (locally constructed results) |
| `ObjectResult` | Single dict with no cursor |

Other specialized types (`MultipleResults`, `StreamResult`, `EmptyResult`) exist for less common cases — see `src/snowflake/cli/api/output/types.py`.

### Lifecycle and visibility

New commands start in PrPr behind a feature flag. See [lifecycle.md](lifecycle.md)
for the `SnowTyperFactory` and `@app.command()` patterns that hide commands
until the flag is enabled, and for the PrPr → PuPr → GA progression.

### Destructive commands

Commands that modify or delete existing resources should use `ForceOption` and
`InteractiveOption` from `snowflake.cli.api.commands.flags`.

Behavior matrix:

| `--force` | `--interactive` | Result |
|-----------|-----------------|--------|
| unset | False (non-interactive default, e.g. CI or piped input) | Abort — safe default for scripts |
| unset | True (interactive terminal default) | Prompt the user |
| set | either | Proceed without prompting |

### Testing

Every new command needs comprehensive unit and integration tests covering the
happy path and the main error paths. See [testing.md](testing.md) for the
`runner` fixture, snapshot tests, and how to test feature-flagged commands.

## Reusable code in `src/snowflake/cli/api/`

Before writing new utilities, check `src/snowflake/cli/api/` — it contains
shared flags, helpers, and base classes that most plugins rely on:

| Module | What it provides |
|--------|-----------------|
| `commands/flags.py` | Standard flags: `ForceOption`, `InteractiveOption`, `OutputFormatOption`, `PatternOption`, and others |
| `commands/snow_typer.py` | `SnowTyperFactory` — the standard way to create a command group |
| `output/types.py` | All `CommandResult` subtypes |
| `identifiers.py` | `FQN` — fully-qualified name handling (see [conventions.md](conventions.md)) |
| `project/util.py` | `to_string_literal`, `identifier_to_show_like_pattern` |
| `console/__init__.py` | `cli_console` for user-visible output |
| `exceptions.py` | `CliError` subclasses |

For commands that operate on Snowflake objects, check whether an `ObjectManager`
subclass already exists for your object type before writing raw SQL queries.
