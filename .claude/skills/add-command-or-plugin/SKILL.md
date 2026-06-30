---
name: add-command-or-plugin
description: >-
  Use when adding, creating, or contributing a command, command group,
  subcommand, or plugin to the Snowflake CLI — e.g. "add a new command",
  "add my team's commands", "new CLI command", "create a command group",
  "write a plugin", "add a plugin", or "interface-first plugin". Routes
  between the classic in-repo command flow and the interface-first two-phase
  plugin workflow and points to the authoritative contributing guides.
---

# Adding a command or plugin to Snowflake CLI

"Add a command", "add a command group", "add a subcommand", and "add a plugin"
are the same task: you are adding commands under `snow`. Pick the workflow, get
the command surface signed off, then implement.

## 1. Pick the workflow

| Situation | Workflow | Full guide |
|-----------|----------|------------|
| Your team owns its command surface and wants it reviewed *before* implementation (especially teams outside the CLI core) | **Interface-first plugin** (two-phase) — **the default for team contributions** | `docs/contributing/writing-a-plugin.md` |
| You are a CLI-core contributor extending the CLI in-repo | **Classic** — commands defined directly with `@app.command()` | `docs/contributing/adding-commands.md` |

If you are unsure which applies, **ask the user**: does your team own these
commands (→ interface-first) or are you extending core CLI commands (→ classic)?
Default to interface-first for team contributions. Start an interface-first
plugin from the cookiecutter template in `plugin-template/`.

## 2. Always first: design sign-off

Before writing code, get the command surface approved — names, flags, lifecycle
stage (PrPr/PuPr/GA), output type. See `docs/contributing/adding-commands.md`
→ "Design sign-off before writing code". This applies to both workflows.

## 3a. Interface-first, in brief

Full steps and the complete type reference are in
`docs/contributing/writing-a-plugin.md` — read it before implementing. The shape:

- **Phase 1 PR — `interface.py`:** declare the command surface as frozen
  dataclasses (`CommandGroupSpec` or `SingleCommandSpec`, `CommandDef`,
  `ParamDef`) plus a `CommandHandler` ABC. Reviewed on its own, before any
  implementation exists. For a built-in plugin, add yourself to `CODEOWNERS`
  for the plugin directory in this PR so Phase 2 only needs your team's review.
- **Phase 2 PR — `handler.py` + `plugin_spec.py`:** implement each handler
  method, then wire them together with `build_command_spec(SPEC, HandlerImpl())`.
  A plugin that lives in this repository must also be registered in
  `builtin_plugins.py`.
- **Test** with `assert_interface_well_formed`, `assert_handler_satisfies`, and
  `assert_builds_valid_spec` from `snowflake.cli.api.plugins.command.testing`
  (no Snowflake connection required).

## 3b. Classic, in brief

Full steps are in `docs/contributing/adding-commands.md`. The shape: `commands.py`
(`@app.command()` definitions) + `manager.py` (business logic) + `plugin_spec.py`
(the pluggy hook); register a new top-level group in
`src/snowflake/cli/_app/commands_registration/builtin_plugins.py`.

## 4. Rules that apply to both workflows

- Return a `CommandResult` subtype (`src/snowflake/cli/api/output/types.py`);
  never call `print()` — use `cli_console` for progress output.
- **Never interpolate user input into SQL.** Escape strings with
  `to_string_literal` and wrap identifiers with `FQN.sql_identifier`
  (see `docs/contributing/conventions.md`).
- New commands start in PrPr behind a feature flag — see
  `docs/contributing/lifecycle.md`.
- Add unit **and** integration tests — see `docs/contributing/testing.md`.

## Reference plugins

- `src/snowflake/cli/_plugins/git/` — flat layout.
- `src/snowflake/cli/_plugins/spcs/` — nested layout.
