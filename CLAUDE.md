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

# Claude Code Instructions

Read **[AGENTS.md](AGENTS.md)** before writing any code. It is the source of
truth for contributing to this repository — which guides to read, the mandatory
code conventions, and the contribution process. Everything in AGENTS.md applies
to Claude Code.

## Upgrading a dependency

Upgrading a Python dependency is covered by the **`upgrade-package`** skill
(`.claude/skills/upgrade-package/`). It activates automatically when you say
"upgrade", "bump", or "update dependency". You can also invoke it explicitly
with `/upgrade-package`.

## Adding a command or plugin

Adding a command, command group, subcommand, or plugin to the CLI is covered by
the **`add-command-or-plugin`** skill (`.claude/skills/add-command-or-plugin/`).
It activates automatically from your request — whether you say "command" or
"plugin" — and routes between the classic in-repo command flow and the
interface-first two-phase plugin workflow. You can also invoke it explicitly
with `/add-command-or-plugin`.
