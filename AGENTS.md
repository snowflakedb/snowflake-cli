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

# Agent Instructions

Before writing any code, read each of the following guides in full:

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [docs/contributing/adding-commands.md](docs/contributing/adding-commands.md)
- [docs/contributing/writing-a-plugin.md](docs/contributing/writing-a-plugin.md)
- [docs/contributing/conventions.md](docs/contributing/conventions.md)
- [docs/contributing/lifecycle.md](docs/contributing/lifecycle.md)
- [docs/contributing/testing.md](docs/contributing/testing.md)
- [docs/contributing/process.md](docs/contributing/process.md)

All requirements in those guides are mandatory, unless explicitly marked otherwise.

## Adding a command, command group, or plugin

Treat "add a command", "add a command group", "add a subcommand", and "add a
plugin" as the same task — they all add commands under `snow`. Two workflows
exist; read both guides above and present the one that fits:

- **Interface-first plugin** ([writing-a-plugin.md](docs/contributing/writing-a-plugin.md))
  — declare the command surface in `interface.py`, get it reviewed, then
  implement `handler.py`. **Default for a team contributing its own commands.**
- **Classic in-repo command** ([adding-commands.md](docs/contributing/adding-commands.md))
  — define commands directly with `@app.command()`. For CLI-core contributions.

When unsure, ask whether the team owns its command surface (interface-first) or
is extending core CLI commands (classic).
