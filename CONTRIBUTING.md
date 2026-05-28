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

# Contributing to Snowflake CLI

## Detailed guides

- **Adding commands**: [docs/contributing/adding-commands.md](docs/contributing/adding-commands.md)
  — design sign-off, plugin structure, registration, return types, destructive commands
- **Command lifecycle and feature flags**: [docs/contributing/lifecycle.md](docs/contributing/lifecycle.md)
  — PrPr/PuPr/GA rules, feature flag mechanics, release notes format
- **Code conventions**: [docs/contributing/conventions.md](docs/contributing/conventions.md)
  — SQL safety, secrets, file access, terminal output, error handling, imports, logging, user-visible output
- **Testing**: [docs/contributing/testing.md](docs/contributing/testing.md)
  — unit tests, snapshots, fixtures, feature flag test helpers
- **Process**: [docs/contributing/process.md](docs/contributing/process.md)
  — merge model, commit format, reviewer etiquette
- **Remote debugging**: [docs/contributing/remote-debugging.md](docs/contributing/remote-debugging.md)
  — debugging with PyCharm or IntelliJ

## Quick start

```bash
pip install -U hatch==1.15.1 virtualenv==20.39.1
hatch shell
hatch run test
```

`hatch shell` spawns a new shell with the virtual environment active and the CLI
installed in editable mode. Press `^D` to exit.

To also activate pre-commit hooks (required before your first commit):

```bash
hatch run pre-commit install
```

## Reporting bugs and requesting features

File a [GitHub Issue](https://github.com/snowflakedb/snowflake-cli/issues). Use
the issue templates — they collect the information needed to act on the report
quickly.

## How to contribute code

Fork the repo and submit a pull request from your fork. All forks of
`snowflake-cli` are publicly visible on GitHub.

Keep your fork up to date by rebasing onto upstream rather than merging:

```bash
git remote add sfcli https://github.com/snowflakedb/snowflake-cli.git
git fetch sfcli
git checkout <your-branch>
git rebase sfcli/main
```

A maintainer will review and merge your PR after approval.

## Development environment

We use [hatch](https://hatch.pypa.io/latest/) to manage development environments.
The supported Python versions are listed in `pyproject.toml` under the
`Programming Language :: Python` classifiers. We recommend
[pyenv](https://github.com/pyenv/pyenv) for managing Python versions locally.

Do not use language features that are unavailable in the minimum supported
version — code must run on all versions in the supported range.

```bash
pip install -U hatch==1.15.1 virtualenv==20.39.1
hatch run pre-commit install
```

To create an environment pinned to a specific Python version:

```bash
hatch env create local.py3.10
```

To list all available environments:

```bash
hatch env show
```

## Running tests

```bash
hatch run test                 # unit tests
hatch run integration:test     # integration tests (requires Snowflake connection)
```

See [docs/contributing/testing.md](docs/contributing/testing.md) for snapshots,
fixtures, markers, and integration test environment setup.

## Before submitting a PR

The PR template contains a checklist. Complete it honestly — reviewers check
every item and CI enforces some of them.
