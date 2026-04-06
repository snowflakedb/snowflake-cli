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

There are two ways to contribute code to the repository: directly or by use of forks. For best practices for the second approach, refer to the section on forks below. Right now, there is limited access to contributing to the repository directly, and hence using forks is the recommended approach.

## Setup a development environment
If you are interested in contributing, you will need to instantiate dev virtual environments and the pre-commit logic to help with formatting and linting of commits.

We use [hatch](https://hatch.pypa.io/latest/) to manage and created development environments.
Default environment will use the python version in your shell.
```bash
pip install -U hatch==1.15.1 virtualenv==20.39.1
hatch run pre-commit
```
This will spawn new shell with environment and all required packages installed.
This will also install snowflake cli package in editable mode.

To enter environment use following command.
```bash
hatch shell
```
This will spawn new shell with virtual environment enables. To leave just press ^D.


Currently, the required Python version for development is Python 3.10+. For local development we recommend to use
a wrapper for virtual environments like [pyenv](https://github.com/pyenv/pyenv).

If you wish to setup environment with specific version ie. 3.10 you can use following command:

```bash
hatch env create local.py3.10```

You can see all locally supported environments with

```bash
hatch env show
```

Please keep in mind that you need these python versions available in your `$PATH`. You can install them using `hatch` or other tool like [pyenv](https://github.com/pyenv/pyenv)

## Unit tests

Unit tests are executed in random order. If tests fail after your change, you can re-execute them in the same order using `pytest --randomly-seed=<number>`, where number is a seed printed at the beginning of the test execution output.
Random order of test execution is provided by pytest-randomly, so more details are available in [pytest-randomly docs](https://pypi.org/project/pytest-randomly/).

```bash
hatch run test
```
or by running `pytest` inside activated environment.


## Integration tests

Every integration test should have `integration` mark. By default, integration tests are not execute when running `pytest`.

To execute only integration tests run `hatch run integration:test` or `pytest -m integration` inside environment.

## Snapshot files

If you added a new test, or changed behavior, you need to regenerate the Syrupy snapshots, which are stored in `.ambr` files.

```bash
hatch run pytest tests/ --snapshot-update
```

Verify your change didn't introduce anything unexpected!

### User setup

Integration tests require environment variables to be set up. Parameters must use the following format:

``SNOWFLAKE_CONNECTIONS_INTEGRATION_<key>=<value>``

where ``<key>`` is the name of the key. The following environment variables are required:

- `SNOWFLAKE_CONNECTIONS_INTEGRATION_AUTHENTICATOR=SNOWFLAKE_JWT`
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_HOST`
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT`
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_USER`
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE` (Preferred)
  - `SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_PATH` (Alternative)
  - `SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW` (Loads the private key directly from the environment variable)
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE`
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE`
- `SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE`

### Integration account setup script

To set up an account for integration tests, run the following script with `ACCOUNTADMIN` role:

```bash
snow sql \
  -f tests_integration/scripts/integration_account_setup.sql \
  -D "user=${SNOWFLAKE_CONNECTIONS_INTEGRATION_USER}" \
  -D "role=${SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE}" \
  -D "warehouse=${SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE}" \
  -D "main_database=${SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE}"\
  -c <your_connection_name>
```

Note: Before running the script, set up your environment variables.

### Build and push Docker images

To build and push all required Docker images, run the following script:

```bash
./tests_integration/tests_using_container_services/spcs/docker/build_and_push_all.sh
```

## Remote debugging with PyCharm or IntelliJ

Snowflake CLI can connect to a remote debug server started in PyCharm or Intellij.
It allows you to debug any installation of our tool in one of mentioned IDEs.

There are only three requirements:
* the same source code loaded in your IDE as running in the debugged CLI installation
* open network connection to your IDE
* `pydevd-pycharm.egg` file accessible on the machine where the CLI is installed (the file has to match the version of your IDE)

How to use it?
1. Create a "remote debug config" run configuration in your IDE.
    * Steps 1-2 from [this tutorial from JetBrains](https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#create-remote-debug-config).
    * `localhost` and `12345` port are defaults both in the IDE and in the CLI.
1. Run your new configuration in debug mode.
1. Find `pydevd-pycharm.egg` in the directory where your IDE is installed.
    * Some tips how to find it should be present in the following places:
      * The instruction from JetBrains linked above.
      * The "remote debug config" creation window.
1. If the CLI and the IDE are on the same machine, you have to just copy the path to the file.
1. If the CLI is on another machine, then you have to copy the file there and also copy the target file path.
1. Run the CLI using `snow --pycharm-debug-library-path <path-to-pydevd-pycharm.egg> <other-options-and-command>`.
    * Example: `snow --pycharm-debug-library-path "/Users/xyz/Library/Application Support/JetBrains/Toolbox/apps/IDEA-U/ch-0/231.9011.34/IntelliJ IDEA.app.plugins/python/debugger-eggs-output/pydevd-pycharm.egg" snowpark function list`
    * The CLI will try to connect to your debug server (by default to `localhost:12345`).
    * If a connection cannot be established, you will see some exception about it.
    * If you want to use other host or port you can use `--pycharm-debug-server-host` and `--pycharm-debug-server-port` options.
    * The code execution will be paused before execution of your command.
      You will see the exact line in the IDE's debug view.
      You can resume code execution, add breakpoints, evaluate variables, do all the things you usually do when debugging locally.

## Using Forks
Create your own fork from the `snowflake-cli` repo. As a heads up, all `snowflake-cli` forks are publicly accessible on Github.

Syncing forks with the upstream `snowflake-cli` repo can be a hassle when trying to resolve merge conflicts. To avoid issues with this approach, we recommend always rebasing to the upstream `snowflake-cli` branch.

In the cloned copy of your fork, perform the following steps.

```bash
git remote add sfcli https://github.com/snowflakedb/snowflake-cli.git
git fetch sfcli
git checkout <your-branch>
git rebase sfcli/main
```

## Presenting intermediate output to users

Snowflake CLI enables users to interact with the Snowflake ecosystem using command line. Some commands provide immediate results, while others require some amount of operations to be executed before the result can be presented to the user.

Presenting intermediate output to the user during execution of complex commands can improve users' experience.

Since snowflake-cli is preparing to support additional commands via plugins, it is the right time to introduce a unified mechanism for displaying intermediate output. This will help keep consistent output among cli and plugins. There is no way to restrain usage of any kind of output in plugins developed in external repositories, but providing api may discourage others from introducing custom output.

The proposal is to introduce cli_console object that will provide following helper methods to interact with the output:
step - a method for printing regular output
warning - a method for printing messages that should be
phase - a context manager that will group all output within its scope as distinct unit

Implemented CliConsole class must respect parameter `–silent` and disable any output when requested.

Context manager must allow only one grouping level at a time. All subsequent invocations will result in raising `CliConsoleNestingProhibitedError` derived from `RuntimeError`.

Logging support
All messages handled by CliConsole may be logged regardless of is_silent property.

### Example usage

#### Simple output

```python
from snowflake.cli.api.console import cli_console as cc

def my_command():
    cc.step("Some work...")
    ...
    cc.step("Next work...")
```

#### Output

```bash
> snow my_command

Some work...
Next work...
```

#### Grouped output

```python
from snowflake.cli.api.console import cli_console as cc

def my_command():
    cc.step("Building and publishing the application")
    prepare_data_for_processing()

    with cc.phase(
      enter_message="Building app bundle...",
      exit_message="Application bundle created.",
    ):
        try:
          cc.step("Building package artifact")
          make_archive_bundle()
        except BundleSizeWarning:
          cc.warning("Bundle size is large. It may take some time to upload.")

        cc.step("Uploading bundle")
        upload_bundle()

    cc.step("Publishing application")
    publish_application()
```

#### Output

```bash
> snow my_command

Building and publishing the application
Building app bundle...
  Building package artifact
  __Bundle size is large. It may take some time to upload.__
  Uploading bundle
Application bundle created.
Publishing application
```

## Writing a Plugin (Interface-First)

Snowflake CLI uses an **interface-first** plugin pattern that separates command
definition from business logic. This lets outside contributors propose a
command surface, get it reviewed, and only then write the implementation.

### Two-Phase Contribution Workflow

```
Phase 1 PR:  interface.py  -->  review command surface  -->  merge
Phase 2 PR:  handler.py + plugin_spec.py  -->  review implementation  -->  merge
```

### Quickstart with the Plugin Template

The fastest way to start is with the cookiecutter template:

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

This generates a complete plugin project:

```
snow-analytics/
├── pyproject.toml                    # Package config with entry point
├── README.md
├── src/snowflakecli_plugins/snow_analytics/
│   ├── interface.py                  # Phase 1: command surface + handler ABC
│   ├── handler.py                    # Phase 2: implementation
│   └── plugin_spec.py               # Wires interface + handler
└── tests/
    └── test_interface.py             # Contract validation tests
```

### Phase 1: Define the Interface

The `interface.py` file contains two things:

1. **Command spec** -- frozen dataclasses describing every command, its
   parameters, help text, and connection requirements.
2. **Handler ABC** -- an abstract class with one method per command.

Example for a plugin with two commands (`snow analytics run` and
`snow analytics report`):

```python
from __future__ import annotations
from abc import abstractmethod
from snowflake.cli.api.output.types import CommandResult
from snowflake.cli.api.plugins.command.interface import (
    CommandDef, CommandGroupSpec, CommandHandler,
    ParamDef, ParamKind, REQUIRED,
)

ANALYTICS_SPEC = CommandGroupSpec(
    name="analytics",
    help="Run analytics queries.",
    parent_path=(),                          # root level: snow analytics
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
                    default=100,
                    required=False,
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
    @abstractmethod
    def run(self, query_name: str, limit: int) -> CommandResult: ...

    @abstractmethod
    def report(self) -> CommandResult: ...
```

**Key types used in interfaces:**

| Dataclass | Purpose |
|-----------|---------|
| `CommandGroupSpec` | Command group with subcommands (e.g. `snow notebook`) |
| `SingleCommandSpec` | A single command (e.g. `snow sql`) |
| `CommandDef` | One command: name, help, params, connection requirements |
| `ParamDef` | One parameter: name, type, argument vs option, CLI names |
| `CommandHandler` | ABC base class for handler methods |

**ParamDef fields:**

| Field | Description |
|-------|-------------|
| `name` | Python parameter name (kwarg to handler method) |
| `type` | Python type (`str`, `int`, `bool`, `FQN`, `Path`, ...) |
| `kind` | `ParamKind.ARGUMENT` or `ParamKind.OPTION` |
| `help` | Help text for `--help` |
| `cli_names` | CLI names, e.g. `("--limit", "-l")`. Empty = auto-derived |
| `default` | Default value. `REQUIRED` = no default |
| `is_flag` | `True` for boolean flags like `--replace` |
| `click_type` | Custom Click `ParamType` for non-standard types (e.g. `IdentifierType()` for `FQN`) |

**Submit the interface for review.** Reviewers can evaluate the complete command
surface without seeing any implementation.

### Setting Up CODEOWNERS for Phase 2

As part of the interface PR, add yourself and a colleague as `CODEOWNERS` for
your plugin directory. This way the Phase 2 implementation PR only requires
review from your team -- not the Snowflake CLI core team.

In your interface PR, append a line to the `CODEOWNERS` file:

```
# my-analytics plugin
/src/snowflake/cli/_plugins/analytics/   @your-github-handle @colleague-handle
```

For external plugins in a separate repository this is not needed, since you
own the repo. This guidance applies to built-in plugins contributed to
`snowflake-cli` by teams outside the CLI core team.

### Phase 2: Implement the Handler

After the interface is approved, create `handler.py`:

```python
from snowflake.cli.api.output.types import CommandResult, MessageResult, QueryResult
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from .interface import AnalyticsHandler


class AnalyticsHandlerImpl(AnalyticsHandler):

    def run(self, query_name: str, limit: int) -> CommandResult:
        executor = SqlExecutionMixin()
        cursor = executor.execute_query(
            f"SELECT * FROM analytics.{query_name} LIMIT {limit}"
        )
        return QueryResult(cursor)

    def report(self) -> CommandResult:
        return MessageResult("Report generated.")
```

Then wire it up in `plugin_spec.py`:

```python
from snowflake.cli.api.plugins.command import build_command_spec, plugin_hook_impl
from .interface import ANALYTICS_SPEC
from .handler import AnalyticsHandlerImpl


@plugin_hook_impl
def command_spec():
    return build_command_spec(ANALYTICS_SPEC, AnalyticsHandlerImpl())
```

### Testing Your Plugin

Use the built-in testing utilities to validate the interface-handler contract:

```python
from snowflake.cli.api.plugins.command.testing import (
    assert_interface_well_formed,
    assert_handler_satisfies,
    assert_builds_valid_spec,
)
from .interface import ANALYTICS_SPEC
from .handler import AnalyticsHandlerImpl


def test_interface():
    assert_interface_well_formed(ANALYTICS_SPEC)

def test_handler_contract():
    assert_handler_satisfies(ANALYTICS_SPEC, AnalyticsHandlerImpl())

def test_full_build():
    assert_builds_valid_spec(ANALYTICS_SPEC, AnalyticsHandlerImpl())
```

These tests run without a Snowflake connection and catch contract mismatches
at development time.

### Installing and Enabling Your Plugin

```bash
# Install in development mode
pip install -e /path/to/your/plugin

# Enable it
snow plugin enable your_plugin_module

# Verify
snow plugin list
```

### Using Decorators

Some commands need extra decorators like `@with_project_definition`. Add them
to `CommandDef.decorators`:

```python
CommandDef(
    name="deploy",
    help="Deploy from project definition.",
    handler_method="deploy",
    requires_connection=True,
    decorators=("with_project_definition",),
    ...
)
```

Available built-in decorators: `with_project_definition`.

Register custom decorators with `register_decorator("name", factory_fn)`.

### Reference Example

See the notebook plugin for a complete migration example:
- `src/snowflake/cli/_plugins/notebook/interface.py` -- 5 commands with various param types
- `src/snowflake/cli/_plugins/notebook/handler.py` -- concrete implementation
- `src/snowflake/cli/_plugins/notebook/plugin_spec.py` -- one-liner wiring

## Known issues

### `permission denied` during integration tests on Windows
This error occurs when using NamedTemporaryFile and try to open it second time https://docs.python.org/3/library/tempfile.html#tempfile.NamedTemporaryFile
