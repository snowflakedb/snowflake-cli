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
pip install -U hatch
hatch run pre-commit
```
This will spawn new shell with environment and all required packages installed.
This will also install snowflake cli package in editable mode.

To enter environment use following command.
```bash
hatch shell
```
This will spawn new shell with virtual environment enables. To leave just press ^D.


Currently, the required Python version for development is Python 3.8+. For local development we recommend to use
a wrapper for virtual environments like [pyenv](https://github.com/pyenv/pyenv).

If you wish to setup environment with specific version ie. 3.8 you can use following command:

```bash
hatch env create local.py3.8```

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
### Connection parameters in `config.toml`

Add the following connection to your `config.toml`

```toml
[connections.integration]
host = <host>
account = <account_name>
user = <user>
password = <password>
```

### Connection parameters in environment parameters

Parameters must use the following format:

``SNOWFLAKE_CONNECTIONS_INTEGRATION_<key>=<value>``

where ``<key>`` is the name of the key

For example: SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT="my-account"

List of required parameter keys:
- host
- account
- user
- password

### User setup

Run the script with ACCOUNTADMIN role

```bash
tests_integration/scripts/integration_account_setup.sql
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

## Known issues

### `permission denied` during integration tests on Windows
This error occurs when using NamedTemporaryFile and try to open it second time https://docs.python.org/3/library/tempfile.html#tempfile.NamedTemporaryFile
