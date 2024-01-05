# Contributing to SnowCLI

There are two ways to contribute code to the repository: directly or by use of forks. For best practices for the second approach, refer to the section on forks below. Right now, there is limited access to contributing to the repository directly, and hence using forks is the recommended approach.

## Setup a development environment
If you are interested in contributing, you will need to instantiate the pre-commit logic to help with formatting and linting of commits.
To do this, run the following in the `snowcli` cloned folder on your development machine:

```bash
pip install pre-commit
pre-commit
```

Currently, the required Python version for development is Python 3.8+. For local development we recommend to use
a wrapper for virtual environments like [pyenv](https://github.com/pyenv/pyenv).

Once you created a dedicated virtual environment you can install SnowCLI in editable mode with all required dependencies:

```bash
pip install -e ".[dev]"
```

## Unit tests

Unit tests are executed in random order. If tests fail after your change, you can re-execute them in the same order using `pytest --randomly-seed=<number>`, where number is a seed printed at the beginning of the test execution output.
Random order of test execution is provided by pytest-randomly, so more details are available in [pytest-randomly docs](https://pypi.org/project/pytest-randomly/).

## Integration tests

Every integration test should have `integration` mark. By default, integration tests are not execute when running `pytest`.

To execute only integration tests run `pytest -m integration`.

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

### User requirements

The user requires a default role with the following grants

```snowflake
grant create database on account to role <role_name>;
grant create role on account to role <role_name>;
grant usage on warehouse xsmall to role <role_name>;
grant operate on warehouse xsmall to role <role_name>;
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
Create your own fork from the `snowcli` repo. As a heads up, all `snowcli` forks are publicly accessible on Github.

Syncing forks with the upstream `snowcli` repo can be a hassle when trying to resolve merge conflicts. To avoid issues with this approach, we recommend always rebasing to the upstream `snowcli` branch.

In the cloned copy of your fork, perform the following steps.

```bash
git remote add sfcli https://github.com/Snowflake-Labs/snowcli.git
git fetch sfcli
git checkout <your-branch>
git rebase sfcli/main
```

## Known issues

### `permission denied` during integration tests on Windows
This error occurs when using NamedTemporaryFile and try to open it second time https://docs.python.org/3/library/tempfile.html#tempfile.NamedTemporaryFile
