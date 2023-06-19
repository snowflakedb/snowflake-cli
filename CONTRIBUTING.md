# Contributing to SnowCLI


## Setup a development environment
If interested in contributing, you will want to instantiate the pre-commit logic to help with formatting and linting of commits. To do this, run the following in the `snowcli` cloned folder on your development machine:

```bash
pip install pre-commit
pre-commit
```

Required Python version 3.8+

Install all required dependencies

```bash
pip install ".[dev]"
```

Install SnowCLI

```bash
pip install -e .
```

## Integration tests

Every integration test should have `integration` mark. By default, integration tests are not execute when running `pytest`.

To execute only unit tests run `pytest`.

To execute only integration tests run `pytest -m integration`.
