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

# Testing

## What to run

The project has three test suites:

- **Unit tests** (`tests/`) — fast, no external dependencies, provide quick feedback during development.
  - **Snapshot tests** (subset of unit tests) — capture and verify CLI output formatting. Stored as `.ambr` files in `tests/__snapshots__/`.
- **Integration tests** (`tests_integration/`) — test against a real Snowflake backend. Require a live connection.
- **E2E tests** (`tests_e2e/`) — install the CLI from scratch in a clean virtualenv and run commands against a real Snowflake backend. Require a live connection.

```bash
hatch run test                              # full unit suite (includes snapshots)
hatch run pytest tests/<file>              # single file
hatch run pytest tests/<file>::<test>      # single test
```

| Directory | Type | Requires connection | How to run |
|-----------|------|---------------------|------------|
| `tests/` | Unit | No | `hatch run test` |
| `tests_integration/` | Integration | Yes | `hatch run integration:test` |
| `tests_e2e/` | E2E | Yes | (not for most contributors) |
| `tests_common/` | Shared fixtures | — | (not run directly) |

## Snapshot tests

The project uses [syrupy](https://github.com/toptal/syrupy) for snapshot testing.
Snapshots are stored as `.ambr` files in `tests/__snapshots__/`.

When a snapshot test fails, it means the CLI output changed. If the change is
intentional, regenerate the snapshots:

```bash
hatch run pytest tests/ --snapshot-update
```

`tests_integration/__snapshots__/` and `tests_e2e/__snapshots__/` also contain
syrupy snapshots. Updating them requires running the respective test suite with
`--snapshot-update` and a live Snowflake connection.

Always review the `.ambr` diff before committing. Look for unexpected changes
alongside the intended ones.

### Snapshots capture hidden commands

Snapshot tests capture all commands including those hidden behind feature flags.
Any change to a hidden command — adding, removing, or renaming a command, flag,
or argument — requires regenerating snapshots even if the command is not
user-visible.

## Testing feature-flagged commands

Use the `with_feature_flags` helper from `tests_common/feature_flag_utils.py`:

```python
from tests_common.feature_flag_utils import with_feature_flags
from snowflake.cli.api.feature_flags import FeatureFlag

# As a decorator
@with_feature_flags({FeatureFlag.ENABLE_MY_FEATURE: True})
def test_my_command(runner):
    result = runner.invoke(["my-group", "my-command"])
    assert result.exit_code == 0

# As a context manager (useful in parametrized tests)
def test_my_command_parametrized(runner):
    with with_feature_flags({FeatureFlag.ENABLE_MY_FEATURE: True}):
        result = runner.invoke(["my-group", "my-command"])
        assert result.exit_code == 0
```

Do not enable feature flags by setting environment variables in tests. Use the
helper — it scopes the flag correctly and avoids cross-test contamination.

## What a good unit test covers

- The happy path
- Error paths — assert `result.exit_code != 0` and check the error message
- `result.exit_code` assertion on every invoke — do not only check output

## Fixtures

Unit-only fixtures belong in `tests/conftest.py`. Fixtures shared between unit
and integration tests belong in `tests_common/conftest.py`. Do not import
fixtures from `tests_integration/` into unit tests — the two directories have
different assumptions about what's available.

The fixtures you'll reach for most often in unit tests:

| Fixture | What it gives you |
|---------|-------------------|
| `runner` | `SnowCLIRunner` — invoke CLI commands via `runner.invoke(["cmd", "subcmd"])` |
| `mock_cursor` | Factory: `mock_cursor(rows, columns)` → `MockCursor`. Use this to build fake Snowflake responses. |
| `mock_ctx` | Factory: `mock_ctx(cursor=...)` → `MockConnectionCtx`. Wraps a `mock_cursor` in a connection context. |
| `mock_connect` | Patches `snowflake.connector.connect` to return a `mock_ctx()`. Use when you need the full connection stack mocked. |
| `mock_statement_success` | Returns a `mock_cursor` pre-loaded with `"Statement executed successfully."` |
| `temporary_directory` | `Path` to a fresh temp dir; cleaned up after the test. |
| `project_directory` | Copies a test-project fixture into `temporary_directory` and `chdir`s into it. |
| `config_manager` | Isolated `ConfigManager` backed by the test config file. |

Prefer these fixtures over writing your own `@mock.patch` decorators — they
handle common setup correctly and keep tests consistent.

## Random test order

Tests run in random order by default (via pytest-randomly). If a test fails
intermittently, re-run it in the same order using the seed printed at the top of
the test run:

```bash
hatch run pytest tests/ --randomly-seed=<number>
```

## Integration test environment setup

Integration tests connect using a dedicated `integration` connection. Set these
environment variables before running them:

```bash
SNOWFLAKE_CONNECTIONS_INTEGRATION_AUTHENTICATOR=SNOWFLAKE_JWT
SNOWFLAKE_CONNECTIONS_INTEGRATION_HOST=<host>
SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT=<account>
SNOWFLAKE_CONNECTIONS_INTEGRATION_USER=<user>
SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_PATH=<path>   # preferred
# SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE=<path> # alternative
# SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_RAW=<key>   # load key from env
SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE=<role>
SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE=<database>
SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE=<warehouse>
```

To prepare the account, run the setup script with `ACCOUNTADMIN`:

```bash
snow sql \
  -f tests_integration/scripts/integration_account_setup.sql \
  -D "user=${SNOWFLAKE_CONNECTIONS_INTEGRATION_USER}" \
  -D "role=${SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE}" \
  -D "warehouse=${SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE}" \
  -D "main_database=${SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE}" \
  -c <your_connection_name>
```

## Pytest markers

| Marker | Meaning |
|--------|---------|
| `integration` | Requires live Snowflake connection |
| `e2e` | Runs against a freshly installed CLI in a clean venv |
| `no_qa` | Exclude from QA runs |
| `qa_only` | Run only in QA environments |

The default `hatch run test` run excludes `integration`, `performance`, `e2e`,
`spcs`, `loaded_modules`, and `integration_experimental` markers.
