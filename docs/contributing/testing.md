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

Most contributors run unit tests only. Integration and E2E tests require a live
Snowflake connection.

```bash
hatch run test                              # full unit suite
hatch run pytest tests/<file>              # single file
hatch run pytest tests/<file>::<test>      # single test
```

Do not run integration or E2E tests unless you have a Snowflake account configured.

| Directory | Type | Requires connection | How to run |
|-----------|------|---------------------|------------|
| `tests/` | Unit | No | `hatch run test` |
| `tests_integration/` | Integration | Yes | `pytest -m integration` |
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

@with_feature_flags({FeatureFlag.ENABLE_MY_FEATURE: True})
def test_my_command(runner):
    result = runner.invoke(["my-group", "my-command"])
    assert result.exit_code == 0
    assert "expected output" in result.output
```

Do not enable feature flags by setting environment variables in tests. Use the
helper — it scopes the flag correctly and avoids cross-test contamination.

## What a good unit test covers

- The happy path
- Error paths — assert `result.exit_code != 0` and check the error message
- `result.exit_code` assertion on every invoke — do not only check output

## Fixtures

Unit test fixtures belong in `tests/`. Do not import fixtures from
`tests_integration/` into unit tests — the two directories have different
assumptions about what's available.

## Random test order

Tests run in random order by default (via pytest-randomly). If a test fails
intermittently, re-run it in the same order using the seed printed at the top of
the test run:

```bash
hatch run pytest tests/ --randomly-seed=<number>
```

## Pytest markers

| Marker | Meaning |
|--------|---------|
| `integration` | Requires live Snowflake connection |
| `e2e` | Runs against a freshly installed CLI in a clean venv |
| `performance` | Performance benchmarks |
| `spcs` | Snowpark Container Services (requires SPCS account) |
| `loaded_modules` | Checks which modules are imported at startup |

The default `hatch run test` run excludes `integration`, `performance`, `e2e`,
`spcs`, `loaded_modules`, and `integration_experimental` markers.
