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

# Command Lifecycle: PrPr, PuPr, and GA

Commands, command groups, flags, and arguments each progress through their own
lifecycle independently. A command group can be GA while a specific command
within it is still in PuPr, and a specific flag on that command can be in PrPr.

By default, users must never see non-GA features. PrPr and PuPr features must
always require an explicit opt-in (via feature flag) before they are visible.

## Stage definitions

### PrPr (Private Preview)

- Feature is hidden behind a feature flag (default `False`)
- No release note entry required
- Breaking changes are acceptable
- No backward compatibility obligation

### PuPr (Public Preview)

- Feature remains behind its feature flag — not visible to users who have not opted in
- One-line release note required when entering PuPr (under `## New additions`):
  ```
  * <name> is now available in preview.
  ```
- Breaking changes are no longer acceptable from this point
- Full backward compatibility rules apply going forward

### GA (Generally Available)

- Feature flag removed from `src/snowflake/cli/api/feature_flags.py`
- `is_hidden` / `hidden=` stripped from the feature
- Explicit release note required when entering GA (under `## New additions`):
  ```
  * <name> is now generally available.
  ```
- Full backward compatibility rules apply

## Feature flags

Flags are defined in `src/snowflake/cli/api/feature_flags.py` as entries in the
`FeatureFlag` enum:

```python
class FeatureFlag(FeatureFlagMixin):
    ENABLE_MY_FEATURE = BooleanFlag("ENABLE_MY_FEATURE", False)
```

### Enabling a flag

Flags can be set in two ways:

**Environment variable** — `SNOWFLAKE_CLI_FEATURES_<FLAG_NAME_UPPERCASE>`:
```bash
SNOWFLAKE_CLI_FEATURES_ENABLE_MY_FEATURE=true snow my-command
```

**`config.toml`** — under the `[cli.features]` section:
```toml
[cli.features]
enable_my_feature = true
```

### Applying flags to features

Flags can gate a command group, a single command, a specific argument, or any
combination. The right scope depends on what you're hiding.

**Hide an entire command group** (group absent from `snow --help`, but still
callable by users who know the flag — useful for internal testing):
```python
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag
import typer

app = SnowTyperFactory(
    name="my-group",
    is_hidden=FeatureFlag.ENABLE_MY_FEATURE.is_disabled,
)
```

**Hide a single command** (command absent from `snow my-group --help`, still
callable):
```python
@app.command(hidden=not FeatureFlag.ENABLE_MY_FEATURE.is_enabled())
def my_command():
    ...
```

**Hide a single option or argument**:
```python
@app.command()
def my_command(
    my_option: Optional[str] = typer.Option(
        None,
        hidden=not FeatureFlag.ENABLE_MY_FEATURE.is_enabled(),
    ),
):
    ...
```

**Gate a feature so it cannot be invoked at all** (use sparingly — only when
running without the flag would be unsafe or meaningless):
```python
@app.command(is_enabled=FeatureFlag.ENABLE_MY_FEATURE.is_enabled)
def my_command():
    ...
```

### Independent lifecycle example

A command group, one of its commands, and a flag on that command can each be at
a different lifecycle stage simultaneously:

```python
# Command group is GA — no feature flag
app = SnowTyperFactory(name="my-group")

# This command is PuPr — hidden unless flag is enabled
@app.command(hidden=not FeatureFlag.ENABLE_MY_COMMAND.is_enabled())
def my_command(
    # This option is PrPr — hidden unless a second flag is enabled
    experimental_option: Optional[str] = typer.Option(
        None,
        hidden=not FeatureFlag.ENABLE_MY_EXPERIMENTAL_OPTION.is_enabled(),
    ),
):
    ...
```

## Transitioning between stages

### PrPr → PuPr

1. Add a release note entry under `## New additions`:
   `* <name> is now available in preview.`
2. No breaking changes from this point forward

### PuPr → GA

1. Remove the flag entry from `src/snowflake/cli/api/feature_flags.py` and all
   remaining references to it in the codebase.
2. Remove `is_hidden=` / `hidden=` from the feature
3. Regenerate snapshots — removing `hidden=` changes snapshot output even though
   the command was already callable. See [testing.md](testing.md#snapshot-tests).
4. Add a release note entry under `## New additions`:
   `* <name> is now generally available.`

## Testing feature-flagged features

Use `with_feature_flags` from `tests_common/feature_flag_utils.py`. Never set
feature flag env vars directly in tests.

```python
from tests_common.feature_flag_utils import with_feature_flags
from snowflake.cli.api.feature_flags import FeatureFlag

@with_feature_flags({FeatureFlag.ENABLE_MY_FEATURE: True})
def test_my_command(runner):
    result = runner.invoke(["my-group", "my-command"])
    assert result.exit_code == 0
```

## Snapshot tests and hidden features

Snapshot tests capture all commands and options including hidden ones. Any
change to a hidden feature — adding, removing, or renaming — requires
regenerating snapshots even though the feature is not user-visible:

```bash
hatch run pytest tests/ --snapshot-update
```

Always review the `.ambr` diff in `tests/__snapshots__/` before committing.

## Backward compatibility

Once a feature reaches PuPr, the following are covered by the backward
compatibility guarantee:

- Command and subcommand names
- Flag names (both long form `--flag` and short form `-f`)
- Flag default values
- `config.toml` fields
- `snowflake.yml` schema fields

Command output is **not** guaranteed — many commands return raw Snowflake server
responses whose shape is outside CLI's control.

Any change that breaks the above requires a `## Backward incompatibility` entry
in `RELEASE-NOTES.md` and must not land in a minor version.

## Deprecating commands and flags

### Deprecating a command

Mark the command with `deprecated=True`. Click will print a deprecation warning
automatically when the command is invoked:

```python
@app.command(deprecated=True)
def my_old_command():
    ...
```

Add a release note entry under `## Deprecations`.

### Deprecating a flag

Use `deprecated_flag_callback` from `snowflake.cli.api.commands.flags` — do not
roll your own callback:

```python
from snowflake.cli.api.commands.flags import deprecated_flag_callback

@app.command()
def my_command(
    old_option: Optional[str] = typer.Option(
        None,
        callback=deprecated_flag_callback("Use --new-option instead."),
    ),
):
    ...
```

Add a release note entry under `## Deprecations`.

### Removal policy

Deprecated commands and flags are not removed between minor versions. Removal
may happen in a major version bump, but is not guaranteed to happen in the next
one.

## Release notes format

The full release notes format and per-stage rules are in
[`RELEASE-NOTES.md`](../../RELEASE-NOTES.md). The sections are:

```markdown
## Backward incompatibility
## Deprecations
## New additions
## Fixes and improvements
```

All unreleased changes go under `# Unreleased version` at the top of the file.

Entries should describe user-facing impact, not implementation details. Focus on
what changed for the user, not how it was implemented.

## CI enforcement

A GitHub Actions workflow (`changelog.yaml`) checks that `RELEASE-NOTES.md` is
modified on every PR targeting `main`. The check fails if the file is untouched.

To skip the check for PRs that genuinely need no entry (e.g. pure test or
documentation changes), apply the **`skip-release-notes`** label to the PR.
