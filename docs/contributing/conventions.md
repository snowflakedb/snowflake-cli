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

# Code Conventions


## SQL safety

Never interpolate user-supplied values or config values directly into SQL strings.
Use the appropriate helper depending on whether the value is an **object identifier**
or a **string value**.

### Object identifiers (table names, stage names, schema names, …)

Use `FQN.from_string().sql_identifier`, which wraps the identifier in
`IDENTIFIER('...')` so Snowflake resolves it correctly:

```python
# WRONG
cursor.execute(f"CREATE TABLE {database}.{schema}.{table_name} ...")

# CORRECT
from snowflake.cli.api.identifiers import FQN
fqn = FQN.from_string(f"{database}.{schema}.{table_name}")
cursor.execute(f"CREATE TABLE {fqn.sql_identifier} ...")
```

### String values (LIKE patterns, CALL arguments, string parameters)

Use `to_string_literal()` from `snowflake.cli.api.project.util`, which produces
a properly escaped single-quoted string literal:

```python
# WRONG
cursor.execute(f"SHOW DATABASES LIKE {database}")

# CORRECT
from snowflake.cli.api.project.util import to_string_literal
cursor.execute(f"SHOW DATABASES LIKE {to_string_literal(database)}")
```

## Terminal output safety

Never print values from Snowflake server responses or user config directly to the
terminal. They may contain ANSI escape sequences or terminal control characters.

```python
# WRONG
cc.step(f"Object name: {server_response['name']}")

# CORRECT
from snowflake.cli.api.sanitizers import sanitize_for_terminal
cc.step(f"Object name: {sanitize_for_terminal(server_response['name'])}")
```

## Error handling

The project is migrating from `ClickException` to typed `CliError` subclasses.
Existing `ClickException` usages are legacy — do not imitate them.

All new code must use `CliError` or one of its subclasses from
`snowflake.cli.api.exceptions`:

```python
# WRONG
from click import ClickException
raise ClickException("Something went wrong")

# CORRECT
from snowflake.cli.api.exceptions import SnowflakeCLIError  # or a more specific subclass
raise SnowflakeCLIError("Something went wrong")
```

Check `src/snowflake/cli/api/exceptions.py` for available subclasses before
raising a generic error.

Error messages should describe what went wrong and, where possible, what the
user can do about it:

```python
# WRONG — states what happened but not what to do
raise SnowflakeCLIError(f"File {path} not found")

# CORRECT — states what happened and how to fix it
raise SnowflakeCLIError(f"File {path} not found. Check the path and try again.")
```

## Imports

Prefer top-of-file imports. Local imports inside functions are acceptable only
when unavoidable (e.g. to break circular imports or for config-time deferred
loading), not as a general convenience.

```python
# PREFERRED
from snowflake.cli.api.identifiers import FQN

def my_command():
    ...

# ACCEPTABLE only when a top-level import causes issues
def my_command():
    from snowflake.cli.api.identifiers import FQN
    ...
```

## Logging

Use `logging.getLogger(__name__)` for diagnostic messages that help with
troubleshooting but are not user-facing:

```python
import logging

log = logging.getLogger(__name__)

def my_function():
    log.debug("Starting operation with params: %s", params)
```

Use `%`-style formatting, not f-strings:

```python
# WRONG
log.debug(f"Processing {len(items)} items")

# CORRECT
log.debug("Processing %s items", len(items))
```

Never log sensitive values such as passwords, tokens, or private keys. Connection
parameters from config may contain credentials — log identifiers (account, user,
role) but not secrets.

Use `cli_console` (see below) for messages the user should see during normal
operation. The two are complementary: `cli_console` drives the interactive
experience, `logging` provides the paper trail visible in debug output.

## User-visible output

Use `cli_console` from `snowflake.cli.api.console` for all user-visible output.
Never use `print()` directly. `cli_console` respects `--silent` automatically.

```python
from snowflake.cli.api.console import cli_console as cc

cc.step("Doing something...")          # regular progress line
cc.warning("Something looks off")      # warning line

with cc.phase("Building...", "Done."): # grouped output block
    cc.step("Step A")
    cc.step("Step B")
```
