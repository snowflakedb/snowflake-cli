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

# Configuration Merging Test Framework

## Overview

This test framework provides an easy-to-use, readable way to test merged configuration from multiple sources in the Snowflake CLI.

## Features

### Configuration Sources

The framework supports testing all configuration sources:

1. **SnowSQLConfig**: SnowSQL INI-style config files (`.snowsql/config`)
2. **SnowSQLEnvs**: SnowSQL environment variables (`SNOWSQL_*`)
3. **CliConfig**: CLI TOML config files (`.snowflake/config.toml`)
4. **CliEnvs**: CLI environment variables (`SNOWFLAKE_*`)
5. **CliParams**: CLI command-line parameters (`--account`, `--user`, etc.)
6. **ConnectionsToml**: Connections TOML files (`.snowflake/connections.toml`)

### Configuration Priority

The framework correctly tests the precedence order:
1. CLI parameters (highest)
2. CLI environment variables (`SNOWFLAKE_*`)
3. SnowSQL environment variables (`SNOWSQL_*`)
4. CLI config files
5. Connections TOML
6. SnowSQL config files (lowest)

## Usage

### Basic Example

```python
from tests.config_ng.conftest import (
    CliConfig,
    CliEnvs,
    CliParams,
    SnowSQLConfig,
    SnowSQLEnvs,
    config_sources,
)

def test_configuration_merging():
    sources = (
        SnowSQLConfig("config"),
        SnowSQLEnvs("snowsql.env"),
        CliConfig("config.toml"),
        CliEnvs("cli.env"),
        CliParams("--account", "test-account", "--user", "alice"),
    )
    
    with config_sources(sources) as ctx:
        merged = ctx.get_merged_config()
        
        # CLI params have highest priority
        assert merged["account"] == "test-account"
        assert merged["user"] == "alice"
```

### Testing Specific Connections

```python
def test_specific_connection():
    sources = (ConnectionsToml("connections.toml"),)
    
    with config_sources(sources, connection="prod") as ctx:
        merged = ctx.get_merged_config()
        assert merged["account"] == "prod-account"
```

### Using FinalConfig for Readability

```python
from textwrap import dedent

from tests.config_ng.conftest import FinalConfig

# From dictionary
expected = FinalConfig(config_dict={
    "account": "test-account",
    "user": "alice",
})

# From TOML string (more readable for complex configs)
# Use dedent to avoid indentation issues
expected = FinalConfig(toml_string=dedent("""
    [connections.prod]
    account = "prod-account"
    user = "prod-user"
    password = "secret"
    """))

# Compare with merged config
assert merged == expected
```

### Accessing Resolution History

```python
with config_sources(sources) as ctx:
    resolver = ctx.get_resolver()
    config = resolver.resolve()
    
    # Check which source won
    history = resolver.get_resolution_history("account")
    assert history.selected_entry.config_value.source_name == "cli_arguments"
    
    # Get resolution summary
    summary = resolver.get_history_summary()
    print(f"Total keys resolved: {summary['total_keys_resolved']}")
    print(f"Keys with overrides: {summary['keys_with_overrides']}")
```

## Test File Structure

### Required Directory Structure

```
tests/config_ng/
├── conftest.py              # Test framework implementation
├── test_configuration.py    # Example tests
└── configs/                 # Test configuration files
    ├── config               # SnowSQL config
    ├── snowsql.env         # SnowSQL environment variables
    ├── config.toml         # CLI config
    ├── cli.env             # CLI environment variables
    └── connections.toml    # Connections config
```

### Configuration Files

Create test configuration files in `tests/config_ng/configs/`:

**config** (SnowSQL format):
```ini
[connections.a]
accountname = account-a
user = user
password = password
```

**config.toml** (CLI format):
```toml
[connections.a]
account = "account-a"
username = "user"
password = "abc"
```

**cli.env**:
```bash
SNOWFLAKE_USER=Alice
```

**snowsql.env**:
```bash
SNOWSQL_USER=Bob
```

## Implementation Details

### Context Manager

The `config_sources` context manager:
- Creates temporary directories for config files
- Writes config files to proper locations
- Sets environment variables
- Cleans up after test completion

### ConfigSourcesContext

Provides methods:
- `get_merged_config()`: Returns the merged configuration dictionary
- `get_resolver()`: Returns the ConfigurationResolver for advanced testing

## Running Tests

```bash
# Run with timeout
timeout 30 hatch env run -- pytest tests/config_ng/test_configuration.py -v -p no:warnings

# Run all config_ng tests
timeout 60 hatch env run -- pytest tests/config_ng/ -v -p no:warnings

# Run with pre-commit checks
hatch env run -- pre-commit run --files tests/config_ng/conftest.py tests/config_ng/test_configuration.py
```

## Benefits

1. **Readable**: Tests clearly express intent with descriptive source objects
2. **Isolated**: Each test runs in a clean temporary environment
3. **Comprehensive**: Tests all configuration sources and their interactions
4. **Type-safe**: Full mypy type checking support
5. **Maintainable**: Centralized logic in `conftest.py`
6. **Flexible**: Easy to add new test scenarios

## Examples from Tests

See `test_configuration.py` for complete examples:
- `test_all_sources_merged`: Tests complete precedence chain
- `test_cli_envs_override_snowsql_envs`: Tests environment variable precedence
- `test_config_files_precedence`: Tests file precedence
- `test_resolution_history_tracking`: Tests resolution debugging features
