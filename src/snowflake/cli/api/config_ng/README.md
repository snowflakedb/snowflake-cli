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

## Config NG overview

This package implements the "next generation" configuration system used by
Snowflake CLI. It is a read-only resolver with explicit source ordering,
full provenance tracking, and dedicated presentation/logging utilities.

### Resolution flow
- **Four phases in `resolver.py`**: file sources (connection-level replacement),
  overlay sources (field-level merge), default connection creation, then history
  finalization.
- **Precedence is explicit**: the resolver consumes sources in the order they are
  provided; later sources win.
- **Default connection** is created when only root-level connection parameters
  are provided (no `connections` section).

### Source types
- **FILE sources** (replace whole connections): `SnowSQLConfigFile`,
  `CliConfigFile`, `ConnectionsConfigFile`.
- **OVERLAY sources** (merge fields): `SnowSQLEnvironment`, `CliEnvironment`,
  `ConnectionSpecificEnvironment`, `CliParameters`.
- Root-level params are split via `extract_root_level_connection_params()` and
  merged into connections via `merge_params_into_connections()`.

### Observability and presentation
- **Observers** (`observers.py`) collect telemetry stats and full per-key history.
- **Presentation** (`presentation.py`) formats tables, chains, and exports masked
  history to JSON.
- **Logging helpers** (`resolution_logger.py`) expose high-level entry points
  when the alternative config provider is enabled.
- **Diagnostics**: sources can emit `SourceDiagnostic` messages that the resolver
  collects and the presenter renders.
- **Masking** is key-based: sensitive values are masked when keys include
  sensitive fragments (see `masking.py`). Values under non-sensitive keys are
  displayed as-is.

### Extending the system
- Add a new source by implementing `ValueSource` in `core.py`, returning nested
  dicts from `discover()` and identifying `source_type` and `source_name`.
- Wire new sources in `source_factory.py` / `source_manager.py` and update
  `constants.py` with any new source names or env var keys.
- Keep masking in mind for sensitive values (see `masking.py` and presenter
  usage).

### Example: snow helpers config resolution
The `snow helpers show-config-sources` command renders config and connection
resolution details when the alternative config provider is enabled:

```
export SNOWFLAKE_CLI_CONFIG_V2_ENABLED=true

# Show a summary table for all keys
snow helpers show-config-sources

# Show detailed resolution for a single key in a specific connection
snow helpers show-config-sources account --connection dev --show-details
```

Example output (abridged):

```
key                       value   params  global_envs  connections_env  snowsql_env  connections.toml  config.toml  snowsql
connections.dev.account   acme    +       +                                         +                 -            -
connections.dev.user      alice           +                                         +                 -            -
connections.dev.role      analyst                                                        +              -            -

Configuration Resolution History
================================================================================

Key: connections.dev.account
Final Value: acme
Resolution Chain:
  1. [overridden] snowsql_config: legacy_acme
  2. [overridden] cli_config_toml: acme
  3. [SELECTED] cli_arguments: acme
```
