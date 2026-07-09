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

# Unreleased version
## Backward incompatibility
* The `snow object stage` command now requires an explicit `--scope` flag; existing scripts that omitted it will need to be updated.

## Deprecations
* The `--format legacy` option for `snow sql` is deprecated and will be removed in a future release; use `--format table` instead.

## New additions
* Added support for specifying multiple warehouse sizes in a single `snow warehouse create` call using a comma-separated list.
* `snow notebook execute` now accepts a `--timeout` flag to set a maximum execution duration in seconds.

## Fixes and improvements
* Fixed `snow connection test` incorrectly reporting a failed connection when the server returns a non-critical warning during handshake.
* Fixed `snow stage copy` failing silently when the destination path contains spaces; an error message is now shown and the operation is aborted cleanly.
* Fixed `snow git fetch` crashing when the repository URL includes authentication tokens in certain formats.
* Updated `snowflake-connector-python` to version 4.6.0.
* Upgraded `pip` from 24.0 to 24.2.
