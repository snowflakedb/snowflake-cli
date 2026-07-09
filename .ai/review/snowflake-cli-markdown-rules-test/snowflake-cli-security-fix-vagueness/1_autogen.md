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

## Deprecations

## New additions

## Fixes and improvements
* Fixed a SQL injection vulnerability in the `query` field of `snowflake.yml` that allowed bypassing row-level security filters and executing arbitrary SQL statements against the connected account.
* Fixed a path traversal vulnerability in `--artifact-dir` that allowed reading arbitrary files outside the project root directory.
