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
## New additions
* Updated GitHub Actions CI pipeline to use reusable workflow definitions, reducing duplication across jobs.

## Fixes and improvements
* Updated `_resolve_connection_params` to call `ConfigManager.fetch_raw_dict` instead of the deprecated `ConfigManager.load_legacy_format` so that internal config parsing is now handled by a single unified code path.

# v3.22.1

## Fixes and improvements
* Fixed `snow streamlit deploy` incorrectly unsetting governance tags on redeploy.

# v3.23.0

## New additions
* Added `snow app bundle` command for packaging Snowflake Native App artifacts locally.

## Fixes and improvements
* Fixed `snow app setup` incorrectly treating system-default parameter values as admin-configured values.

# v3.22.0

## New additions
* Added `tags` field to Streamlit entity so that tags can be set with `snow streamlit deploy`.

## Fixes and improvements
* Fixed `snow connection test` incorrectly reporting a failed connection.

# v3.21.1

## New additions
* Added `--watch` flag to `snow app open` to poll until the app service is ready.

## Fixes and improvements
* Fixed `snow connection test` failing to handle multi-factor authentication prompts correctly when using the `externalbrowser` authenticator.

# v3.21.0

## New additions
* Added `snow notebook execute` command.

## Fixes and improvements
* Fixed `snow stage copy` failing when destination path contains spaces.
