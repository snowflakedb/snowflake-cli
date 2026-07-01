# Copyright (c) 2025 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

RESULT_COLUMN_NAME = "SUCCESS"
OUTPUT_COLUMN_NAME = "STDOUT"
PROFILES_FILENAME = "profiles.yml"
# Higher-precedence profiles file. When ENABLE_FIX_3659937_DBT_PROJECTS_PROFILES_FILE
# is on, this file (if present in --profiles-dir) is staged in preference to
# profiles.yml, under its own name. Mirrors the server-side
# DBT_PROJECTS_PROFILES_FILENAME default.
DBT_PROJECTS_PROFILES_FILENAME = "dbt_projects_profiles.yml"
SUPPORTED_DBT_VERSIONS_QUERY = "SELECT SYSTEM$SUPPORTED_DBT_VERSIONS()"
ENV_FILENAME = "env.yml"

DBT_COMMANDS = [
    "build",
    "compile",
    "deps",
    "list",
    "parse",
    "retry",
    "run",
    "run-operation",
    "seed",
    "show",
    "snapshot",
    "test",
]

UNSUPPORTED_COMMANDS = [
    "clean",
    "clone",
    "debug",
    "docs",
    "init",
    "source",
]
