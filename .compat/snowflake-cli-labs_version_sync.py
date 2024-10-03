# Copyright (c) 2024 Snowflake Inc.
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

import pathlib as p

import tomlkit as tk
from snowflake.cli.__about__ import VERSION

PROJECT_TOML_FILE = p.Path(__file__).parent.joinpath(
    "snowflake-cli-labs",
    "pyproject.toml",
)


def sync_dependecies_version(toml_file: p.Path = PROJECT_TOML_FILE):
    project_toml = tk.loads(toml_file.read_text())

    dependencies = project_toml.get("project", {}).get("dependencies", [])
    updated_dependencies = [f"snowflake-cli=={VERSION}"]
    print(f"Updating `{dependencies}` to `{updated_dependencies}`")

    dependencies.clear()
    dependencies.extend(updated_dependencies)

    toml_file.write_text(tk.dumps(project_toml))


if __name__ == "__main__":
    sync_dependecies_version()
