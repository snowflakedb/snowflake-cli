# Copyright (c) 2026 Snowflake Inc.
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


from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

app = SnowTyperFactory(
    name="project",
    help="Manages notebook projects in Snowflake.",
)


@app.command(requires_connection=True)
def create(**options):
    """Creates a notebook project in Snowflake."""
    pass


@app.command(requires_connection=True)
def list_projects(**options):
    """Lists notebook projects in Snowflake."""
    pass


@app.command(requires_connection=True)
def delete(**options):
    """Deletes a notebook project in Snowflake."""
    pass


@app.command(requires_connection=True)
def execute(**options):
    """Executes a notebook project in Snowflake."""
    pass
