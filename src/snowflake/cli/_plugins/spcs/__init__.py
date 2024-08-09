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

from snowflake.cli._plugins.spcs.compute_pool.commands import (
    app as compute_pools_app,
)
from snowflake.cli._plugins.spcs.image_registry.commands import app as registry_app
from snowflake.cli._plugins.spcs.image_repository.commands import (
    app as image_repository_app,
)
from snowflake.cli._plugins.spcs.services.commands import app as services_app
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory

app = SnowTyperFactory(
    name="spcs",
    help="Manages Snowpark Container Services compute pools, services, image registries, and image repositories.",
)

app.add_typer(compute_pools_app)
app.add_typer(services_app)
app.add_typer(registry_app)
app.add_typer(image_repository_app)
