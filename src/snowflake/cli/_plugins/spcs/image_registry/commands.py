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

from snowflake.cli._plugins.spcs.image_registry.manager import (
    RegistryManager,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult, ObjectResult

app = SnowTyperFactory(
    name="image-registry",
    help="Manages Snowpark Container Services image registries.",
    short_help="Manages image registries.",
)


_TOKEN_EPILOG = """\
Usage Example: snow spcs image-registry token --format JSON | docker login $(snow spcs image-registry url) -u 0sessiontoken --password-stdin

See the following for how to use this command to authenticate with SSO:
https://community.snowflake.com/s/article/Authenticating-with-Snowpark-Container-Services-Image-Repository-via-SSO-and-Token-with-Snow-CLI\
"""


@app.command(requires_connection=True, epilog=_TOKEN_EPILOG)
def token(**options) -> ObjectResult:
    """
    Retrieves a registry authentication token based on your current connection.

    Note that this token is specific to your current user and will not grant access to any repositories that your current user cannot access.


    """
    return ObjectResult(RegistryManager().get_token())


@app.command(requires_connection=True)
def url(**options) -> MessageResult:
    """
    Gets the image registry URL for the current account.

    Must be called from a role that can view at least one image repository in the image registry.
    """
    return MessageResult(RegistryManager().get_registry_url())


@app.command(requires_connection=True)
def login(**options) -> MessageResult:
    """
    Logs in to the account image registry with the current user's credentials through Docker.

    Must be called from a role that can view at least one image repository in the image registry.
    """
    return MessageResult(RegistryManager().docker_registry_login().strip())
