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

import typer
from snowflake.cli._plugins.spcs.image_registry.manager import (
    RegistryManager,
)
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import MessageResult, ObjectResult

app = SnowTyperFactory(
    name="image-registry",
    help="Manages Snowpark Container Services image registries.",
    short_help="Manages image registries.",
)

PrivateLinkOption: bool = typer.Option(
    False,
    "--private-link",
    help="Get the private link URL instead of the public URL.",
)

_IMAGE_REGISTRY_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/command-reference/overview"),
    link(
        "/developer-guide/snowflake-cli/command-reference/spcs-commands/overview",
        "spcs command reference",
    ),
    link(
        "/developer-guide/snowflake-cli/command-reference/spcs-commands/image-registry-commands/overview",
        "image-repository command reference",
    ),
    link("/developer-guide/snowflake-cli/services/manage-images"),
)

_TOKEN_EPILOG = """\
Usage Example: snow spcs image-registry token --format JSON | docker login $(snow spcs image-registry url) -u 0sessiontoken --password-stdin

See the following for how to use this command to authenticate with SSO:
https://community.snowflake.com/s/article/Authenticating-with-Snowpark-Container-Services-Image-Repository-via-SSO-and-Token-with-Snow-CLI\
"""


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REGISTRY_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "The following example shows how to return the token associated with "
                    "the specified connection that you can use to authenticate with the "
                    "registry."
                ),
                command="snow spcs image-registry token --connection mytest",
                output=(
                    "+----------------------------------------------------------------------------------------------------------------------+\n"
                    "| key        | value                                                                                                   |\n"
                    "|------------+---------------------------------------------------------------------------------------------------------|\n"
                    "| token      | ****************************************************************************************************    |\n"
                    "|            | ****************************************************************************************************    |\n"
                    "| expires_in | 3600                                                                                                    |\n"
                    "+----------------------------------------------------------------------------------------------------------------------+"
                ),
            ),
            Example(
                description=plain_text("Example usage with docker:"),
                command=(
                    "snow spcs image-registry token --format=JSON | docker login "
                    "YOUR_HOST -u 0sessiontoken --password-stdin"
                ),
            ),
        ),
    ),
    epilog=_TOKEN_EPILOG,
)
def token(**options) -> ObjectResult:
    """
    Retrieves a registry authentication token based on your current connection.

    Note that this token is specific to your current user and will not grant access to any repositories that your current user cannot access.


    """
    return ObjectResult(RegistryManager().get_token())


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REGISTRY_RELATED,
        usage_notes=(
            plain_text(
                "The current role must have READ privileges for the image repository "
                "in the account to get the registry URL."
            ),
        ),
        examples=(
            Example(
                command="snow spcs image-registry url",
                output="<orgname-acctname>.registry.snowflakecomputing.com",
            ),
        ),
    ),
)
def url(private_link: bool = PrivateLinkOption, **options) -> MessageResult:
    """
    Gets the image registry URL for the current account.

    Must be called from a role that can view at least one image repository in the image registry.
    """
    return MessageResult(RegistryManager().get_registry_url(private_link))


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REGISTRY_RELATED,
        usage_notes=(
            bullet_list(
                bullet(
                    link(
                        "https://www.docker.com/products/docker-desktop/",
                        "Docker Desktop",
                    ),
                    " must be installed because the command uses docker to log in to Snowflake.",
                ),
                bullet(
                    "The current role must have READ privileges for the image repository "
                    "in the account to get the registry URL."
                ),
            ),
        ),
        examples=(
            Example(
                command="snow spcs image-registry login",
                output="Login Succeeded",
            ),
        ),
    ),
)
def login(private_link: bool = PrivateLinkOption, **options) -> MessageResult:
    """
    Logs in to the account image registry with the current user's credentials through Docker.

    Must be called from a role that can view at least one image repository in the image registry.
    """
    return MessageResult(RegistryManager().docker_registry_login(private_link).strip())
