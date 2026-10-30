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

from __future__ import annotations

import json
from typing import Optional

import requests
import typer
from click import ClickException
from snowflake.cli._plugins.object.command_aliases import (
    add_object_command_aliases,
    scope_option,
)
from snowflake.cli._plugins.spcs.image_registry.manager import RegistryManager
from snowflake.cli._plugins.spcs.image_repository.manager import ImageRepositoryManager
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    bullet,
    bullet_list,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.decorators import with_project_definition
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    ReplaceOption,
    entity_argument,
    identifier_argument,
    like_option,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.output.types import (
    CollectionResult,
    MessageResult,
    QueryResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.definition_helper import (
    get_entity_from_project_definition,
)
from snowflake.cli.api.project.util import is_valid_object_name

app = SnowTyperFactory(
    name="image-repository",
    help="Manages Snowpark Container Services image repositories.",
    short_help="Manages image repositories.",
)


def _repo_name_callback(name: FQN):
    if not is_valid_object_name(name.identifier, max_depth=2, allow_quoted=False):
        raise ClickException(
            f"'{name}' is not a valid image repository name. Note that image repository names must be unquoted identifiers. The same constraint also applies to database and schema names where you create an image repository."
        )
    return name


REPO_NAME_ARGUMENT = identifier_argument(
    sf_object="image repository",
    example="my_repository",
    callback=_repo_name_callback,
)

_IMAGE_REPOSITORY_RELATED = (
    link("/developer-guide/snowflake-cli/index"),
    link("/developer-guide/snowflake-cli/command-reference/overview"),
    link(
        "/developer-guide/snowflake-cli/command-reference/spcs-commands/overview",
        "spcs command reference",
    ),
    link(
        "/developer-guide/snowflake-cli/command-reference/spcs-commands/image-repository-commands/overview",
        "image-repository command reference",
    ),
)

add_object_command_aliases(
    app=app,
    object_type=ObjectType.IMAGE_REPOSITORY,
    name_argument=REPO_NAME_ARGUMENT,
    like_option=like_option(
        help_example='`--like "my%"` lists all image repositories that begin with “my”.'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["describe"],
    list_docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(plain_text("None."),),
    ),
    drop_docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(plain_text("None."),),
    ),
)


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                command="snow spcs image-repository create tutorial_repository",
                output=(
                    "+-------------------------------------------+\n"
                    "| key    | value                            |\n"
                    "|--------+----------------------------------|\n"
                    "| status | Statement executed successfully. |\n"
                    "+-------------------------------------------+"
                ),
            ),
        ),
    ),
)
def create(
    name: FQN = REPO_NAME_ARGUMENT,
    replace: bool = ReplaceOption(),
    if_not_exists: bool = IfNotExistsOption(),
    **options,
):
    """
    Creates a new image repository in the current schema.
    """
    return SingleQueryResult(
        ImageRepositoryManager().create(
            name=name.identifier, replace=replace, if_not_exists=if_not_exists
        )
    )


@app.command(
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(
            plain_text(
                "The ",
                code("snow spcs image repository deploy"),
                " command creates an image repository from its definition in a ",
                code("snowflake.yml"),
                " project definition file. For more information, see ",
                link(
                    "#label-sfcli-repo-pdf",
                    "Image repository project definition file",
                ),
                ".",
            ),
        ),
        examples=(
            Example(
                description=plain_text(
                    "The following example creates an image repository defined in the ",
                    code("snowflake.yml"),
                    " file in the current directory.",
                ),
                command="snow spcs image-repository deploy",
                output=(
                    "+---------------------------------------------------------------------+\n"
                    "| key    | value                                                      |\n"
                    "|--------+------------------------------------------------------------|\n"
                    "| status | Image Repository MY_IMAGE_REPOSITORY successfully created. |\n"
                    "+---------------------------------------------------------------------+"
                ),
            ),
        ),
    ),
)
@with_project_definition()
def deploy(
    entity_id: str = entity_argument("image-repository"),
    replace: bool = ReplaceOption(
        help="Replace the image repository if it already exists."
    ),
    **options,
):
    """
    Deploys a new image repository from snowflake.yml file.
    """
    image_repository = get_entity_from_project_definition(
        ObjectType.IMAGE_REPOSITORY, entity_id
    )

    cursor = ImageRepositoryManager().create(
        name=image_repository.fqn.identifier,
        if_not_exists=False,
        replace=replace,
    )
    return SingleQueryResult(cursor)


@app.command(
    "list-images",
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "The following example lists the images and tags in a repository named ",
                    code("images"),
                    " in the ",
                    code("my_db"),
                    " database:",
                ),
                command="snow spcs image-repository list-images images --database my_db",
                output=(
                    "+--------------------------------------------------------------------------------------------------------------------------------------------------------+\n"
                    "| created_on                | image_name            | tags   | digest                                         | image_path                               |\n"
                    "|---------------------------+-----------------------+--------+------------------------------------------------+------------------------------------------|\n"
                    "| 2024-10-11 14:23:49-07:00 | echo_service          | latest | sha256:a8a001fef406fdb3125ce8e8bf9970c35af7084 | my_db/test_schema/images/echo_service:   |\n"
                    "|                           |                       |        | fc33b0886d7a8915d3082c781                      | latest                                   |\n"
                    "| 2024-10-14 22:21:14-07:00 | test_counter          | latest | sha256:8cae96dac29a4a05f54bb5520003f964baf67fc | my_db/test_schema/images/test_counter:   |\n"
                    "|                           |                       |        | 38dcad3d2c85d6c5aa7381174                      | latest                                   |\n"
                    "+--------------------------------------------------------------------------------------------------------------------------------------------------------+"
                ),
            ),
        ),
    ),
)
def list_images(
    name: FQN = REPO_NAME_ARGUMENT,
    like_option: Optional[str] = like_option(
        help_example='`--like "my%"` lists all image repositories that begin with “my”.'
    ),
    **options,
) -> CollectionResult:
    """Lists images in the given repository."""
    return QueryResult(
        ImageRepositoryManager().list_images(name.identifier, like_option)
    )


@app.command(
    "list-tags",
    requires_connection=True,
    deprecated=True,
    docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(plain_text("None."),),
        examples=(
            Example(
                description=plain_text(
                    "The following example lists the tags associated with the registry named ",
                    code("MY_DB/PUBLIC/images/cp-schema-registry"),
                    ".",
                ),
                command=(
                    "snow spcs image-repository list-tags images --image_name "
                    '"MY_DB/PUBLIC/images/cp-schema-registry" --database my_db'
                ),
                output=(
                    "+----------------------------------------------------+\n"
                    "| tag                                                |\n"
                    "|----------------------------------------------------|\n"
                    "| /MY_DB/PUBLIC/images/cp-schema-registry:7.3.0      |\n"
                    "+----------------------------------------------------+"
                ),
            ),
        ),
    ),
)
def list_tags(
    name: FQN = REPO_NAME_ARGUMENT,
    image_name: str = typer.Option(
        ...,
        "--image-name",
        "--image_name",
        "-i",
        help="Fully qualified name of the image as shown in the output of list-images",
        show_default=False,
    ),
    **options,
) -> CollectionResult:
    """Lists tags for the given image in a repository. This command is deprecated and will be removed in a future release. Use `list-images` instead."""

    repository_manager = ImageRepositoryManager()
    url = repository_manager.get_repository_url(name.identifier)
    api_url = repository_manager.get_repository_api_url(url)
    bearer_login = RegistryManager().login_to_registry(api_url)

    image_realname = "/".join(image_name.split("/")[4:])
    tags = []
    query: Optional[str] = f"{api_url}/{image_realname}/tags/list?n=10"

    while query is not None:
        # Make paginated catalog requests
        response = requests.get(
            query, headers={"Authorization": f"Bearer {bearer_login}"}
        )

        if response.status_code != 200:
            cli_console.warning(f"Call to the registry failed {response.text}")

        data = json.loads(response.text)
        if "tags" in data:
            tags.extend(data["tags"])

        if "Link" in response.headers:
            # There are more results
            query = f"{api_url}/{image_realname}/tags/list?n=10&last={tags[-1]}"
        else:
            query = None

    tags_list = []
    for tag in tags:
        image_tag = f"{image_name}:{tag}"
        tags_list.append({"tag": image_tag})

    return CollectionResult(tags_list)


@app.command(
    "url",
    requires_connection=True,
    docs=CommandDocs(
        related=_IMAGE_REPOSITORY_RELATED,
        usage_notes=(
            bullet_list(
                bullet(
                    "The current role must have READ privileges for the image repository "
                    "in the account to get the registry URL."
                ),
                bullet(
                    "The URL is returned as a text string, so you can store it in an "
                    "environment variable for convenience. For example: ",
                    code("export REPO_URL = $(snow spcs image-repository url <name>)"),
                ),
            ),
        ),
        examples=(
            Example(
                command="snow spcs image-repository url tutorial_repository",
                output=(
                    "<orgname-acctname>.registry.snowflakecomputing.com/"
                    "tutorial_db/data_schema/tutorial_repository"
                ),
            ),
        ),
    ),
)
def repo_url(
    name: FQN = REPO_NAME_ARGUMENT,
    **options,
):
    """Returns the URL for the given repository."""
    return MessageResult(
        (
            ImageRepositoryManager().get_repository_url(
                repo_name=name.identifier, with_scheme=False
            )
        )
    )
