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
from snowflake.cli.api.commands.flags import (
    IfNotExistsOption,
    ReplaceOption,
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
    SingleQueryResult,
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

add_object_command_aliases(
    app=app,
    object_type=ObjectType.IMAGE_REPOSITORY,
    name_argument=REPO_NAME_ARGUMENT,
    like_option=like_option(
        help_example='`list --like "my%"` lists all image repositories that begin with “my”.'
    ),
    scope_option=scope_option(help_example="`list --in database my_db`"),
    ommit_commands=["describe"],
)


@app.command(requires_connection=True)
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


@app.command("list-images", requires_connection=True)
def list_images(
    name: FQN = REPO_NAME_ARGUMENT,
    **options,
) -> CollectionResult:
    """Lists images in the given repository."""
    repository_manager = ImageRepositoryManager()
    database = repository_manager.get_database()
    schema = repository_manager.get_schema()
    url = repository_manager.get_repository_url(name.identifier)
    api_url = repository_manager.get_repository_api_url(url)
    bearer_login = RegistryManager().login_to_registry(api_url)
    repos = []
    query: Optional[str] = f"{api_url}/_catalog?n=10"

    while query:
        # Make paginated catalog requests
        response = requests.get(
            query, headers={"Authorization": f"Bearer {bearer_login}"}
        )

        if response.status_code != 200:
            raise ClickException(f"Call to the registry failed {response.text}")

        data = json.loads(response.text)
        if "repositories" in data:
            repos.extend(data["repositories"])

        if "Link" in response.headers:
            # There are more results
            query = f"{api_url}/_catalog?n=10&last={repos[-1]}"
        else:
            query = None

    images = []
    for repo in repos:
        prefix = f"/{database}/{schema}/{name}/"
        repo = repo.replace("baserepo/", prefix)
        images.append({"image": repo})

    return CollectionResult(images)


@app.command("list-tags", requires_connection=True)
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
    """Lists tags for the given image in a repository."""

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


@app.command("url", requires_connection=True)
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
