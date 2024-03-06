import json
from typing import Optional

import requests
import typer
from click import ClickException
from snowflake.cli.api.commands.flags import IfNotExistsOption, ReplaceOption
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import (
    CollectionResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.util import is_valid_object_name
from snowflake.cli.plugins.spcs.image_registry.manager import RegistryManager
from snowflake.cli.plugins.spcs.image_repository.manager import ImageRepositoryManager

app = SnowTyper(
    name="image-repository",
    help="Manages Snowpark Container Services image repositories.",
    short_help="Manages image repositories.",
)


def _repo_name_callback(name: str):
    if not is_valid_object_name(name, max_depth=2, allow_quoted=False):
        raise ClickException(
            f"'{name}' is not a valid image repository name. Note that image repository names must be unquoted identifiers. The same constraint also applies to database and schema names where you create an image repository."
        )
    return name


REPO_NAME_ARGUMENT = typer.Argument(
    help="Name of the image repository.",
    callback=_repo_name_callback,
)


@app.command(requires_connection=True)
def create(
    name: str = REPO_NAME_ARGUMENT,
    replace: bool = ReplaceOption(),
    if_not_exists: bool = IfNotExistsOption(),
    **options,
):
    """
    Creates a new image repository in the current schema.
    """
    return SingleQueryResult(
        ImageRepositoryManager().create(
            name=name, replace=replace, if_not_exists=if_not_exists
        )
    )


@app.command("list-images", requires_connection=True)
def list_images(
    name: str = REPO_NAME_ARGUMENT,
    **options,
) -> CollectionResult:
    """Lists images in the given repository."""
    repository_manager = ImageRepositoryManager()
    database = repository_manager.get_database()
    schema = repository_manager.get_schema()
    url = repository_manager.get_repository_url(name)
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
    name: str = REPO_NAME_ARGUMENT,
    image_name: str = typer.Option(
        ...,
        "--image-name",
        "--image_name",
        "-i",
        help="Fully qualified name of the image as shown in the output of list-images",
    ),
    **options,
) -> CollectionResult:
    """Lists tags for the given image in a repository."""

    repository_manager = ImageRepositoryManager()
    url = repository_manager.get_repository_url(name)
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
    name: str = REPO_NAME_ARGUMENT,
    **options,
):
    """Returns the URL for the given repository."""
    return MessageResult(
        (ImageRepositoryManager().get_repository_url(repo_name=name, with_scheme=False))
    )
