import json
import requests
import sys
import typer
from typing import Optional

from click import ClickException

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.output.decorators import with_output
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.output.types import CollectionResult, ObjectResult
from snowcli.cli.snowpark.registry.manager import RegistryManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="registry",
    help="Manages Snowpark registries.",
    rich_markup_mode="markdown",
)


@app.command("token")
@with_output
@global_options_with_connection
def token(**options) -> ObjectResult:
    """
    Gets the token from environment to use for authenticating with the registry. Example usage with docker:

    snow registry token --format=JSON | docker login YOUR_HOST -u 0sessiontoken --password-stdin

    """
    return ObjectResult(RegistryManager().get_token())


@app.command("list-images")
@with_output
@global_options_with_connection
def list_images(
    repo_name: str = typer.Option(
        ...,
        "--repository_name",
        "-r",
        help="Name of the image repository as seen in `show image repositories`",
    ),
    **options,
) -> CollectionResult:
    """Lists images in given repository."""
    registry_manager = RegistryManager()
    database = registry_manager.get_database()
    schema = registry_manager.get_schema()
    url = registry_manager.get_repository_url(repo_name)
    api_url = registry_manager.get_repository_api_url(url)
    bearer_login = registry_manager.login_to_registry(api_url)

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
        prefix = f"{database}/{schema}/{repo_name}/"
        repo = repo.replace("baserepo/", prefix)
        images.append({"image": repo})

    return CollectionResult(images)


@app.command("list-tags")
@with_output
@global_options_with_connection
def list_tags(
    repo_name: str = typer.Option(
        ...,
        "--repository_name",
        "-r",
        help="Name of the image repository as seen in `show image repositories`",
    ),
    image_name: str = typer.Option(
        ...,
        "--image_name",
        "-i",
        help="Name of the image as shown in the output of list-images",
    ),
    **options,
) -> CollectionResult:
    """Lists tags for given image in a repository."""

    registry_manager = RegistryManager()
    url = registry_manager.get_repository_url(repo_name)
    api_url = registry_manager.get_repository_api_url(url)
    bearer_login = registry_manager.login_to_registry(api_url)

    repo_name = image_name.split("/")[2]
    image_realname = "/".join(image_name.split("/")[3:])

    tags = []
    query: Optional[str] = f"{api_url}/{image_realname}/tags/list?n=10"

    while query is not None:
        # Make paginated catalog requests
        response = requests.get(
            query, headers={"Authorization": f"Bearer {bearer_login}"}
        )

        if response.status_code != 200:
            print("Call to the registry failed", response.text)

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
