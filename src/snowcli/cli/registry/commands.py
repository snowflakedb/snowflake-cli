import json
import requests
import sys
import typer
from typing import Optional

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.output.decorators import with_output
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.output.types import MessageResult
from snowcli.cli.registry.manager import RegistryManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="registry",
    help="Manages Snowpark registries.",
)


@app.command("token")
@global_options_with_connection
@with_output
def token(**options):
    """
    Gets the token from environment to use for authenticating with the registry.
    """
    sys.stdout.write(json.dumps(RegistryManager().get_token()))


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
) -> MessageResult:
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
            return MessageResult(f"Call to the registry failed {response.text}")

        data = json.loads(response.text)
        if "repositories" in data:
            repos.extend(data["repositories"])

        if "Link" in response.headers:
            # There are more results
            query = f"{api_url}/_catalog?n=10&last={repos[-1]}"
        else:
            query = None

    message = "Images in this repository:\n\n"
    for repo in repos:
        prefix = f"{database}/{schema}/{repo_name}/"
        repo = repo.replace("baserepo/", prefix)
        message = f"{message}{repo}\n"

    return MessageResult(message)


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
) -> MessageResult:

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

    message = "Tags for this image:\n\n"
    for tag in tags:
        message = f"{message}{image_name}:{tag}\n"

    return MessageResult(message)
