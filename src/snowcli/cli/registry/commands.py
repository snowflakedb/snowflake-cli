import json
import requests
import sys
import typer
from typing import Optional

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.cli.registry.manager import RegistryManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="registry",
    help="Manages Snowpark registries.",
)


@app.command("token")
@global_options_with_connection
def token(**options):
    """
    Gets the token from environment to use for authenticating with the registry.
    """
    sys.stdout.write(json.dumps(RegistryManager().get_token()))


@app.command("list-images")
@global_options_with_connection
def list_images(
    repo_name: str = typer.Option(
        ...,
        "--repository_name",
        "-r",
        help="Name of the image repository as seen in `show image repositories`",
    ),
    **options,
):
    database = RegistryManager().get_database()
    schema = RegistryManager().get_schema()
    url = RegistryManager().get_repository_url(repo_name)
    api_url = RegistryManager().get_repository_api_url(url)
    bearer_login = RegistryManager().login_to_registry(api_url)

    repos = []
    query: Optional[str] = f"{api_url}/_catalog?n=10"

    while query is not None:
        # Make paginated catalog requests
        response = requests.get(
            query, headers={"Authorization": f"Bearer {bearer_login}"}
        )

        if response.status_code != 200:
            print("Call to the registry failed", response.text)

        data = json.loads(response.text)
        if "repositories" in data:
            repos.extend(data["repositories"])

        if "Link" in response.headers:
            # There are more results
            query = f"{api_url}/_catalog?n=10&last={repos[-1]}"
        else:
            query = None

    sys.stdout.write("Images in this repository:\n\n")
    for repo in repos:
        prefix = f"{database}/{schema}/{repo_name}/"
        repo = repo.replace("baserepo/", prefix)

        sys.stdout.write(f"{repo}\n")


@app.command("list-tags")
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
):

    url = RegistryManager().get_repository_url(repo_name)
    api_url = RegistryManager().get_repository_api_url(url)
    bearer_login = RegistryManager().login_to_registry(api_url)

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

    sys.stdout.write("Tags for this image:\n\n")
    for tag in tags:
        sys.stdout.write(f"{image_name}:{tag}\n")
