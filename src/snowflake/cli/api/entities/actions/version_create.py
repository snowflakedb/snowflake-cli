from typing import Annotated, Optional

from .lib import HelpText, ParameterDeclarations


def version_create_signature(
    version: Annotated[
        Optional[str],
        HelpText(""),
    ] = None,
    patch: Annotated[
        Optional[int],
        ParameterDeclarations("--patch"),
        HelpText(
            """The patch number you want to create for an existing version.
        Defaults to undefined if it is not set, which means the Snowflake CLI either uses the patch specified in the `manifest.yml` file or automatically generates a new patch number."""
        ),
    ] = None,
    skip_git_check: Annotated[
        Optional[bool],
        ParameterDeclarations("--skip-git-check"),
        HelpText(
            "When enabled, the Snowflake CLI skips checking if your project has any untracked or stages files in git. Default: unset."
        ),
    ] = False,
):
    """
    Generic help text for deploy.
    """
    raise NotImplementedError()
