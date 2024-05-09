from os import PathLike
from pathlib import Path
from sys import stdin, stdout
from typing import List, Optional, Union

from click import ClickException


def needs_confirmation(needs_confirm: bool, auto_yes: bool) -> bool:
    return needs_confirm and not auto_yes


def is_tty_interactive():
    return stdin.isatty() and stdout.isatty()


def get_first_paragraph_from_markdown_file(file_path: Path) -> Optional[str]:
    """
    Reads a Markdown file at the given file path and finds the first paragraph

    Parameters:
        file_path (Path): Path to Markdown file

    Returns:
        Optional[str]: the first paragraph as a string, or None
        if no paragraph could be found

    Raises:
        FileNotFoundError: if file_path to Markdown file does not exist
    """
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    with open(file_path, "r") as markdown_file:
        paragraph_text = None

        for line in markdown_file:
            stripped_line = line.strip()
            if not stripped_line.startswith("#") and stripped_line:
                paragraph_text = stripped_line
                break

        return paragraph_text


def shallow_git_clone(url: Union[str, PathLike], to_path: Union[str, PathLike]):
    """
    Performs a shallow clone of the repository at the provided url to the path specified

    Parameters:
        url (str | PathLike): Valid git url.
        to_path (str | PathLike): Path to which the repository should be cloned to.

    Returns:
        Repo: the repository that was cloned
    """
    from git import Repo

    # Clone the repository in the directory with options.
    repo = Repo.clone_from(
        url=url,
        to_path=to_path,
        filter=["tree:0"],
        depth=1,
    )
    # Close repo to avoid issues with permissions on Windows
    repo.close()

    return repo


def verify_no_directories(paths_to_sync: List[Path]):
    for path in paths_to_sync:
        if path.is_dir():
            raise ClickException(
                f"{path} is a directory. Add the -r flag to deploy directories."  #
            )


def verify_exists(paths_to_sync: List[Path]):
    for path in paths_to_sync:
        if not path.exists():
            raise ClickException(f"The following path does not exist: {path}")
