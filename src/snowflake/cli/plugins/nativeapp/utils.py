from pathlib import Path
from sys import stdin, stdout
from typing import Optional, Union

from git import PathLike, Repo


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
        if no paragraph could be found, or the path was invalid
    """
    if not file_path.exists():
        return None

    with open(file_path, "r") as markdown_file:
        paragraph_text = None

        for line in markdown_file:
            stripped_line = line.strip()
            if not stripped_line.startswith("#") and stripped_line:
                paragraph_text = stripped_line
                break

        return paragraph_text


def shallow_git_clone(url: Union[str, PathLike], to_path: Union[str, PathLike]) -> Repo:
    """
    Performs a shallow clone of the repository at the provided url to the path specified

    Parameters:
        url (str | PathLike): Valid git url.
        to_path (str | PathLike): Path to which the repository should be cloned to.

    Returns:
        Repo: the repository that was cloned
    """
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
