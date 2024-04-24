from pathlib import Path

from typer import Argument, Typer

EMPTY_RELEASE_NOTES = """\
# Unreleased version
## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

"""

REPO_ROOT = Path(__file__).parent.parent


def main():
    app = Typer(help="Snowflake CLI dev tool")

    @app.command(hidden=True)
    def _dummy():
        """To force typer to require command"""
        pass

    @app.command()
    def update_release_notes(
        new_version: str = Argument(help="Version being released"),
    ):
        """
        Updates release notes with version being release, prepares template
        for next unreleased version.
        """
        release_notes = REPO_ROOT / "RELEASE-NOTES.md"
        with release_notes.open("r") as fh:
            current_content = fh.readlines()
        current_content[0] = f"# v{new_version}\n"
        new_content = [
            *EMPTY_RELEASE_NOTES.splitlines(keepends=True),
            "\n",
            *current_content,
        ]
        with release_notes.open("w+") as fh:
            fh.writelines(new_content)

    app()


if __name__ == "__main__":
    main()
