from typing import Optional

from snowflake.cli._plugins.stage.diff import (
    DiffResult,
    _to_diff_line,
    _to_src_dest_pair,
)
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.console import cli_console as cc


def print_diff_to_console(
    diff: DiffResult,
    bundle_map: Optional[BundleMap] = None,
):
    if not diff.has_changes():
        cc.message("Your stage is up-to-date with your local deploy root.")
        return

    blank_line_needed = False
    if diff.only_local or diff.different:
        cc.message("Local changes to be deployed:")
        messages_to_output = []
        for p in diff.different:
            src_dest_pair = _to_src_dest_pair(p, bundle_map)
            messages_to_output.append(
                (
                    src_dest_pair,
                    _to_diff_line("modified", src_dest_pair[0], src_dest_pair[1]),
                )
            )
        for p in diff.only_local:
            src_dest_pair = _to_src_dest_pair(p, bundle_map)
            messages_to_output.append(
                (
                    src_dest_pair,
                    _to_diff_line("added", src_dest_pair[0], src_dest_pair[1]),
                )
            )

        with cc.indented():
            for key, message in sorted(messages_to_output, key=lambda pair: pair[0]):
                cc.message(message)

        blank_line_needed = True

    if diff.only_on_stage:
        if blank_line_needed:
            cc.message("")
        cc.message(f"Deleted paths to be removed from your stage:")
        with cc.indented():
            for p in sorted(diff.only_on_stage):
                diff_line = _to_diff_line("deleted", src=None, dest=str(p))
                cc.message(diff_line)
