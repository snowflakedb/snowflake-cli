from typing import Optional

from click import ClickException


def setup_pycharm_remote_debugger_if_provided(
    pycharm_debug_library_path: Optional[str],
    pycharm_debug_server_host: Optional[str],
    pycharm_debug_server_port: Optional[int],
):
    if pycharm_debug_library_path:
        if (
            pycharm_debug_server_host is not None
            and pycharm_debug_server_port is not None
        ):
            import sys

            sys.path.append(pycharm_debug_library_path)
            import pydevd_pycharm

            pydevd_pycharm.settrace(
                pycharm_debug_server_host,
                port=pycharm_debug_server_port,
                stdoutToServer=True,
                stderrToServer=True,
            )
        else:
            raise ClickException(
                "Debug server host and port have to be provided to use PyCharm remote debugger"
            )
