from typing import Optional

from snowflake.cli._app.dev.pycharm_remote_debug import (
    setup_pycharm_remote_debugger_if_provided,
)

# How to use remote debugger?
# 1. Create "Python Remote Debugger" run configuration in PyCharm / IntelliJ.
# 2. Install matching pydevd-pycharm via pip in your venv (see instructions in your remote debug run configuration window).
# 3. Add invocation of setup_default_pycharm_remote_debugger() somewhere in your test code - I suggest to do it at the very beginning - in conftest.py.
# 4. Run "remote debug" configuration, create breakpoints your tests and then run your tests, you can start the fun.
def setup_default_pycharm_remote_debugger(
    pycharm_debug_library_path: Optional[str] = None,
):
    setup_pycharm_remote_debugger_if_provided(
        pycharm_debug_library_path=pycharm_debug_library_path
        or "unused if you install pydevd-pycharm via pip",
        pycharm_debug_server_host="localhost",
        pycharm_debug_server_port=12345,
    )
