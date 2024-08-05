# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

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
