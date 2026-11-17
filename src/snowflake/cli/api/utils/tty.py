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

import sys
from typing import Any


def is_tty_interactive() -> bool:
    return is_stdin_tty() and is_stdout_tty()


def is_stdin_tty() -> bool:
    return _is_tty(sys.stdin)


def is_stdout_tty() -> bool:
    return _is_tty(sys.stdout)


def _is_tty(stream: Any) -> bool:
    try:
        return bool(stream.isatty())
    except Exception:
        return False
