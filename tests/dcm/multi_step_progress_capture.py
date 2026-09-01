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
import re
from io import StringIO

from rich.console import Console
from snowflake.cli._plugins.dcm.multistep_progress import MultiStepProgress

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def capture_rendered(progress: MultiStepProgress) -> str:
    """Renders a MultiStepProgress's current frame to plain text (ANSI stripped)."""
    console = Console(
        file=StringIO(),
        force_terminal=True,
        color_system="truecolor",
        width=90,
        legacy_windows=False,
    )
    console.print(progress.get_renderable())
    return ANSI_RE.sub("", console.file.getvalue())


def find_line(progress: MultiStepProgress, label: str) -> str:
    rendered = capture_rendered(progress)
    return next(line for line in rendered.split("\n") if label in line)
