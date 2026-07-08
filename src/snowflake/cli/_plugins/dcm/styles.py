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
from rich.style import Style

# Single source of truth for the DCM "blue" hue. Use the terminal-default
# named color (not a fixed hex) so it respects the user's theme and stays
# identical everywhere it's used: progress spinner/bar, running phase,
# analyze INFO findings / file headers, refresh status, and unknown rows.
BLUE = "blue"

BOLD_STYLE = Style(bold=True)

# Object names (FQNs / table names) across all DCM commands — the top-level
# CREATE/ALTER/DROP row in plan/deploy and the table column in refresh/test.
# Named "magenta" (the ANSI purple) rather than a fixed hex so it follows the
# user's terminal theme, consistent with BLUE above.
OBJECT_NAME_STYLE = Style(color="magenta")

# Refresh
STATUS_STYLE = Style(color=BLUE)
REMOVED_STYLE = Style(color="red", italic=True)
INSERTED_STYLE = Style(color="green", italic=True)

# Test / Analyze
PASS_STYLE = Style(color="green")
FAIL_STYLE = Style(color="red")
WARNING_STYLE = Style(color="yellow")
# INFO-severity analyze findings: plain blue (distinct from bold-blue file headers).
INFO_STYLE = Style(color=BLUE)

# Plan
# Terminal-default (no color). Used for the "set" sub-change keyword, which
# assigns a property value and shouldn't read as a creation (green).
NEUTRAL_STYLE = Style()
CREATE_STYLE = Style(color="green")
ALTER_STYLE = Style(color="yellow")
DROP_STYLE = Style(color="red")
UNKNOWN_STYLE = Style(color=BLUE)
# Property values that are set / changed in ALTER detail rows. Uses the cyan
# hue that object names previously used (now magenta) so values stand out from
# the (neutral) property name and the operation keyword.
VALUE_STYLE = Style(color="cyan")

# Deploy progress phases
PHASE_DONE_STYLE = Style(color="green", bold=True)
PHASE_RUNNING_STYLE = Style(color=BLUE, bold=True)
PHASE_FAILED_STYLE = Style(color="red", bold=True)

# Analyze (file/source path headers stand out in bold blue).
FILE_PATH_STYLE = Style(color=BLUE, bold=True)
