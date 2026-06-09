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
# refresh status, and unknown rows.
BLUE = "blue"

DOMAIN_STYLE = Style(color="cyan")
BOLD_STYLE = Style(bold=True)

# Refresh
STATUS_STYLE = Style(color=BLUE)
REMOVED_STYLE = Style(color="red", italic=True)
INSERTED_STYLE = Style(color="green", italic=True)

# Test
PASS_STYLE = Style(color="green")
FAIL_STYLE = Style(color="red")

# Plan
CREATE_STYLE = Style(color="green")
ALTER_STYLE = Style(color="yellow")
DROP_STYLE = Style(color="red")
UNKNOWN_STYLE = Style(color=BLUE)

# Deploy progress phases
PHASE_DONE_STYLE = Style(color="green", bold=True)
PHASE_RUNNING_STYLE = Style(color=BLUE, bold=True)
PHASE_FAILED_STYLE = Style(color="red", bold=True)
