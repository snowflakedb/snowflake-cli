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

DOMAIN_STYLE = Style(color="cyan")
BOLD_STYLE = Style(bold=True)

# Refresh
STATUS_STYLE = Style(color="blue")
REMOVED_STYLE = Style(color="red", italic=True)
INSERTED_STYLE = Style(color="green", italic=True)

# Test
PASS_STYLE = Style(color="green")
FAIL_STYLE = Style(color="red")

# Plan
CREATE_STYLE = Style(color="green")
ALTER_STYLE = Style(color="yellow")
DROP_STYLE = Style(color="red")
UNKNOWN_STYLE = Style(color="blue")
