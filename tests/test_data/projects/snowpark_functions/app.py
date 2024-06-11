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

import sys


def hello(name: str) -> str:
    return f"Hello {name}!"


# For local debugging. Be aware you may need to type-convert arguments if
# you add input parameters
if __name__ == "__main__":
    if len(sys.argv) > 1:
        print(hello(sys.argv[1]))  # type: ignore
    else:
        print(hello("world"))
