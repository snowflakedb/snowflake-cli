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
import time
from dataclasses import dataclass
from enum import Enum


class ExecutionStatus(Enum):
    SUCCESS = "success"
    FAILURE = "failure"


@dataclass
class ExecutionMetadata:
    execution_id: str
    start_time: float = 0
    end_time: float = 0
    status: ExecutionStatus = ExecutionStatus.SUCCESS

    def __post_init__(self):
        self.start_time = time.monotonic()

    def complete(self, status: ExecutionStatus):
        self.end_time = time.monotonic()
        self.status = status

    def duration(self):
        return self.end_time - self.start_time
