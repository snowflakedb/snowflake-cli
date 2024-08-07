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

import threading
from typing import Callable, Generic, TypeVar

T = TypeVar("T")


class ThreadsafeValue(Generic[T]):
    def __init__(self, value: T):
        self._value = value
        self._lock = threading.Lock()

    def set(self, new_value: T) -> T:  # noqa: A003
        return self.transform(lambda _: new_value)

    def transform(self, f: Callable[[T], T]) -> T:
        with self._lock:
            new_value = f(self._value)
            self._value = new_value
            return new_value

    @property
    def value(self) -> T:
        with self._lock:
            return self._value


class ThreadsafeCounter(ThreadsafeValue[int]):
    def increment(self, d=1) -> int:
        return self.transform(lambda v: v + d)

    def decrement(self, d=1) -> int:
        return self.increment(-d)
