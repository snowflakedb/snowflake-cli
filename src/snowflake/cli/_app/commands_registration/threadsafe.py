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
