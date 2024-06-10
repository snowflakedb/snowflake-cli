from __future__ import annotations

import os
from typing import Any, Dict


class EnvironWithDefinedDictFallback(Dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as e:
            raise AttributeError(e)

    def __getitem__(self, item):
        if item in os.environ:
            return os.environ[item]
        return super().__getitem__(item)

    def __contains__(self, item):
        return item in os.environ or super().__contains__(item)

    def update_from_dict(self, update_values: Dict[str, Any]):
        return super().update(update_values)
