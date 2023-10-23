from __future__ import annotations

from enum import Enum

DEPLOYMENT_STAGE = "deployments"


class ObjectType(Enum):
    FUNCTION = "function"
    PROCEDURE = "procedure"
