import configparser
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectionParams:
    user: str
    password: str
    account: str
    host: str = "snowflakecomputing.com"
    port: int = 443
    protocol: str = "https"
