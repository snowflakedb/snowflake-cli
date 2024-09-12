from time import time

from snowflake.cli.__about__ import VERSION

if len(version := VERSION.split(".")) > 3:
    version[-1] = str(int(time()))
else:
    version.append("0")

print(".".join(version))
