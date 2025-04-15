from __future__ import annotations


# test import
import syrupy
import logging

log = logging.getLogger("SnowCLI_Logs_Test")


def hello_function(name: str) -> str:
    log.debug("This is a debug message")
    log.info("This is an info message")
    log.warning("This is a warning message")
    log.error("This is an error message")
    return f"Hello {name}!"
