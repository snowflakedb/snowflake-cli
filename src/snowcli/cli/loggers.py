import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import typer

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
DEBUG_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
FILE_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

PCZAJKA_PATH = Path(__file__).parents[4] / "logs" / "snowcli.log"  # DEBUG


def create_loggers(verbose: bool, debug: bool):
    """Creates a logger depending on the SnowCLI parameters
    verbose == True - print info and higher logs in default format
    debug == True - print debug and higher logs in debug format
    none of above - print only error logs in default format
    """
    logger = logging.getLogger("snowcli")
    if verbose and debug:
        raise typer.BadParameter("Only one parameter `verbose` or `debug` is possible")
    elif debug:
        log_format = DEBUG_LOG_FORMAT
        log_level = logging.DEBUG
    elif verbose:
        log_format = DEFAULT_LOG_FORMAT
        log_level = logging.INFO
    else:
        log_format = DEFAULT_LOG_FORMAT
        log_level = logging.ERROR

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter(log_format, DATE_FORMAT))
    logger.addHandler(console)
    file = TimedRotatingFileHandler(filename=PCZAJKA_PATH, when="midnight")
    file.setFormatter(logging.Formatter(FILE_LOG_FORMAT, DATE_FORMAT))
    logger.addHandler(file)
    logger.setLevel(log_level)
    logging.getLogger("snowflake").setLevel(log_level)
