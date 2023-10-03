import logging
import typer
from click.exceptions import ClickException
from snowflake.connector import SnowflakeConnection

LOCAL_DEPLOYMENT: str = "us-west-2"

log = logging.getLogger(__name__)


class MissingConnectionHostError(ClickException):
    def __init__(self, conn: SnowflakeConnection):
        super().__init__(
            f"The connection host ({conn.host}) was missing or not in "
            "the expected format "
            "(<account>.<deployment>.snowflakecomputing.com)"
        )


def get_deployment(conn: SnowflakeConnection) -> str:
    """
    Determines the deployment this connection refers to; useful
    to generate URLs that point to Snowsight.
    """
    if not conn.host:
        raise MissingConnectionHostError(conn)

    host_parts = conn.host.split(".")
    if host_parts[-1] == "local":
        return LOCAL_DEPLOYMENT

    if len(host_parts) != 6:
        raise MissingConnectionHostError(conn)

    return ".".join(host_parts[1:4])


def get_account(conn: SnowflakeConnection) -> str:
    """
    Determines the account that this connection refers to.
    """
    if conn.account:
        return conn.account

    if not conn.host:
        raise MissingConnectionHostError(conn)

    host_parts = conn.host.split(".")
    return host_parts[0]
