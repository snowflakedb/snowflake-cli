import logging
import typer
from click.exceptions import ClickException
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor

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


def make_snowsight_url(conn: SnowflakeConnection, path: str) -> str:
    """Returns a URL on the correct Snowsight instance for the connected account."""
    try:
        *_, cursor = conn.execute_string(
            f"select system$get_snowsight_host()", cursor_class=DictCursor
        )
        snowsight_host = cursor.fetchone()["SYSTEM$GET_SNOWSIGHT_HOST()"]
    except Exception as e:
        # if we cannot determine the host, assume we're on prod
        snowsight_host = "https://app.snowflake.com"

    deployment = get_deployment(conn)
    account = get_account(conn)
    path_with_slash = path if path.startswith("/") else f"/{path}"
    return f"{snowsight_host}/{deployment}/{account}{path_with_slash}"
