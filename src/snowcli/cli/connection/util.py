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
    to generate URLs that point to Snowsight. If there is not enough
    information to determine the deployment, we return the organization
    name instead, as this can be used in production Snowsight.
    """
    if not conn.host:
        raise MissingConnectionHostError(conn)

    host_parts = conn.host.split(".")
    if host_parts[-1] == "local":
        return LOCAL_DEPLOYMENT

    if len(host_parts) == 6:
        return ".".join(host_parts[1:4])

    try:
        *_, cursor = conn.execute_string(
            f"select system$return_current_org_name()", cursor_class=DictCursor
        )
        return cursor.fetchone()["SYSTEM$RETURN_CURRENT_ORG_NAME()"]
    except Exception as e:
        raise MissingConnectionHostError(conn)


def get_account(conn: SnowflakeConnection) -> str:
    """
    Determines the account that this connection refers to.
    """
    try:
        *_, cursor = conn.execute_string(
            f"select current_account_name()", cursor_class=DictCursor
        )
        return cursor.fetchone()["CURRENT_ACCOUNT_NAME()"].lower()
    except Exception as e:
        # try to extract the account from the connection information
        if conn.account:
            return conn.account

        if not conn.host:
            raise MissingConnectionHostError(conn)

        host_parts = conn.host.split(".")
        return host_parts[0]


def get_snowsight_host(conn: SnowflakeConnection) -> str:
    try:
        *_, cursor = conn.execute_string(
            f"select system$get_snowsight_host()", cursor_class=DictCursor
        )
        return cursor.fetchone()["SYSTEM$GET_SNOWSIGHT_HOST()"]
    except Exception as e:
        # if we cannot determine the host, assume we're on prod
        return "https://app.snowflake.com"


def make_snowsight_url(conn: SnowflakeConnection, path: str) -> str:
    """Returns a URL on the correct Snowsight instance for the connected account."""
    snowsight_host = get_snowsight_host(conn)
    deployment = get_deployment(conn)
    account = get_account(conn)
    path_with_slash = path if path.startswith("/") else f"/{path}"
    return f"{snowsight_host}/{deployment}/{account}{path_with_slash}"
