from __future__ import annotations

from typing import List, Optional

import typer
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CollectionResult
from snowflake.core import Root

app = SnowTyper(
    name="cortex",
    help="Provides access to Snowflake Cortex LLM.",
)


@app.command(requires_connection=True)
def search(
    query: str = typer.Argument(help="Query to look for in your data"),
    service: str = typer.Option(help="Cortex search service to be used"),
    columns: Optional[List[str]] = typer.Option(
        help="Columns that will be returned with the results. If set to none, only search column will be included in results",
        default=[],
    ),
    limit: int = typer.Option(help="Maximum number of results retrieved"),
    **options,
):
    """
    Allows access to Cortex Search Services
    """
    query_filter: dict = {}

    conn = cli_context.connection
    search_service = (
        Root(conn)
        .databases[conn.database]
        .schemas[conn.schema]
        .cortex_search_services[service]
    )

    response = search_service.search(
        query=query, columns=columns, limit=limit, filter=query_filter
    )

    return CollectionResult(response.results)

