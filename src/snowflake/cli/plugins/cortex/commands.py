from __future__ import annotations

from typing import List, Optional

import typer
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import CollectionResult

app = SnowTyper(
    name="cortex",
    help="Provides access to Snowflake Cortex.",
)


@app.command(
    requires_connection=True,
)
def search(
    query: str = typer.Argument(help="Query to look for in your data"),
    service: str = typer.Option(
        help="Cortex search service to be used. Example: --service my_cortex_service",
    ),
    columns: Optional[List[str]] = typer.Option(
        help='Columns that will be returned with the results. If none is provided, only search column will be included in results. Example --columns "foo" --columns "bar"',
        default=None,
    ),
    limit: int = typer.Option(help="Maximum number of results retrieved", default=1),
    **options,
):
    """
    Performs query search using Cortex Search Services
    """
    from snowflake.core import Root

    if not columns:
        columns = []

    conn = cli_context.connection

    search_service = (
        Root(conn)
        .databases[conn.database]
        .schemas[conn.schema]
        .cortex_search_services[service]
    )

    response = search_service.search(
        query=query, columns=columns, limit=limit, filter={}
    )

    return CollectionResult(response.results)


@app.command()
def complete():
    """
    Dummy command placeholder. This is added to register whole group. Command will be added with sfc-gh-pjob PR
    """
    pass
