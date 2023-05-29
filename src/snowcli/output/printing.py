from __future__ import annotations

from datetime import datetime
from json import JSONEncoder
from typing import List, Optional

import click
from rich import box, print, print_json
from rich.table import Table
from snowflake.connector.cursor import SnowflakeCursor

from snowcli.output.formats import OutputFormat


class CustomJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def print_db_cursor(
    cursor: SnowflakeCursor,
    only_cols: Optional[List[str]] = None,
    show_header: bool = True,
    show_border: bool = True,
):
    context = click.get_current_context()
    output_format = OutputFormat(context.find_root().params.get("output_format"))

    result = cursor.fetchall()
    column_names = [t[0] for t in cursor.description]
    only_cols = only_cols or column_names

    data = [
        {k: v for k, v in zip(column_names, row) if k in only_cols} for row in result
    ]

    if output_format == OutputFormat.TABLE:
        table = Table(
            show_header=show_header, box=box.HEAVY_HEAD if show_border else None
        )

        for column in column_names:
            if column in only_cols:
                table.add_column(column)

        for row in data:
            table.add_row(*[str(i) for i in row.values()])
        print(table)
    elif output_format == OutputFormat.JSON:
        import json

        print_json(json.dumps(data, cls=CustomJSONEncoder))
    else:
        raise Exception(f"Unknown {output_format} format option")
