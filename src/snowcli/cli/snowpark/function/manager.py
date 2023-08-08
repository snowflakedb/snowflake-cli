from __future__ import annotations

from typing import Optional, List

import click

from snowcli.cli.common.sql_execution import SqlExecutionMixin


class FunctionManager(SqlExecutionMixin):
    @staticmethod
    def identifier(
        name: Optional[str] = None,
        signature: Optional[str] = None,
        name_and_signature: Optional[str] = None,
    ):
        if all([name, signature, name_and_signature]):
            raise click.ClickException(
                "Provide only one, name and arguments or full signature. Both provided."
            )

        if not (name and signature) and not name_and_signature:
            raise click.ClickException(
                "Provide either name and arguments or full signature. None provided."
            )

        if name and signature:
            name_and_signature = name + signature

        return name_and_signature

    def drop(self, identifier: str):
        return self._execute_query(f"drop function {identifier}")

    def show(self, like: Optional[str] = None):
        query = "show user functions"
        if like:
            query += f" like '{like}'"
        return self._execute_query(query)

    def describe(self, identifier: str):
        return self._execute_query(f"describe function {identifier}")

    def execute(self, expression: str):
        return self._execute_query(f"select {expression}")

    @staticmethod
    def artifact_stage_path(identifier: str):
        return (
            identifier.replace(
                "(",
                "",
            )
            .replace(
                ")",
                "",
            )
            .replace(
                " ",
                "_",
            )
            .replace(
                ",",
                "",
            )
            .lower()
        )

    def create(
        self,
        identifier: str,
        return_type: str,
        handler: str,
        artifact_file: str,
        packages: List[str],
        overwrite: bool,
    ):
        create_stmt = "create or replace" if overwrite else "create"
        return self._execute_query(
            f"""\
            {create_stmt} function {identifier}
            returns {return_type}
            language python
            runtime_version=3.8
            imports=('{artifact_file}')
            handler='{handler}'
            packages=()
        """
        )
