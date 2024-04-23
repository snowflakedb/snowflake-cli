import string


class _SnowSQLTemplate(string.Template):
    delimiter = "&"


class _Mapper:
    def __getitem__(self, item):
        return "&{ " + item + " }"


def transpile_snowsql_templates(text: str) -> str:
    return _SnowSQLTemplate(text).safe_substitute(_Mapper())  # type: ignore[arg-type]
