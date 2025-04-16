import re

from pygments.lexer import bygroups, include
from pygments.lexers.sql import RegexLexer
from pygments.token import (
    Comment,
    Keyword,
    Name,
    Number,
    Operator,
    Punctuation,
    String,
    Text,
)
from snowflake.cli._plugins.sql.lexer.functions import FUNCTIONS
from snowflake.cli._plugins.sql.lexer.keywords import KEYWORDS
from snowflake.cli._plugins.sql.lexer.types import TYPES


class CliLexer(RegexLexer):
    name = "Snowflake-CLI"
    aliases = ("cli", "snowflake-cli")
    mimetype = ("text/x-snowflake-cli",)
    flags = re.IGNORECASE
    tokens = {
        "comments": [
            (r"--.*?$", Comment.Single),
            (r"/\*", Comment.Multiline, "multiline-comments"),
        ],
        "multiline-comments": [
            (r"\*/", Comment.Multiline, "#pop"),
            (r"[^/*]+", Comment.Multiline),
            (r"[/*]", Comment.Multiline),
        ],
        "root": [
            include("comments"),
            (r"\s+", Text),
            (r"[0-9]+", Number.Integer),
            (r"[0-9]*\.[0-9]+(e[+-][0-9]+)", Number.Float),
            (r"'(\\\\|\\'|''|[^'])*'", String.Single),
            (r'"(\\\\|\\"|""|[^"])*"', String.Double),
            (r"`(\\\\|\\`|``|[^`])*`", String.Symbol),
            (r"[+*/<>=~!@#%^&$|`?-]", Operator),
            (
                r"\b({0})(\b\s*)(\()?".format("|".join(TYPES)),
                bygroups(Keyword.Type, Text, Punctuation),
            ),
            (r"\b({0})\b".format("|".join(KEYWORDS + FUNCTIONS)), Keyword),
            (r"\b(auto_increment|engine|charset|tables)\b", Keyword.Pseudo),
            (r"(true|false|null)", Name.Constant),
            (r"([a-z_]\w*)(\s*)(\()", bygroups(Name.Function, Text, Punctuation)),
            (r"[a-z_]\w*", Name),
            (r"\$[a-z0-9]*[._]*[a-z0-9]*", Name.Variable),
            (r"[;:()\[\],.]", Punctuation),
        ],
    }
