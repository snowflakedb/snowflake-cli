from prompt_toolkit.completion import WordCompleter, merge_completers
from snowflake.cli._plugins.sql.lexer.functions import FUNCTIONS
from snowflake.cli._plugins.sql.lexer.keywords import KEYWORDS
from snowflake.cli._plugins.sql.lexer.types import TYPES

functions_completer = WordCompleter(FUNCTIONS, ignore_case=True)
keywords_completer = WordCompleter(KEYWORDS, ignore_case=True)
types_completer = WordCompleter(TYPES, ignore_case=True)

cli_completer = merge_completers(
    [functions_completer, keywords_completer, types_completer]
)
