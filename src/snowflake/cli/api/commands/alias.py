from copy import deepcopy


def build_alias(main_app, name: str, help_str: str):
    alias = deepcopy(main_app)
    alias.info.name = name
    alias.info.help = help_str
    alias.info.hidden = True
    return alias
