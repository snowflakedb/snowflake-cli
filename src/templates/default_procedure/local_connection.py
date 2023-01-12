from __future__ import annotations

import configparser
from pathlib import Path

import toml


def get_dev_config(
    environment: str = "dev",
    app_config_path: Path = Path.cwd().joinpath("app.toml"),
) -> dict:
    try:
        app_config = toml.load(app_config_path)
        config = configparser.ConfigParser(inline_comment_prefixes="#")
        config.read(app_config["snowsql_config_path"])
        session_config = config["connections." + app_config["snowsql_connection_name"]]
        session_config_dict = {
            k.replace("name", ""): v.strip('"') for k, v in session_config.items()
        }
        session_config_dict.update(app_config.get(environment))  # type: ignore
        return session_config_dict
    except Exception:
        raise Exception(
            "Error creating snowpark session - be sure you've logged into "
            "the SnowCLI and have a valid app.toml file",
        )
