from typing import List, Union

from snowflake.cli.api.project.schemas.v1.native_app.path_mapping import PathMapping

Artifacts = List[Union[PathMapping, str]]
