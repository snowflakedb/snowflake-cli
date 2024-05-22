from click.exceptions import ClickException


class NotebookStagePathError(ClickException):
    def __init__(self, path: str):
        super().__init__(f"Cannot extract notebook file name from {path=}")
