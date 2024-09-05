from typing import Any, List

from pydantic.fields import FieldInfo



class FieldInfoWithGetter(FieldInfo):
    getter_return_type: type

    def __init__(self, **kwargs):
        super().__init__()
        self.getter_return_type = kwargs.get("getter_return_type", Any)


def get_converter(source_type: type, target_type: type) -> Any:
    return list_of_path_mapping_to_list_of_str
def convert_path_mapping_to_str(path_mapping):
    return path_mapping.src

def list_of_path_mapping_to_list_of_str(path_mappings: List[Any]) -> List[str]:
    return [convert_path_mapping_to_str(path_mapping) for path_mapping in path_mappings]