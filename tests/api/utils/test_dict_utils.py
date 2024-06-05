from snowflake.cli.api.utils.dict_utils import deep_merge_dicts, deep_traverse


def test_merge_dicts_empty():
    test_dict = {}
    deep_merge_dicts(test_dict, {})
    assert test_dict == {}


def test_merge_dicts_recursive_map():
    test_dict = {"a": "a1", "b": "b1", "c": {"d": "d1", "e": "e1"}}
    deep_merge_dicts(test_dict, {"c": {"d": "d2", "a": "a2"}})
    assert test_dict == {"a": "a1", "b": "b1", "c": {"d": "d2", "e": "e1", "a": "a2"}}


def test_merge_dicts_recursive_scalar_replace_map():
    test_dict = {"a": "a1", "b": "b1", "c": {"d": "d1", "e": "e1"}}
    deep_merge_dicts(test_dict, {"c": "c2"})
    assert test_dict == {"a": "a1", "b": "b1", "c": "c2"}


def test_merge_dicts_recursive_two_arrays():
    test_dict = {"a": "a1", "b": {"c": ["c1", "c2", "c3"], "d": "d1"}}
    deep_merge_dicts(test_dict, {"b": {"c": ["c4", "c5"]}})
    assert test_dict == {"a": "a1", "b": {"c": ["c4", "c5"], "d": "d1"}}


def test_deep_traverse_on_map():
    test_struct = {
        "scalar_key": "hello",
        "map_key": {"key1": "value1", "key2": "value2", "key3": True},
        "array_key": ["array1", "array2", 333, {"nestedKey1": "nestedVal1"}],
    }

    visited_elements = []

    def visit_action(element):
        visited_elements.append(element)

    deep_traverse(test_struct, visit_action)

    assert visited_elements == [
        "hello",
        "value1",
        "value2",
        True,
        "array1",
        "array2",
        333,
        "nestedVal1",
    ]


def test_deep_traverse_on_list():
    test_struct = ["val1", 123, False, {"mapKey1": "mapVal1"}]

    visited_elements = []

    def visit_action(element):
        visited_elements.append(element)

    deep_traverse(test_struct, visit_action)

    assert visited_elements == ["val1", 123, False, "mapVal1"]


def test_deep_traverse_on_scalar():
    test_struct = 444

    visited_elements = []

    def visit_action(element):
        visited_elements.append(element)

    deep_traverse(test_struct, visit_action)

    assert visited_elements == [444]


def test_deep_traverse_with_updates():
    test_struct = {
        "scalar_key": "hello",
        "map_key": {"key1": "value1", "key3": True},
        "array_key": ["array1", 333, {"nestedKey1": "nestedVal1"}],
    }

    visited_elements = []

    def update_action(element):
        if isinstance(element, str):
            return element + "_"
        else:
            return None

    deep_traverse(test_struct, update_action=update_action)

    assert test_struct == {
        "scalar_key": "hello_",
        "map_key": {"key1": "value1_", "key3": None},
        "array_key": ["array1_", None, {"nestedKey1": "nestedVal1_"}],
    }
