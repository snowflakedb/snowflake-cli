from __future__ import annotations


def deep_merge_dicts(original_values: dict, override_values: dict):
    """
    Takes 2 dictionaries as input: original and override.

    For every key in the override dictionary, override the same key
    in the original dictionary, or create a new one if it doesn't exist.

    If the override value and the original value are both dictionaries,
    instead of overriding, recursively call this function to merge the keys of the sub-dictionaries.
    """
    if not isinstance(override_values, dict) or not isinstance(original_values, dict):
        return

    for field, value in override_values.items():
        if (
            field in original_values
            and isinstance(original_values[field], dict)
            and isinstance(value, dict)
        ):
            deep_merge_dicts(original_values[field], value)
        else:
            original_values[field] = value


def deep_traverse(
    element, visit_action=lambda element: None, update_action=lambda element: element
):
    """
    Traverse a nested structure (lists, dicts, scalars).

    On traversal, it allows for actions or updates on each visit.

    visit_action: caller can provide a function to execute on each scalar element in the structure (leaves of the tree).
    visit_action accepts an element (scalar) as input. Return value is ignored.

    update_action: caller can provide a function to update each scalar element in the structure.
    update_action accepts an element (scalar) as input, and returns the modified value.

    """
    if isinstance(element, dict):
        for key, value in element.items():
            element[key] = deep_traverse(value, visit_action, update_action)
        return element
    elif isinstance(element, list):
        for index, value in enumerate(element):
            element[index] = deep_traverse(value, visit_action, update_action)
        return element
    else:
        visit_action(element)
        return update_action(element)
