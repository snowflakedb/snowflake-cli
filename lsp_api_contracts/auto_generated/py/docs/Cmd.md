# Cmd


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | 

## Example

```python
from lsp_api_contracts.models.cmd import Cmd

# TODO update the JSON string below
json = "{}"
# create an instance of Cmd from a JSON string
cmd_instance = Cmd.from_json(json)
# print the JSON string representation of the object
print(Cmd.to_json())

# convert the object into a dict
cmd_dict = cmd_instance.to_dict()
# create an instance of Cmd from a dict
cmd_from_dict = Cmd.from_dict(cmd_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


