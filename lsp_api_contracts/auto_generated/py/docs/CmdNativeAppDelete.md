# CmdNativeAppDelete


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | 
**delete_input_1** | **str** |  | [optional] 

## Example

```python
from lsp_api_contracts.models.cmd_native_app_delete import CmdNativeAppDelete

# TODO update the JSON string below
json = "{}"
# create an instance of CmdNativeAppDelete from a JSON string
cmd_native_app_delete_instance = CmdNativeAppDelete.from_json(json)
# print the JSON string representation of the object
print(CmdNativeAppDelete.to_json())

# convert the object into a dict
cmd_native_app_delete_dict = cmd_native_app_delete_instance.to_dict()
# create an instance of CmdNativeAppDelete from a dict
cmd_native_app_delete_from_dict = CmdNativeAppDelete.from_dict(cmd_native_app_delete_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


