# CmdNativeAppOpen


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | 
**open_input_1** | **str** |  | [optional] 
**open_input_2** | **str** |  | [optional] 

## Example

```python
from lsp_api_contracts.models.cmd_native_app_open import CmdNativeAppOpen

# TODO update the JSON string below
json = "{}"
# create an instance of CmdNativeAppOpen from a JSON string
cmd_native_app_open_instance = CmdNativeAppOpen.from_json(json)
# print the JSON string representation of the object
print(CmdNativeAppOpen.to_json())

# convert the object into a dict
cmd_native_app_open_dict = cmd_native_app_open_instance.to_dict()
# create an instance of CmdNativeAppOpen from a dict
cmd_native_app_open_from_dict = CmdNativeAppOpen.from_dict(cmd_native_app_open_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


