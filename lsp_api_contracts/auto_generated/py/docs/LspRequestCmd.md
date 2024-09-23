# LspRequestCmd


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type_name** | **str** |  | 
**open_input_1** | **str** |  | [optional] 
**open_input_2** | **str** |  | [optional] 
**delete_input_1** | **str** |  | [optional] 

## Example

```python
from lsp_api_contracts.models.lsp_request_cmd import LspRequestCmd

# TODO update the JSON string below
json = "{}"
# create an instance of LspRequestCmd from a JSON string
lsp_request_cmd_instance = LspRequestCmd.from_json(json)
# print the JSON string representation of the object
print(LspRequestCmd.to_json())

# convert the object into a dict
lsp_request_cmd_dict = lsp_request_cmd_instance.to_dict()
# create an instance of LspRequestCmd from a dict
lsp_request_cmd_from_dict = LspRequestCmd.from_dict(lsp_request_cmd_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


