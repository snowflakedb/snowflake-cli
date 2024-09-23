# LspRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**context** | [**Context**](Context.md) |  | 
**cmd** | [**Cmd**](Cmd.md) |  | 

## Example

```python
from lsp_api_contracts.models.lsp_request import LspRequest

# TODO update the JSON string below
json = "{}"
# create an instance of LspRequest from a JSON string
lsp_request_instance = LspRequest.from_json(json)
# print the JSON string representation of the object
print(LspRequest.to_json())

# convert the object into a dict
lsp_request_dict = lsp_request_instance.to_dict()
# create an instance of LspRequest from a dict
lsp_request_from_dict = LspRequest.from_dict(lsp_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


