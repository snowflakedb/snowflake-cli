# Context


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**connection** | [**Connection**](Connection.md) |  | [optional] 
**env** | **Dict[str, str]** |  | [optional] 
**project_path** | **str** |  | [optional] 

## Example

```python
from lsp_api_contracts.models.context import Context

# TODO update the JSON string below
json = "{}"
# create an instance of Context from a JSON string
context_instance = Context.from_json(json)
# print the JSON string representation of the object
print(Context.to_json())

# convert the object into a dict
context_dict = context_instance.to_dict()
# create an instance of Context from a dict
context_from_dict = Context.from_dict(context_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


