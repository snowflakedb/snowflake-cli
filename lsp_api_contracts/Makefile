generate/lsp_api_python_models:
	# https://openapi-generator.tech/docs/generators/python
	# --global-property models, generates only models, see https://github.com/OpenAPITools/openapi-generator/issues/12754#issuecomment-1282285724
	docker run --rm -v "${PWD}:/lsp_api_contracts" \
		-v "${PWD}/../src:/src" \
		openapitools/openapi-generator-cli generate \
		--global-property models \
		-p packageName=snowflake.cli.plugins.lsp \
		-i /lsp_api_contracts/lsp_api_contracts.yaml \
		-g python \
		-o /src

generate/lsp_api_ts_node_models:
	# https://openapi-generator.tech/docs/generators/typescript-node
	docker run --rm -v "${PWD}:/lsp_api_contracts" openapitools/openapi-generator-cli generate \
		--global-property models \
		-p modelPackage=lsp_api_contracts \
		-i /lsp_api_contracts/lsp_api_contracts.yaml \
		-g typescript-node \
		-o /lsp_api_contracts/auto_generated/ts

generate: generate/lsp_api_python_models generate/lsp_api_ts_node_models