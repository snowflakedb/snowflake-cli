set -e
export SF_REGISTRY="$(snow spcs image-registry url -c integration)"
DATABASE=$(echo "${SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE}" | tr '[:upper:]' '[:lower:]')

echo "Using registry: ${SF_REGISTRY}"
docker build --platform linux/amd64 -t "${SF_REGISTRY}/${DATABASE}/public/snowcli_repository/snowpark_test_echo:1" .
snow spcs image-registry token --format=json -c integration | docker login "${SF_REGISTRY}/${DATABASE}/public/snowcli_repository" -u 0sessiontoken --password-stdin
docker push "${SF_REGISTRY}/${DATABASE}/public/snowcli_repository/snowpark_test_echo:1"
