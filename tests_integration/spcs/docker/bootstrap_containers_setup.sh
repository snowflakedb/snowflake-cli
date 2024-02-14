set -e
export SF_REGISTRY="$(snow spcs image-registry url -c int)"
echo "Using registry: ${SF_REGISTRY}"
docker build --platform linux/amd64 -t "${SF_REGISTRY}/snowcli_db/public/snowcli_repository/snowpark_test_echo:1" .
snow spcs image-registry token --format=json -c int | docker login "${SF_REGISTRY}/snowcli_db/public/snowcli_repository" -u 0sessiontoken --password-stdin
docker push "${SF_REGISTRY}/snowcli_db/public/snowcli_repository/snowpark_test_echo:1"
