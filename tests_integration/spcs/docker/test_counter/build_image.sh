set -e
export SF_REGISTRY="$(snow spcs image-registry url -c integration)"
echo "Using registry: ${SF_REGISTRY}"
docker build --platform linux/amd64 -t "${SF_REGISTRY}/snowcli_db/public/snowcli_repository/test_counter" .
snow spcs image-registry token --format=json -c integration | docker login "${SF_REGISTRY}/snowcli_db/public/snowcli_repository" -u 0sessiontoken --password-stdin
docker push "${SF_REGISTRY}/snowcli_db/public/snowcli_repository/test_counter"
