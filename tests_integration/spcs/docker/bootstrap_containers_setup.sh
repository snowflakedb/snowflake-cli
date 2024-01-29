set -e
echo "Using registry: ${SF_REGISTRY}"
docker build -t "${SF_REGISTRY}.registry.snowflakecomputing.com/snowcli_db/public/snowcli_repository/snowpark_test:1" .
snow registry token --format=json -c int | docker login "${SF_REGISTRY}.registry.snowflakecomputing.com/snowcli_db/public/snowcli_repository" -u 0sessiontoken --password-stdin
docker push "${SF_REGISTRY}.registry.snowflakecomputing.com/snowcli_db/public/snowcli_repository/snowpark_test:1"
