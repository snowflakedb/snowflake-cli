python3.10 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip hatch

echo "Test cleanup"
python -m hatch env prune
python -m hatch env create integration
python -m hatch run e2e:cleanup

echo ">>> DEBUG <<<"
export SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE=accountadmin
python -m hatch run integration:debug
export SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE=integration_tests

echo "Run test"
python -m hatch run integration:test_qa
