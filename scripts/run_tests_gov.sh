# Python 3.11 matches QA/preprod RT and project requires-python >= 3.11.
# Extra pytest args come from PYTEST_ARGS, not a marker in this repository.
python3.11 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip click==8.2.1 hatch==1.15.1 virtualenv==20.39.1

echo "Test cleanup"
python -m hatch env create integration
python -m hatch run e2e:cleanup

echo "Run test"
echo "PYTEST_ARGS=${PYTEST_ARGS}"
python -m hatch run integration:test_gov ${PYTEST_ARGS}
