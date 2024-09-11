python3.10 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip hatch

echo "Test cleanup"
python -m hatch env create integration
python -m hatch run e2e:cleanup

echo "Run test"
python -m hatch run integration:test_qa
