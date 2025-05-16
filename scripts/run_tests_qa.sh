python3.10 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip hatch

#openssl s_client -connect snowcli-it.qa6.us-west-2.aws.snowflakecomputing.com:443 -showcerts
#ls /etc/ssl
#ls /etc/ssl/certs
#export REQUESTS_CA_BUNDLE='/etc/ssl/certs/ca-bundle.trust.crt'
update-ca-certificates

echo "Test cleanup"
python -m hatch env create integration
python -m hatch run e2e:cleanup

echo "Run test"
python -m hatch run integration:test_qa
