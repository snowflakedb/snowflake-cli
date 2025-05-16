python3.10 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip hatch

openssl req -x509 -newkey rsa:4096 -sha256 -days 1 \
  -nodes -keyout tmp_cert.key -out tmp_cert.crt -subj "/CN=snowcli-it.qa6.us-west-2.aws.snowflakecomputing.com"
#openssl s_client -connect snowcli-it.qa6.us-west-2.aws.snowflakecomputing.com:443 -showcerts
#ls /etc/ssl
#ls /etc/ssl/certs
export REQUESTS_CA_BUNDLE='$(pwd)tmp_cert.crt'
#update-ca-certificates

echo "Test cleanup"
echo "prune"
python -m hatch env prune
echo "create env"
python -m hatch env create integration
echo "run cleanup script"
python -m hatch run e2e:cleanup

echo "debug"
python -m hatch run integration:debug

echo "Run test"
python -m hatch run integration:test_qa
