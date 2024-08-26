import os
from urllib.request import urlretrieve

import certifi

os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

urlretrieve(
    "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe",
    "python-3.11.9-amd64.exe",
)
