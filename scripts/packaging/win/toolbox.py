import os
from importlib import import_module
from urllib.request import urlretrieve

try:
    certifi = import_module("certifi")
except ImportError:
    import pip

    pip.main(["install", "--upgrade", "certifi"])
    certifi = import_module("certifi")


os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()
os.environ["SSL_CERT_FILE"] = certifi.where()

urlretrieve(
    "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe",
    "python-3.11.9-amd64.exe",
)
