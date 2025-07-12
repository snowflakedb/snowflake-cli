FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/development-chainguard-virtual/snowflake.com/python-fips:3.10-dev@sha256:b1ac389148a3e0a5b47b655db134f9881885471a7fb3df33df38e8b3b604c3e0

USER root
WORKDIR /home/user
RUN chmod 777 /home/user

RUN apk update \
  && apk add --no-cache openssl-dev rustup

RUN python -m venv .venv
RUN . .venv/bin/activate
RUN pip install -U setuptools pip && pip install cryptography==44.0.3 --no-binary cryptography
RUN pip install -U snowflake-connector-python[secure-local-storage] --no-binary snowflake-connector-python[secure-local-storage]
RUN pip install -U snowflake-cli --no-binary snowflake-cli

RUN apk del openssl-dev rustup

RUN mkdir -p "/root/.ssh" "/root/.snowflake"

ENTRYPOINT ["/bin/bash"]
