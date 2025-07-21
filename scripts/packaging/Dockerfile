FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/library/ubuntu:18.04 AS base

ENV HOME=/root

RUN apt -y update
RUN apt -y install -y software-properties-common binutils --fix-missing
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt install -y --fix-missing ruby\
  squashfs-tools\
  rpm\
  vim \
  build-essential\
  libssl-dev\
  zlib1g-dev\
  libbz2-dev\
  libreadline-dev\
  libsqlite3-dev\
  curl\
  git \
  libncursesw5-dev\
  xz-utils \
  libxml2-dev\
  libxmlsec1-dev\
  libffi-dev\
  liblzma-dev
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y tk-dev

RUN gem install dotenv -v 2.8.1
RUN gem install fpm

ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH"
ENV PYTHON_VERSION=3.10

# Set CPU compatibility flags for Rust and Python distribution
ENV RUSTFLAGS="-C target-feature=-crt-static -C target-cpu=x86-64"
ENV HATCH_PYTHON_SOURCE_3_10="https://github.com/indygreg/python-build-standalone/releases/download/20220802/cpython-3.10.6+20220802-x86_64-unknown-linux-gnu-install_only.tar.gz"

RUN curl https://pyenv.run | bash
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv rehash
RUN pyenv global ${PYTHON_VERSION}
RUN pyenv versions
RUN pyenv rehash
RUN pyenv exec pip install -U pip uv hatch

WORKDIR /snowflake-cli
