FROM ubuntu:18.04 AS base

ENV HOME=/root

RUN apt -y update
RUN apt -y install -y software-properties-common binutils
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt install -y ruby\
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

RUN curl https://pyenv.run | bash
RUN echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.profile
RUN echo 'export PATH=$PYENV_ROOT/bin:$PATH' >> ~/.profile
RUN echo 'eval "$(pyenv init -)"' >> ~/.profile
RUN echo 'eval "$(pyenv init -)"' >> ~/.bashrc

ENV PYENV_ROOT=$HOME/.pyenv
ENV PATH=$PYENV_ROOT/bin:$PATH
RUN eval "$(pyenv init -)"
RUN pyenv install 3.10
RUN pyenv global 3.10 && pyenv exec pip install -U pip uv hatch
RUN gem install dotenv -v 2.8.1
RUN gem install fpm

WORKDIR /snowflake-cli
