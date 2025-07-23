FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/library/ubuntu:18.04 AS base

ENV HOME=/root

# Update package lists and install core tools in one layer
RUN apt update && apt install -y --no-install-recommends \
    software-properties-common \
    binutils \
    curl \
    git \
    build-essential \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python build dependencies in one layer
RUN apt update && apt install -y --no-install-recommends \
    ruby \
    squashfs-tools \
    rpm \
    vim \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    libncursesw5-dev \
    xz-utils \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y tk-dev \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

# Install Ruby gems (separate layer for better caching)
RUN gem install dotenv -v 2.8.1 && gem install fpm

# Install Python via pyenv (separate layer for caching)
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH"
ENV PYTHON_VERSION=3.10

RUN curl -sSf https://pyenv.run | bash \
    && pyenv install ${PYTHON_VERSION} \
    && pyenv global ${PYTHON_VERSION} \
    && pyenv rehash

# Install Python packages (separate layer)
RUN pyenv exec pip install -U pip uv hatch

# Install Rust and Cargo (separate layer for caching)
RUN curl -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /snowflake-cli
