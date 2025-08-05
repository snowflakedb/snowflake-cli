# syntax=docker/dockerfile:1.4
# Multi-platform Dockerfile for Snowflake CLI
# Use --platform linux/amd64 for consistent builds across architectures

FROM --platform=linux/amd64 artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/library/ubuntu:18.04 AS base

ENV HOME=/root
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt -y update && apt -y install -y \
    software-properties-common \
    binutils \
    ruby \
    squashfs-tools \
    rpm \
    vim \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    curl \
    git \
    libncursesw5-dev \
    xz-utils \
    libxml2-dev \
    libxmlsec1-dev \
    libffi-dev \
    liblzma-dev \
    tk-dev \
    wget \
    ca-certificates \
    --fix-missing && \
    rm -rf /var/lib/apt/lists/*

RUN gem install dotenv -v 2.8.1
RUN gem install fpm

ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH"
ENV PYTHON_VERSION=3.10

# Install pyenv and Python
RUN curl https://pyenv.run | bash && \
    pyenv install ${PYTHON_VERSION} && \
    pyenv rehash && \
    pyenv global ${PYTHON_VERSION} && \
    pyenv versions && \
    pyenv rehash && \
    pyenv exec pip install -U pip uv hatch

# Install Rust for building CLI
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && \
    . ~/.cargo/env && \
    rustup target add x86_64-unknown-linux-musl

ENV PATH="${HOME}/.cargo/bin:$PATH"

# Install musl for static linking
RUN apt update && apt install -y musl-tools musl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /snowflake-cli
