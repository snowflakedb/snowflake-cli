FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/library/ubuntu:18.04 AS base

ENV HOME=/root

RUN apt -y update
RUN apt -y install -y software-properties-common binutils --fix-missing
RUN add-apt-repository ppa:deadsnakes/ppa

RUN apt install -y --fix-missing \
  build-essential \
  curl \
  git \
  pkg-config \
  libssl-dev \
  zlib1g-dev \
  libbz2-dev \
  libreadline-dev \
  libffi-dev \
  liblzma-dev \
  libncursesw5-dev \
  libssl1.1 \
  zlib1g \
  libbz2-1.0 \
  liblzma5 \
  libffi6 \
  ruby \
  squashfs-tools \
  rpm

RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y tk-dev

RUN gem install dotenv -v 2.8.1
# Pin fpm to 1.15.1 for Ruby 2.5 compatibility (Newer versions require Ruby 2.6+ that is not available in Ubuntu 18.04)
RUN gem install fpm -v 1.15.1

# Copy and run Python build script
COPY build_python.sh /tmp/build_python.sh
RUN chmod +x /tmp/build_python.sh && /tmp/build_python.sh && rm /tmp/build_python.sh

# Update PATH to use our custom Python
ENV PATH="/usr/local/bin:$PATH"

# Install build tools - these are not embedded in final binary so can use pre-built wheels
# pinning click to 8.2.1 due to issues with 8.3.0 https://github.com/pypa/hatch/issues/2050
RUN python -m pip install -U click==8.2.1 uv hatch==1.15.1 virtualenv==20.39.1

WORKDIR /snowflake-cli
