FROM python:3.11-bookworm

RUN apt update
RUN apt upgrade -y
RUN apt install -y ruby squashfs-tools rpm
RUN gem install fpm
RUN pip install -U pip uv hatch

WORKDIR /snowflake-cli
RUN git clone https://github.com/snowflakedb/snowflake-cli.git .
