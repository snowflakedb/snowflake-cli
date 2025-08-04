# syntax=docker/dockerfile:1.4
FROM --platform=linux/amd64 python:3.8-alpine
COPY simple_counter.py ./
ENTRYPOINT ["python3", "-u", "simple_counter.py"]
