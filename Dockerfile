# step 1 - build the cli
FROM python:3.10-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./
COPY README.md ./
COPY LICENSE ./
COPY src ./src

RUN pip install --no-cache-dir hatch && hatch build -t wheel

# step 2 - package the cli to a minimal image
FROM python:3.10-slim
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl
ENV SF_SKIP_WARNING_FOR_READ_PERMISSIONS_ON_CONFIG_FILE=true
ENTRYPOINT ["snow"]
CMD ["--help"]
