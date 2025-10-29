#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Building snowflake-cli Docker image..."
echo "======================================="

docker build --progress=plain -t snowflake-cli "$SCRIPT_DIR"

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================="
    echo "Build successful!"
    echo "Image: snowflake-cli:latest"
    echo ""
    echo "Usage:"
    echo "  docker run --rm snowflake-cli --version"
    echo "  docker run --rm snowflake-cli --help"
else
    echo ""
    echo "======================================="
    echo "Build failed!"
    exit 1
fi
