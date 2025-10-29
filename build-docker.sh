#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Building snow-cli Docker image..."
echo "======================================="

docker build --progress=plain -t snow-cli "$SCRIPT_DIR"

if [ $? -eq 0 ]; then
    echo ""
    echo "======================================="
    echo "Build successful!"
    echo "Image: snow-cli:latest"
    echo ""
    echo "Usage:"
    echo "  docker run --rm snow-cli --version"
    echo "  docker run --rm snow-cli --help"
else
    echo ""
    echo "======================================="
    echo "Build failed!"
    exit 1
fi
