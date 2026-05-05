# Test Dockerfile for invalid image integration tests
# This image has wrong entrypoint and missing env vars
FROM python:3.10-slim

# Add base image label to simulate Snowflake base image detection
LABEL org.opencontainers.image.base.name="snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:latest"

# Wrong entrypoint
ENTRYPOINT ["/bin/bash"]
CMD ["echo", "This is an invalid image"]
