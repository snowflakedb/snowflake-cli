# Test Dockerfile for missing packages integration tests
# This image has correct entrypoint but missing required packages
FROM python:3.10-slim

# Add base image label to simulate Snowflake base image detection
LABEL org.opencontainers.image.base.name="snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:latest"

# Copy and set up entrypoint
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set required environment variables
ENV DASHBOARD_PORT=12003

# Only install a subset of packages (missing several required ones)
RUN pip install --no-cache-dir ipykernel psutil

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash"]
