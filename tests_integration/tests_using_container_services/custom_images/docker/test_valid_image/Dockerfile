# Test Dockerfile for valid image integration tests
# This image simulates a valid Snowflake custom image configuration
FROM python:3.10-slim

# Add base image label to simulate Snowflake base image detection
# Using the CPU base image path for testing
LABEL org.opencontainers.image.base.name="snowflake/images/snowflake_images/st_plat/runtime/x86/runtime_image/snowbooks:latest"

# Copy and set up entrypoint
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Set required environment variables
ENV DASHBOARD_PORT=12003

# Install required Python packages
# Note: Some packages like snowflake-ml-python have complex dependencies,
# so we install lightweight alternatives for testing purposes
RUN pip install --no-cache-dir \
    ipykernel \
    sqlparse \
    jinja2 \
    notebook \
    ipython \
    psutil \
    jupyter-server

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["bash"]
