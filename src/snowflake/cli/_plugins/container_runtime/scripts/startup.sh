#!/bin/bash

##### Perform common set up steps #####
set -e # exit if a command fails

echo "Creating log directories..."
mkdir -p /var/log/managedservices/user/mlrs
mkdir -p /var/log/managedservices/system/mlrs
mkdir -p /var/log/managedservices/system/ray

echo "*/1 * * * * root /etc/ray_copy_cron.sh" >> /etc/cron.d/ray_copy_cron
echo "" >> /etc/cron.d/ray_copy_cron
chmod 744 /etc/cron.d/ray_copy_cron

service cron start

mkdir -p /tmp/prometheus-multi-dir

# Create necessary directories
mkdir -p /mnt/data/code-server-user-dir
mkdir -p /mnt/data/app_dev/.code-server-extensions
mkdir -p /mnt/data/workspace

# Install code-server
echo "Installing code-server..."
curl -fsSL https://code-server.dev/install.sh | sh

# Install Python dependencies if requirements.txt exists
if [ -f /mnt/data/requirements.txt ]; then
    echo "Installing Python dependencies..."
    pip install -r /mnt/data/requirements.txt
fi

eth0Ip=$(ifconfig eth0 2>/dev/null | sed -En -e 's/.*inet ([0-9.]+).*/\1/p')
log_dir="/tmp/ray"

# Check if eth0Ip is a valid IP address and fall back to default if necessary
if [[ ! $eth0Ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    eth0Ip="127.0.0.1"
fi

# Determine node type
NODE_TYPE="head"

shm_size=$(df --output=size --block-size=1 /dev/shm | tail -n 1)

# Common parameters for both head and worker nodes
common_params=(
    "--node-ip-address=$eth0Ip"
    "--object-manager-port=${RAY_OBJECT_MANAGER_PORT:-12011}"
    "--node-manager-port=${RAY_NODE_MANAGER_PORT:-12012}"
    "--runtime-env-agent-port=${RAY_RUNTIME_ENV_AGENT_PORT:-12013}"
    "--dashboard-agent-grpc-port=${RAY_DASHBOARD_AGENT_GRPC_PORT:-12014}"
    "--dashboard-agent-listen-port=${RAY_DASHBOARD_AGENT_LISTEN_PORT:-12015}"
    "--min-worker-port=${RAY_MIN_WORKER_PORT:-12031}"
    "--max-worker-port=${RAY_MAX_WORKER_PORT:-13000}"
    "--metrics-export-port=11502"
    "--temp-dir=$log_dir"
    "--disable-usage-stats"
)

# Install VS Code extensions
echo "Installing VS Code extensions..."
# Handle installed extensions if provided via environment variables
if [ ! -z "${VSCODE_EXTENSIONS}" ]; then
    IFS=',' read -ra EXTENSION_LIST <<< "${VSCODE_EXTENSIONS}"
    for ext in "${EXTENSION_LIST[@]}"; do
        echo "Installing extension: $ext"
        (sleep 10; code-server --user-data-dir /mnt/data/code-server-user-dir --extensions-dir /mnt/data/app_dev/.code-server-extensions --install-extension $ext) &
    done
else
    # Install default extensions
    (sleep 10; code-server --user-data-dir /mnt/data/code-server-user-dir --extensions-dir /mnt/data/app_dev/.code-server-extensions --install-extension ms-python.python) &
    (sleep 20; code-server --user-data-dir /mnt/data/code-server-user-dir --extensions-dir /mnt/data/app_dev/.code-server-extensions --install-extension ms-toolsai.jupyter) &
fi

# Specific parameters for head and worker nodes
if [ "$NODE_TYPE" = "worker" ]; then
    # Check mandatory environment variables for worker
    if [ -z "$RAY_HEAD_ADDRESS" ] || [ -z "$SERVICE_NAME" ]; then
        echo "Error: RAY_HEAD_ADDRESS and SERVICE_NAME must be set."
        exit 1
    fi

    # Additional worker-specific parameters
    worker_params=(
        "--address=${RAY_HEAD_ADDRESS}"       # Connect to head node
        "--resources={\"${SERVICE_NAME}\":1, \"node_tag:worker\":1}"  # Custom resource for node identification
        "--object-store-memory=${shm_size}"
    )

    # Start Ray on a worker node
    ray start "${common_params[@]}" "${worker_params[@]}" -v --block
else
    # Additional head-specific parameters
    head_params=(
        "--head"
        "--port=${RAY_HEAD_GCS_PORT:-12001}"                                  # Port of Ray (GCS server)
        "--ray-client-server-port=${RAY_HEAD_CLIENT_SERVER_PORT:-10001}"      # Listening port for Ray Client Server
        "--dashboard-host=${NODE_IP_ADDRESS}"                                 # Host to bind the dashboard server
        "--dashboard-grpc-port=${RAY_HEAD_DASHBOARD_GRPC_PORT:-12002}"        # Dashboard head to listen for grpc on
        "--dashboard-port=${DASHBOARD_PORT}"                                  # Port to bind the dashboard server for local debugging
        "--resources={\"node_tag:head\":1}"                                   # Resource tag for selecting head as coordinator
    )

    # Start Ray on the head node
    ray start "${common_params[@]}" "${head_params[@]}" -v &

    # Start ML runtime grpc server
    PYTHONPATH=/opt/env/site-packages/ python -m web.ml_runtime_grpc_server &

    # Launch code-server
    echo 'Starting code-server...'
    code-server --bind-addr 0.0.0.0:${VSCODE_PORT:-12020} --auth none --user-data-dir /mnt/data/code-server-user-dir --extensions-dir /mnt/data/app_dev/.code-server-extensions --app-name "VSCODE App" --workspace /mnt/data/workspace >/mnt/data/code_server.log 2>&1
fi
