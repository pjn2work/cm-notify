#!/bin/bash

IMAGE_NAME="cm-notify"
CONTAINER_NAME="cm-notify"
PORT=8080

# Stop and remove existing container if running
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Stopping and removing existing container..."
    docker rm -f "$CONTAINER_NAME"
fi

# Build the image
echo "Building Docker image..."
docker build -t "$IMAGE_NAME" "$(dirname "${BASH_SOURCE[0]}")"

# Run the container
echo "Starting container on http://localhost:${PORT} ..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -p "${PORT}:${PORT}" \
    --restart unless-stopped \
    -v "$(dirname "${BASH_SOURCE[0]}")/stop_durations.db:/app/stop_durations.db" \
    "$IMAGE_NAME"

echo "Done. Logs: docker logs -f ${CONTAINER_NAME}"
