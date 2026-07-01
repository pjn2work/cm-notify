#!/bin/bash

# Determine directory of this script to run from the root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate the virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Start the application using Uvicorn
echo "Starting Carris Metropolitana Bus Notifier on http://0.0.0.0:8080..."
python -m uvicorn main:app --host 0.0.0.0 --port 8080 --reload
