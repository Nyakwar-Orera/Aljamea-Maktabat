#!/bin/bash
echo "============================================="
echo "🏃 Starting Maktabat al-Jamea in OFFLINE MODE"
echo "============================================="

# Create a local .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "creating .env from .env.example..."
    cp .env.example .env
fi

# Activate virtual environment if it exists
if [ -f "venv/bin/activate" ]; then
    echo "Activating Linux/Mac virtual environment..."
    source venv/bin/activate
elif [ -f "venv/Scripts/activate" ]; then
    echo "Activating Windows virtual environment..."
    source venv/Scripts/activate
else
    echo "⚠️  No virtual environment found. Make sure you installed requirements!"
fi

# Initialize the local SQLite database if needed
echo "Initializing local database..."
python3 appdata_init.py

# Force KOHA_OFFLINE to true and run Flask
echo "Starting Flask server (press Ctrl+C to stop)..."
export KOHA_OFFLINE=true
export FLASK_DEBUG=true
export FLASK_APP=app.py

python3 -m flask run --host=127.0.0.1 --port=5000
